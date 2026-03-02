import { 
    createWixOrder, 
    getProductsBySkus, 
    findWixOrderByExternalId, 
    findWixOrderById, 
    getWixOrderFulfillments, 
    cancelWixOrderById,
    adjustInventory,
    getWixOrderFulfillmentsBatch,
    updateWixOrderDetails,
    addExternalRefundTransaction, 
    addExternalPayment,
} from '../lib/wixClient.js';
import { ensureAuth } from '../lib/sheetsClient.js'; 

const WIX_STORES_APP_ID = "215238eb-22a5-4c36-9e7b-e7c08025e04e"; 

const SHIPPING_TITLES = {
    BRANCH: "НП Відділення",  
    COURIER: "НП Кур'єр",
    POSTOMAT: "НП Поштомат"
};

const WIX_TO_MURKIT_STATUS_MAPPING = {
    "НП Відділення": "nova-post", 
    "НП Кур'єр": "courier-nova-post",
    "НП Поштомат": "nova-post:postomat"
};

function createError(status, message, code = null) {
    const err = new Error(message);
    err.status = status;
    if (code) err.code = code;
    return err;
}

function normalizeSku(sku) {
    if (!sku) return '';
    return String(sku).trim();
}

// Функция для фонового обновления фида остатков
function triggerBackgroundStockUpdate(req) {
  const protocol = req.headers['x-forwarded-proto'] || 'https';
  const host = req.headers.host;
  if (host) {
    const url = `${protocol}://${host}/api/monomarket-stock?forceUpdate=true`;
    // Выполняем fetch без await. Это запускает процесс "в фоне".
    fetch(url).catch(err => console.error('Failed to trigger background stock update:', err));
  }
}

export function checkAuth(req) {
  const authHeader = req.headers.authorization;
  if (!authHeader) return false;
  const b64auth = authHeader.split(' ')[1];
  const credentials = Buffer.from(b64auth, 'base64').toString('utf-8'); 
  const [login, password] = credentials.split(':');
  return login === process.env.MURKIT_USER && password === process.env.MURKIT_PASS;
}

async function readSheetData(sheets, spreadsheetId) {
    let importRes, controlRes;
    try {
        importRes = await sheets.spreadsheets.values.get({ spreadsheetId, range: 'Import!A1:ZZ' });
        controlRes = await sheets.spreadsheets.values.get({ spreadsheetId, range: 'Feed Control List!A1:F' });
    } catch (e) {
        throw createError(500, `Failed to fetch data from Google Sheets (API ERROR): ${e.message}`, "SHEETS_API_ERROR");
    }

    const importValues = (importRes && importRes.data && importRes.data.values) ? importRes.data.values : [];
    const controlValues = (controlRes && controlRes.data && controlRes.data.values) ? controlRes.data.values : [];
    
    if (importValues.length === 0 || controlValues.length === 0) {
        throw createError(500, 'Sheets: Empty or invalid data retrieved from critical sheets (check data ranges and sheet names).', "SHEETS_DATA_EMPTY");
    }
    return { importValues: importValues, controlValues: controlValues };
}

function getProductSkuMap(importValues, controlValues) {
    const headers = importValues[0] || [];
    const rows = importValues.slice(1);
    const controlHeaders = controlValues[0] || [];
    const controlRows = controlValues.slice(1);

    const idxImportField = controlHeaders.indexOf('Import field');
    const idxFeedName = controlHeaders.indexOf('Feed name');

    let murkitCodeColRaw = '';
    let wixSkuColRaw = '';

    controlRows.forEach(row => {
        const importField = row[idxImportField];
        const feedName = row[idxFeedName];
        if (feedName === 'code') murkitCodeColRaw = String(importField).trim();
        if (feedName === 'id') wixSkuColRaw = String(importField).trim();
    });
    
    const murkitCodeColIndex = headers.indexOf(murkitCodeColRaw);
    const wixSkuColIndex = headers.indexOf(wixSkuColRaw);
    
    if (murkitCodeColIndex === -1 || wixSkuColIndex === -1) return {};

    const map = {};
    rows.forEach(row => {
        const mCode = row[murkitCodeColIndex] ? String(row[murkitCodeColIndex]).trim() : '';
        const wSku = row[wixSkuColIndex] ? String(row[wixSkuColIndex]).trim() : '';
        if (mCode && wSku) map[mCode] = wSku;
    });
    return map;
}

const fmtPrice = (num) => parseFloat(num || 0).toFixed(2);

function getFullName(nameObj) {
    if (!nameObj) return { firstName: "Client", lastName: "" };
    return {
        firstName: String(nameObj.first || nameObj.firstName || "Client"),
        lastName: String(nameObj.last || nameObj.lastName || "")
    };
}

// === НОВАЯ ЛОГИКА МАППИНГА (строго по ТЗ) ===
function mapWixOrderToMurkitResponse(wixOrder, fulfillments, externalId) {
    const fulfillmentStatus = wixOrder.fulfillmentStatus; 
    const paymentStatus = wixOrder.paymentStatus; 
    const wixShippingLine = wixOrder.shippingInfo?.title || ''; 

    // --- 1. Сбор данных о доставке (общий блок) ---
    let shipmentType = null;
    let shipment = null;
    let ttn = null;

    if (Array.isArray(fulfillments) && fulfillments.length > 0) {
        const fulfillmentWithTtn = fulfillments
            .find(f => f.trackingInfo && String(f.trackingInfo.trackingNumber || '').trim().length > 0);
        
        if (fulfillmentWithTtn) {
            ttn = String(fulfillmentWithTtn.trackingInfo.trackingNumber).trim();
            shipmentType = WIX_TO_MURKIT_STATUS_MAPPING[wixShippingLine.trim()] || 'nova-post'; 
            shipment = { ttn: ttn };
        }
    }

    // --- 2. Сценарии Статусов ---

    // СЦЕНАРИЙ 1: Заказ отменен (status: CANCELED)
    // Выдаем "canceled", обнуляем shipment
    if (wixOrder.status === 'CANCELED') {
        return {
            id: externalId,
            status: 'canceled',
            cancelStatus: 'canceled',
            shipmentType: null,
            shipment: null
        };
    }

    // СЦЕНАРИЙ 2: Заказ НЕ отменен, но оплата возвращена (paymentStatus: FULLY_REFUNDED / REFUNDED / VOIDED)
    // Выдаем "sent" + "canceling" (независимо от fulfillmentStatus)
    // Добавляем также REFUNDED и VOIDED для надежности, так как API может вернуть разные варианты
    if (['FULLY_REFUNDED', 'REFUNDED', 'VOIDED'].includes(paymentStatus)) {
        return {
            id: externalId,
            status: 'sent', 
            cancelStatus: 'canceling',
            shipmentType: shipmentType, // Если есть данные доставки - показываем
            shipment: shipment
        };
    }

    // СЦЕНАРИЙ 3: Заказ выполнен (fulfillmentStatus: FULFILLED)
    // Выдаем "sent"
    if (fulfillmentStatus === 'FULFILLED') {
        return {
            id: externalId,
            status: 'sent',
            cancelStatus: null,
            shipmentType: shipmentType,
            shipment: shipment
        };
    }

    // СЦЕНАРИЙ 4: Остальные случаи (NOT_FULFILLED, PARTIALLY_FULFILLED и т.д.)
    // Выдаем "accepted"
    return {
        id: externalId,
        status: 'accepted',
        cancelStatus: null,
        shipmentType: null,
        shipment: null
    };
}

// --- MAIN HANDLER ---
export default async function handler(req, res) {
    if (!checkAuth(req)) {
        return res.status(401).json({ error: 'Unauthorized' });
    }

    triggerBackgroundStockUpdate(req);
    
    const urlPathFull = req.url;
    const urlPath = urlPathFull.split('?')[0]; 

    // --- 1. PUT Cancel Order Endpoint ---
    const cancelOrderPathMatch = urlPath.match(/\/orders\/([^/]+)\/cancel$/);
    if (req.method === 'PUT' && cancelOrderPathMatch) {
        const wixOrderId = cancelOrderPathMatch[1]; 

        try {
            const currentWixOrder = await findWixOrderById(wixOrderId);
            if (!currentWixOrder) {
                 return res.status(404).json({ message: 'Order does not exist', code: 'NOT_FOUND' });
            }
            
            const isSent = currentWixOrder.fulfillmentStatus === 'FULFILLED';
            let fulfillments;

            // Если уже отменен
            if (currentWixOrder.status === 'CANCELED') {
                const batchResponse = await getWixOrderFulfillmentsBatch([wixOrderId]);
                const orderFulfillmentData = batchResponse[0];
                fulfillments = (orderFulfillmentData && orderFulfillmentData.fulfillments) ? orderFulfillmentData.fulfillments : [];
                return res.status(200).json(mapWixOrderToMurkitResponse(currentWixOrder, fulfillments, wixOrderId));
            }

            if (isSent) {
                // FULFILLED LOGIC: Refund via transaction
                try {
                    const totalAmount = currentWixOrder.priceSummary?.total?.amount || "0";
                    const currency = currentWixOrder.priceSummary?.total?.currency || "UAH";
                    const orderLineItems = currentWixOrder.lineItems || []; 
                    let refundSuccess = false;

                    console.log(`[DEBUG] Attempting refund for order: ${wixOrderId}`);
                    const refundResult = await addExternalRefundTransaction(wixOrderId, totalAmount, currency);

                    if (refundResult) {
                       refundSuccess = true;
                       console.log(`Order ${wixOrderId} Refunded via AddPayment.`);
                       await updateWixOrderDetails(wixOrderId, {
                            buyerNote: "⚠️ КЛІЄНТ ПОПРОСИВ ПОВЕРНЕННЯ / REFUND SUCCESS (Transaction Added)"
                       });
                    } 
                    
                    if (!refundSuccess) {
                         console.warn(`[DEBUG] REFUND transaction failed for order ${wixOrderId}.`);
                         await updateWixOrderDetails(wixOrderId, {
                              buyerNote: "⚠️ REFUND REQUIRED (AUTO-REFUND FAILED: Transaction API Error)"
                         });
                    }

                } catch (updateError) {
                    console.error(`Wix refund logic crashed for ${wixOrderId}:`, updateError.message);
                    await updateWixOrderDetails(wixOrderId, {
                         buyerNote: `⚠️ REFUND REQUIRED (AUTO-REFUND CRASHED: ${updateError.message})`
                    });
                }

                // Получаем обновленный заказ (чтобы увидеть новый paymentStatus, если он обновился мгновенно, или используем логику маппинга)
                // Важно: wix может не мгновенно обновить статус до FULLY_REFUNDED, но мы вернем маппинг.
                // В данном ответе мы можем вручную форсировать статус для моментального ответа,
                // но функция mapWixOrderToMurkitResponse полагается на реальные данные заказа.
                const updatedWixOrder = await findWixOrderById(wixOrderId);
                const batchResponse = await getWixOrderFulfillmentsBatch([wixOrderId]);
                const orderFulfillmentData = batchResponse[0];
                fulfillments = (orderFulfillmentData && orderFulfillmentData.fulfillments) ? orderFulfillmentData.fulfillments : [];
                
                return res.status(200).json(mapWixOrderToMurkitResponse(updatedWixOrder || currentWixOrder, fulfillments, wixOrderId));
                
            } else {
                // NOT FULFILLED LOGIC
                const cancelResult = await cancelWixOrderById(wixOrderId);

                if (cancelResult.status === 409) {
                    let message = 'Cannot cancel order';
                    if (cancelResult.code === 'ORDER_ALREADY_CANCELED') message = 'Order already canceled';
                    else if (cancelResult.code === 'CANNOT_CANCEL_ORDER') message = 'Order already completed'; 
                    return res.status(409).json({ message: message, code: cancelResult.code });
                }
                
                if (cancelResult.status === 200) {
                    const wixOrder = await findWixOrderById(wixOrderId);
                    const batchResponse = await getWixOrderFulfillmentsBatch([wixOrderId]);
                    const orderFulfillmentData = batchResponse[0];
                    fulfillments = (orderFulfillmentData && orderFulfillmentData.fulfillments) ? orderFulfillmentData.fulfillments : [];
                    return res.status(200).json(mapWixOrderToMurkitResponse(wixOrder, fulfillments, wixOrderId));
                }
            }

        } catch (error) {
            console.error('PUT Cancel Order Error:', error);
            const status = error.status || 500; 
            return res.status(status).json({ message: 'Internal server error while processing cancellation request', code: 'INTERNAL_ERROR' });
        }
    }

    // --- 2. GET Order Endpoint ---
    const singleOrderPathMatch = urlPath.match(/\/orders\/([^/]+)$/);
    if (req.method === 'GET' && singleOrderPathMatch) {
        const wixOrderId = singleOrderPathMatch[1];
        try {
            const wixOrder = await findWixOrderById(wixOrderId);
            if (!wixOrder) return res.status(404).json({ message: 'Order does not exist', code: 'NOT_FOUND' });
            
            const batchResponse = await getWixOrderFulfillmentsBatch([wixOrderId]);
            const orderFulfillmentData = batchResponse[0];
            const fulfillments = (orderFulfillmentData && orderFulfillmentData.fulfillments) ? orderFulfillmentData.fulfillments : [];

            return res.status(200).json(mapWixOrderToMurkitResponse(wixOrder, fulfillments, wixOrderId));
        } catch (error) {
            return res.status(500).json({ message: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    }

    // --- 3. POST Order Batch Endpoint ---
    if (req.method === 'POST' && urlPath.includes('/orders/batch')) {
        let orderIds; 
        try {
            orderIds = req.body && req.body.orders;
            if (!Array.isArray(orderIds) || orderIds.length === 0) throw new Error();
        } catch (e) {
            return res.status(400).json({ message: 'Invalid body', code: 'BAD_REQUEST' });
        }

        const orderFetchResults = await Promise.all(orderIds.map(async (wixOrderId) => {
            try {
                const wixOrder = await findWixOrderById(wixOrderId);
                return { id: wixOrderId, order: wixOrder };
            } catch (error) {
                return { id: wixOrderId, error: { message: 'Internal Error', code: 'INTERNAL_ERROR' } };
            }
        }));

        const ordersToProcess = orderFetchResults.filter(r => r.order);
        const errors = orderFetchResults.filter(r => !r.order).map(r => r.error || { id: r.id, message: 'Not found', code: 'NOT_FOUND' });
        const idsToBatch = ordersToProcess.map(r => r.id);
        
        let batchFulfillmentMap = new Map();
        if (idsToBatch.length > 0) {
            try {
                const batchResponse = await getWixOrderFulfillmentsBatch(idsToBatch);
                if (Array.isArray(batchResponse)) {
                    batchResponse.forEach(ofd => {
                        if (ofd.orderId && Array.isArray(ofd.fulfillments)) {
                            batchFulfillmentMap.set(ofd.orderId, ofd.fulfillments);
                        }
                    });
                }
            } catch (e) {}
        }
        
        const responses = ordersToProcess.map(result => {
            const fulfillmentsForOrder = batchFulfillmentMap.get(result.id) || [];
            return mapWixOrderToMurkitResponse(result.order, fulfillmentsForOrder, result.id);
        });
        return res.status(200).json({ orders: responses, errors: errors });
    }

    // --- 4. POST LOGIC (Order Creation) ---
    if (req.method === 'POST') {
        if (urlPath.includes('/orders/')) return res.status(404).json({ message: 'Not Found' });

        try {
            const murkitData = req.body;
            if (!murkitData.number) throw createError(400, 'Missing order number');
            const murkitOrderId = String(murkitData.number);
            const cartNumber = murkitData.cartNumber ? String(murkitData.cartNumber) : null; 
            console.log(`Processing Murkit Order #${murkitOrderId}, Cart #${cartNumber}`);

            const existingOrder = await findWixOrderByExternalId(murkitOrderId);
            if (existingOrder) {
                return res.status(200).json({ "id": existingOrder.id }); 
            }

            const murkitItems = murkitData.items || [];
            if (murkitItems.length === 0) throw createError(400, 'No items in order');

            const currency = "UAH";
            const { sheets, spreadsheetId } = await ensureAuth(); 
            const { importValues, controlValues } = await readSheetData(sheets, spreadsheetId);
            const codeToSkuMap = getProductSkuMap(importValues, controlValues);
            
            const wixSkusToFetch = [];
            const itemsWithSku = murkitItems.map(item => {
                const mCode = String(item.code).trim();
                const wSku = codeToSkuMap[mCode] || mCode;
                if(wSku) wixSkusToFetch.push(wSku);
                return { ...item, wixSku: wSku };
            });

            if (wixSkusToFetch.length === 0) throw createError(400, 'No valid SKUs');

            const wixProducts = await getProductsBySkus(wixSkusToFetch);
            const skuMap = {};
            wixProducts.forEach(p => {
                const pSku = normalizeSku(p.sku);
                if (pSku) skuMap[pSku] = { type: 'product', product: p, variantData: null };
                if (p.variants) p.variants.forEach(v => {
                    const vSku = normalizeSku(v.variant?.sku);
                    if (vSku) skuMap[vSku] = { type: 'variant', product: p, variantData: v };
                });
            });

            const lineItems = [];
            const adjustments = []; 
            for (const item of itemsWithSku) {
                const requestedQty = parseInt(item.quantity || 1, 10);
                const targetSku = normalizeSku(item.wixSku); 
                const match = skuMap[targetSku];

                if (!match) throw createError(409, `Product ${item.code} not found`, "ITEM_NOT_FOUND");

                const foundProduct = match.product;
                const foundVariant = match.variantData; 

                let catalogItemId = foundProduct.id; 
                let variantId = null;
                let stockData = foundProduct.stock; 
                let productName = foundProduct.name;
                let variantChoices = null; 
                let descriptionLines = []; 
                
                if (foundVariant) {
                    variantId = foundVariant.variant.id; 
                    stockData = foundVariant.stock; 
                    if (foundVariant.choices) {
                        variantChoices = foundVariant.choices; 
                        descriptionLines = Object.entries(variantChoices).map(([k, v]) => ({
                            name: { original: k, translated: k },
                            plainText: { original: v, translated: v },
                            lineType: "PLAIN_TEXT"
                        }));
                    }
                } 

                if (stockData.inStock === false || (stockData.trackQuantity && stockData.quantity < requestedQty)) {
                     throw createError(409, `Product ${item.code} not enough stock`, "ITEM_NOT_AVAILABLE");
                }
                if (stockData.trackQuantity === true) {
                    adjustments.push({ productId: catalogItemId, variantId: variantId, quantity: requestedQty });
                }

                let imageObj = null;
                if (foundProduct.media?.mainMedia?.image) {
                    imageObj = {
                        url: foundProduct.media.mainMedia.image.url,
                        width: foundProduct.media.mainMedia.image.width,
                        height: foundProduct.media.mainMedia.image.height
                    };
                }

                const catalogRef = { catalogItemId: catalogItemId, appId: WIX_STORES_APP_ID };
                if (variantId) {
                    catalogRef.options = { variantId: variantId };
                    if (variantChoices) catalogRef.options.options = variantChoices;
                }

                const lineItem = {
                    quantity: requestedQty,
                    catalogReference: catalogRef,
                    productName: { original: productName },
                    descriptionLines: descriptionLines, 
                    itemType: { preset: "PHYSICAL" },
                    physicalProperties: { sku: targetSku, shippable: true },
                    price: { amount: fmtPrice(item.price) },
                    taxDetails: { taxRate: "0", totalTax: { amount: "0.00", currency: currency } }
                };
                if (imageObj) lineItem.image = imageObj;
                lineItems.push(lineItem);
            }

            const clientName = getFullName(murkitData.client?.name);
            const recipientName = getFullName(murkitData.recipient?.name);
            const clientPhone = String(murkitData.client?.phone || "").replace(/\D/g,''); 
            const recipientPhone = String(murkitData.recipient?.phone || murkitData.client?.phone || "").replace(/\D/g,'');
            
            let email = murkitData.client?.email;
            if (!email || !email.includes('@')) {
                const phoneForId = clientPhone || recipientPhone || "0000000000";
                email = `client.${phoneForId}@no-email.monomarket.com`;
                console.log(`Generated fake email for client: ${email}`);
            }

            const priceSummary = {
                subtotal: { amount: fmtPrice(murkitData.sum), currency },
                shipping: { amount: "0.00", currency }, 
                tax: { amount: "0.00", currency },
                discount: { amount: "0.00", currency },
                total: { amount: fmtPrice(murkitData.sum), currency }
            };

            const d = murkitData.delivery || {}; 
            const deliveryType = String(murkitData.deliveryType || '').toLowerCase(); 
            const npCity = String(d.settlement || d.city || d.settlementName || '').trim();
            const street = String(d.address || '').trim();
            const house = String(d.house || '').trim();
            const flat = String(d.flat || '').trim();
            const npWarehouse = String(d.warehouseNumber || '').trim();

            let extendedFields = {};
            let finalAddressLine = "невідома адреса";
            let deliveryTitle = "Delivery";

            if (deliveryType.includes('courier')) {
                deliveryTitle = SHIPPING_TITLES.COURIER; 
                const addressParts = [];
                if (street) addressParts.push(street);
                if (house) addressParts.push(`буд. ${house}`);
                if (flat) addressParts.push(`кв. ${flat}`);
                finalAddressLine = addressParts.length > 0 ? addressParts.join(', ') : `Адресна доставка (${npCity})`;
            } else if (deliveryType.includes('postomat')) {
                deliveryTitle = SHIPPING_TITLES.POSTOMAT; 
                finalAddressLine = npWarehouse ? `Нова Пошта Поштомат №${npWarehouse}` : "Нова Пошта Поштомат";
                if(npWarehouse) extendedFields = { "namespaces": { "_user_fields": { "nomer_viddilennya_poshtomatu_novoyi_poshti": npWarehouse } } };
            } else {
                deliveryTitle = SHIPPING_TITLES.BRANCH; 
                finalAddressLine = npWarehouse ? `Нова Пошта №${npWarehouse}` : "Нова Пошта";
                if(npWarehouse) extendedFields = { "namespaces": { "_user_fields": { "nomer_viddilennya_poshtomatu_novoyi_poshti": npWarehouse } } };
            }

            const shippingAddress = { country: "UA", city: npCity || "City", addressLine: finalAddressLine, postalCode: "00000" };


            
const customFields = [];
            
            let combinedIdValue = "";
            if (cartNumber && murkitOrderId) {
                combinedIdValue = `${cartNumber} / ${murkitOrderId}`;
            } else if (cartNumber) {
                combinedIdValue = cartNumber;
            } else if (murkitOrderId) {
                combinedIdValue = murkitOrderId;
            }

            if (combinedIdValue) {
                customFields.push({
                    title: "Monomarket Cart / Order ID", 
                    value: String(combinedIdValue)
                });
            }

            const wixOrderPayload = {
                channelInfo: { type: "WEB", externalOrderId: murkitOrderId },
                status: "APPROVED",
                lineItems: lineItems,
                priceSummary: priceSummary,
                billingInfo: {
                    address: shippingAddress, 
                    contactDetails: { firstName: clientName.firstName, lastName: clientName.lastName, phone: clientPhone, email: email }
                },
                shippingInfo: {
                    title: deliveryTitle,
                    logistics: { shippingDestination: { address: shippingAddress, contactDetails: { firstName: recipientName.firstName, lastName: recipientName.lastName, phone: recipientPhone } } },
                    cost: { price: { amount: "0.00", currency } }
                },
                buyerInfo: { email: email },
                currency: currency,
                weightUnit: "KG",
                taxIncludedInPrices: false,
                ...(Object.keys(extendedFields).length > 0 ? { extendedFields } : {}),
                ...(customFields.length > 0 ? { customFields } : {}) 
            };

            const createdOrder = await createWixOrder(wixOrderPayload);
            const newOrderId = createdOrder.order?.id;

            if (newOrderId && createdOrder.order?.priceSummary?.total?.amount) {
                const wixTotalAmount = createdOrder.order.priceSummary.total.amount;
                try {
                    await addExternalPayment(newOrderId, wixTotalAmount, currency, murkitData.date);
                } catch (payErr) {
                    console.error(`Warning: Failed to add payment to order ${newOrderId}`, payErr);
                }
            }
            if (adjustments.length > 0) {
                try { await adjustInventory(adjustments); } catch (adjErr) { console.error("Warning: Inventory adjustment failed", adjErr); }
            }
            return res.status(201).json({ "id": newOrderId }); 
        } catch (e) {
            console.error('Murkit Webhook Error:', e.message);
            const status = e.status || 500;
            if (status === 409) return res.status(409).json({ message: e.message, code: e.code });
            return res.status(status).json({ error: e.message }); 
        }
    }

    return res.status(404).json({ message: 'Not Found' });
}
