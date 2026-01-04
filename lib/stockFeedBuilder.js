// lib/stockFeedBuilder.js
function getDaysToDispatch() {
  const now = new Date();
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Europe/Kyiv',
    hour: 'numeric',
    weekday: 'short',
    hour12: false
  });
  
  const parts = formatter.formatToParts(now);
  const hourPart = parts.find(p => p.type === 'hour').value;
  const weekdayPart = parts.find(p => p.type === 'weekday').value;
  const hour = parseInt(hourPart, 10);

  if (weekdayPart === 'Fri' && hour >= 14) return 3;
  if (weekdayPart === 'Sat') return 2;
  if (weekdayPart === 'Sun') return 1;
  
  if (hour < 14) return 0;
  return 1;
}

function parseDeliveryMethods(deliveryValues) {
  const rows = deliveryValues.slice(1);
  const methods = [];
  rows.forEach(row => {
    const method = row[0] ? String(row[0]).trim() : '';
    const isActiveRaw = row[1] ? String(row[1]).trim().toLowerCase() : 'false';
    const priceRaw = row[2] ? String(row[2]).trim() : '0';
    if (method && ['true', '1', 'yes', 'так'].includes(isActiveRaw)) {
      methods.push({ method, price: Number(priceRaw) || 0 });
    }
  });
  return methods;
}

export async function buildStockJson(importValues, controlValues, deliveryValues, getInventoryBySkus, cleanPrice) {
  const daysToDispatch = getDaysToDispatch();
  const deliveryMethods = parseDeliveryMethods(deliveryValues || []);
  
  const headers = importValues[0] || [];
  const rows = importValues.slice(1);
  const controlHeaders = controlValues[0] || [];
  const controlRows = controlValues.slice(1);

  const idxImportField = controlHeaders.indexOf('Import field');
  const idxStock = controlHeaders.indexOf('Stock feed');
  const idxFeedName = controlHeaders.indexOf('Feed name');

  const fieldMapping = {};
  controlRows.slice(1).forEach(row => {
    const importField = row[idxImportField];
    const stockEnabledRaw = row[idxStock];
    const jsonName = row[idxFeedName];
    if (!importField || !jsonName) return;
    const isEnabled = stockEnabledRaw && !['false', '0', 'no', 'ni', ''].includes(String(stockEnabledRaw).toLowerCase());
    if (isEnabled) fieldMapping[String(importField).trim()] = String(jsonName).trim();
  });

  const skuHeaderIndex = headers.indexOf(fieldMapping['sku'] || 'SKU');
  if (skuHeaderIndex === -1) return JSON.stringify({ total: 0, data: [] });

  const skus = [];
  const rowBySku = {};

  rows.forEach(row => {
    const sku = row[skuHeaderIndex] ? String(row[skuHeaderIndex]).trim() : '';
    if (!sku) return;
    skus.push(sku);
    rowBySku[sku] = row;
  });

  const uniqueSkus = [...new Set(skus)];
  const inventory = await getInventoryBySkus(uniqueSkus);
  
  const inventoryBySku = {};
  inventory.forEach(item => {
    inventoryBySku[String(item.sku).trim()] = item;
  });

  const offersData = uniqueSkus.map(sku => {
    const row = rowBySku[sku];
    const wixItem = inventoryBySku[sku];
    const rawData = {};

    headers.forEach((header, colIdx) => {
        const jsonKey = fieldMapping[header];
        if (jsonKey) {
            let val = row[colIdx];
            if (val !== undefined && val !== '') rawData[jsonKey] = val;
        }
    });
    
    let price = 0;
    let oldPrice = null;
    
    if (wixItem && wixItem.price) {
        price = wixItem.price; // Цена из Wix (число)
    } else {
        // Фоллбэк: если товара нет в Wix, но есть в таблице, пытаемся взять из таблицы
        price = rawData['price'] ? cleanPrice(rawData['price']) : 0;
    }
    
    if (rawData['old_price']) {
        oldPrice = cleanPrice(rawData['old_price']);
        if (oldPrice === 0 || oldPrice === price) oldPrice = null;
    }


    let warrantyPeriod = rawData['warranty_period'];
    if (warrantyPeriod) warrantyPeriod = parseInt(String(warrantyPeriod).replace(/[^0-9]/g, ''), 10);

    let isAvailable = false;
    if (wixItem && wixItem.inStock === true) {
        isAvailable = true;
    }

    const finalOffer = {
        code: rawData['code'],
        price: price,
        old_price: oldPrice,
        availability: isAvailable,
        warranty_period: warrantyPeriod,
        warranty_type: "manufacturer",
        max_pay_in_parts: 3,
        days_to_dispatch: daysToDispatch,
        delivery_methods: deliveryMethods
    };
    
    // Добавляем остальные поля из таблицы
    Object.keys(rawData).forEach(key => {
        if (!finalOffer.hasOwnProperty(key)) {
            finalOffer[key] = rawData[key];
        }
    });

    return finalOffer;

  }).filter(o => o.price > 0);

  return JSON.stringify({
      total: offersData.length,
      data: offersData
  }, null, 2);
}
