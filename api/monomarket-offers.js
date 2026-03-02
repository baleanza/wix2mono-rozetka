import { google } from 'googleapis';
import { getSheetsClient } from '../lib/sheetsClient.js';
import { getDriveClient } from '../lib/driveClient.js';
import { buildOffersXml } from '../lib/feedBuilder.js';
import { getInventoryBySkus } from '../lib/wixClient.js';

// Настройки кеширования на Google Drive
const CACHE_TTL_SECONDS = parseInt(process.env.CACHE_TTL_SECONDS || '7200', 10);
const DRIVE_FILE_NAME = 'monomarket-offers.xml';
const SHARED_DRIVE_FOLDER_ID = process.env.SHARED_DRIVE_FOLDER_ID || null;

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

function requireEnv(varName) {
  const value = process.env[varName];
  if (!value) {
    console.error(`Missing required env var: ${varName}`);
  }
  return value;
}

// ensureAuth теперь объединяет sheets и drive авторизацию
async function ensureAuth() {
  const keyJson = requireEnv('GOOGLE_SERVICE_ACCOUNT_KEY');
  const spreadsheetId = requireEnv('SPREADSHEET_ID');
  if (!keyJson || !spreadsheetId) {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY or SPREADSHEET_ID not configured');
  }

  let keyObj;
  try {
    keyObj = JSON.parse(keyJson);
  } catch (e) {
    console.error('Failed to parse GOOGLE_SERVICE_ACCOUNT_KEY JSON', e);
    throw e;
  }

  const jwtClient = new google.auth.JWT(
    keyObj.client_email,
    null,
    keyObj.private_key,
    [
      'https://www.googleapis.com/auth/spreadsheets.readonly',
      'https://www.googleapis.com/auth/drive.file'
    ]
  );

  await jwtClient.authorize();

  const sheets = getSheetsClient(jwtClient);
  const drive = getDriveClient(jwtClient);

  return { sheets, drive, spreadsheetId };
}

async function getOrCreateDriveFile(drive) {
  if (!SHARED_DRIVE_FOLDER_ID) {
    throw new Error('SHARED_DRIVE_FOLDER_ID is not set');
  }

  const res = await drive.files.list({
    q: [
      `name='${DRIVE_FILE_NAME}'`,
      `'${SHARED_DRIVE_FOLDER_ID}' in parents`, 
      'trashed = false'
    ].join(' and '),
    fields: 'files(id, name, modifiedTime)',
    spaces: 'drive',
    includeItemsFromAllDrives: true,
    supportsAllDrives: true
  });

  const files = res.data.files || [];
  if (files.length > 0) {
    return files[0];
  }

  const createRes = await drive.files.create({
    requestBody: {
      name: DRIVE_FILE_NAME,
      mimeType: 'application/xml',
      parents: [SHARED_DRIVE_FOLDER_ID]
    },
    supportsAllDrives: true
  });

  return createRes.data;
}

async function readDriveFileContent(drive, fileId) {
  const res = await drive.files.get(
    {
      fileId,
      alt: 'media',
      supportsAllDrives: true
    },
    { responseType: 'arraybuffer' }
  );
  return Buffer.from(res.data).toString('utf-8');
}

async function writeDriveFileContent(drive, fileId, xml) {
  await drive.files.update({
    fileId,
    media: {
      mimeType: 'application/xml',
      body: xml
    },
    supportsAllDrives: true
  });
}

// **** УСИЛЕННАЯ ФУНКЦИЯ ЧТЕНИЯ ДАННЫХ ИЗ SHEETS ****
async function readSheetData(sheets, spreadsheetId) {
  // Параллельное чтение трех диапазонов
  const [importRes, controlRes, deliveryRes] = await Promise.all([
    sheets.spreadsheets.values.get({ spreadsheetId, range: 'Import!A1:ZZ' }),
    sheets.spreadsheets.values.get({ spreadsheetId, range: 'Feed Control List!A1:F' }),
    sheets.spreadsheets.values.get({ spreadsheetId, range: 'Delivery!A1:C' })
  ]);
  
  // Гарантируем, что возвращаем пустой массив, а не null/undefined
  return { 
    importValues: importRes.data.values || [], 
    controlValues: controlRes.data.values || [],
    deliveryValues: deliveryRes.data.values || [] 
  };
}
// *************************************************

function isFresh(modifiedTime) {
  if (!modifiedTime) return false;
  const modified = new Date(modifiedTime).getTime();
  const now = Date.now();
  return now - modified < CACHE_TTL_SECONDS * 1000;
}

// Вспомогательная функция для получения данных о запасах
async function getInventory(importValues, controlValues) {
    const headers = importValues[0] || [];
    const rows = importValues.slice(1);
    const controlHeaders = controlValues[0] || [];
    const controlRows = controlValues.slice(1);

    const idxImportField = controlHeaders.indexOf('Import field');
    const idxFeedName = controlHeaders.indexOf('Feed name');
    
    const fieldMapping = {};
    controlRows.forEach(row => {
        const importField = row[idxImportField];
        const feedName = row[idxFeedName];
        if (importField && feedName) {
            fieldMapping[String(feedName).trim()] = String(importField).trim();
        }
    });

    const skuSheetHeader = fieldMapping['sku'] || 'SKU';
    const skuHeaderIndex = headers.indexOf(skuSheetHeader);
    
    if (skuHeaderIndex === -1) {
        console.warn(`SKU column '${skuSheetHeader}' not found in Import sheet.`);
        return { inventoryMap: {} };
    }

    const skus = [];
    rows.forEach(row => {
        const sku = row[skuHeaderIndex] ? String(row[skuHeaderIndex]).trim() : '';
        if (sku) skus.push(sku);
    });

    const uniqueSkus = [...new Set(skus)];

    const inventory = await getInventoryBySkus(uniqueSkus);
    
    const inventoryMap = {};
    inventory.forEach(item => {
        inventoryMap[String(item.sku).trim()] = item;
    });
    
    return { inventoryMap };
}


export default async function handler(req, res) {
  if (req.method !== 'GET') {
    res.status(405).send('Method Not Allowed');
    return;
  }
  
triggerBackgroundStockUpdate(req);
  
  // Авторизация удалена, доступ публичный

  try {
    const { sheets, drive, spreadsheetId } = await ensureAuth();

    const fileMeta = await getOrCreateDriveFile(drive);

    // **** ЛОГИКА КЕШИРОВАНИЯ НА DRIVE ****
    if (fileMeta.id && isFresh(fileMeta.modifiedTime)) {
      try {
        const xml = await readDriveFileContent(drive, fileMeta.id);

        res.setHeader('Content-Type', "application/xml; charset=utf-8");
        res.setHeader(
          'Cache-Control',
          `public, s-maxage=${CACHE_TTL_SECONDS}, max-age=0`
        );
        res.status(200).send(xml);
        return; 
      } catch (e) {
        console.error('Failed to read cached XML from Drive, will regenerate', e);
      }
    }
    // *************************************
    
    // Если кеш отсутствует или устарел, генерируем новый
    const { importValues, controlValues, deliveryValues } = await readSheetData(
      sheets,
      spreadsheetId
    );
    
    // Если importValues все еще пуст, значит, таблица пуста или проблема с диапазоном
    if (importValues.length === 0) {
        throw new Error("Import sheet is empty or failed to load data.");
    }
    
    const { inventoryMap } = await getInventory(importValues, controlValues);

    const xml = buildOffersXml(importValues, controlValues, deliveryValues, inventoryMap);

    // Записываем новый XML-файл в Drive
    if (fileMeta.id) {
      try {
        await writeDriveFileContent(drive, fileMeta.id, xml);
      } catch (e) {
        console.error('Failed to write XML to Drive', e);
      }
    }

    // Отправляем сгенерированный файл
    res.setHeader('Content-Type', "application/xml; charset=utf-8");
    res.setHeader(
      'Cache-Control',
      `public, s-maxage=${CACHE_TTL_SECONDS}, max-age=0`
    );
    res.status(200).send(xml);
  } catch (err) {
    console.error('Error in /api/monomarket-offers', err);
    // Изменяем ответ на 502, чтобы увидеть сообщение об ошибке, если sheets.get не сработает
    res.status(502).send('Bad Gateway: ' + err.message);
  }
}
