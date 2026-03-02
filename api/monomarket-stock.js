// api/monomarket-stock.js
import { google } from 'googleapis';
import { getInventoryBySkus } from '../lib/wixClient.js';
import { buildStockJson } from '../lib/stockFeedBuilder.js'; 
import { requireEnv, cleanPrice, getSheetsClient } from '../lib/sheetsClient.js';
import { getDriveClient } from '../lib/driveClient.js';


const CACHE_TTL_SECONDS = 300;

const DRIVE_FILE_NAME = 'monomarket-stock.json';
const SHARED_DRIVE_FOLDER_ID = process.env.SHARED_DRIVE_FOLDER_ID || null;

async function ensureAuth() {
  const keyJson = requireEnv('GOOGLE_SERVICE_ACCOUNT_KEY');
  const spreadsheetId = requireEnv('SPREADSHEET_ID');
  const keyObj = JSON.parse(keyJson);

  const jwtClient = new google.auth.JWT(
    keyObj.client_email, null, keyObj.private_key,
    [
      'https://www.googleapis.com/auth/spreadsheets.readonly',
      'https://www.googleapis.com/auth/drive.file'
    ]
  );
  await jwtClient.authorize();
  return { 
    sheets: getSheetsClient(jwtClient), 
    drive: getDriveClient(jwtClient), 
    spreadsheetId 
  };
}

async function getOrCreateDriveFile(drive) {
  if (!SHARED_DRIVE_FOLDER_ID) throw new Error('SHARED_DRIVE_FOLDER_ID is not set');
  const res = await drive.files.list({
    q: [`name='${DRIVE_FILE_NAME}'`, `'${SHARED_DRIVE_FOLDER_ID}' in parents`, 'trashed = false'].join(' and '),
    fields: 'files(id, name, modifiedTime)', spaces: 'drive', includeItemsFromAllDrives: true, supportsAllDrives: true
  });
  if (res.data.files && res.data.files.length > 0) return res.data.files[0];
  const createRes = await drive.files.create({
    requestBody: { name: DRIVE_FILE_NAME, mimeType: 'application/json', parents: [SHARED_DRIVE_FOLDER_ID] },
    supportsAllDrives: true
  });
  return createRes.data;
}

async function readDriveFileContent(drive, fileId) {
  const res = await drive.files.get({ fileId, alt: 'media', supportsAllDrives: true }, { responseType: 'arraybuffer' });
  return Buffer.from(res.data).toString('utf-8');
}

async function writeDriveFileContent(drive, fileId, jsonString) {
  await drive.files.update({ fileId, media: { mimeType: 'application/json', body: jsonString }, supportsAllDrives: true });
}

function isFresh(modifiedTime) {
  if (!modifiedTime) return false;
  return Date.now() - new Date(modifiedTime).getTime() < CACHE_TTL_SECONDS * 1000;
}

// Функция checkApiKey удалена

async function readSheetData(sheets, spreadsheetId) {
  const importRes = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'Import!A1:ZZ'
  });

  const controlRes = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'Feed Control List!A1:F'
  });

  const deliveryRes = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'Delivery!A1:C'
  });

  return { 
    importValues: importRes.data.values || [], 
    controlValues: controlRes.data.values || [],
    deliveryValues: deliveryRes.data.values || [] 
  };
}

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    res.status(405).send('Method Not Allowed');
    return;
  }

  const forceUpdate = req.query.forceUpdate === 'true'; // Принимаем параметр для фонового обновления

  try {
    const { sheets, drive, spreadsheetId } = await ensureAuth();
    const fileMeta = await getOrCreateDriveFile(drive);

    // Если кэш свежий и мы не запрашиваем принудительное обновление
    if (!forceUpdate && fileMeta.id && isFresh(fileMeta.modifiedTime)) {
      try {
        const jsonOutput = await readDriveFileContent(drive, fileMeta.id);
        res.setHeader('Content-Type', "application/json; charset=utf-8");
        res.setHeader('Cache-Control', `public, s-maxage=${CACHE_TTL_SECONDS}, max-age=0`);
        return res.status(200).send(jsonOutput);
      } catch (e) {
        console.error('Failed to read cached JSON from Drive', e);
      }
    }

    // Генерируем новые данные
    const { importValues, controlValues, deliveryValues } = await readSheetData(sheets, spreadsheetId);
    const jsonOutput = await buildStockJson(importValues, controlValues, deliveryValues, getInventoryBySkus, cleanPrice);

    // Записываем результат на Диск
    if (fileMeta.id) {
      try {
        await writeDriveFileContent(drive, fileMeta.id, jsonOutput);
      } catch (e) {
        console.error('Failed to write JSON to Drive', e);
      }
    }

    res.setHeader('Content-Type', "application/json; charset=utf-8");
    res.setHeader('Cache-Control', `public, s-maxage=${CACHE_TTL_SECONDS}, max-age=0`);
    res.status(200).send(jsonOutput);
    
  } catch (e) {
    console.error('Error in /api/monomarket-stock', e);
    res.status(502).json({ error: 'Bad Gateway', details: e.message });
  }
}
