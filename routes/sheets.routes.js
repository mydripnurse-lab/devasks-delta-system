// routes/sheets.routes.js
import {
    loadSheetIndex,
    updateLocationIdInRow,
    appendRow,
    buildRowFromHeaders,
    createSpreadsheet,
    ensureSheetTab,
    ensureHeaderRow,
} from "../services/sheetsClient.js";

function envOrThrow(name) {
    const v = process.env[name];
    if (!v) throw new Error(`Missing env var: ${name}`);
    return v;
}

function json(res, status, payload) {
    res.status(status).json(payload);
}

// Si tu router llama handlers con (req,res) estilo Express:
export const sheetsRoutes = {
    /**
     * GET /sheets/index
     * Lee el sheet y devuelve: headers + index por Account Name
     */
    async getIndex(req, res) {
        try {
            const spreadsheetId = req.query.spreadsheetId || envOrThrow("GOOGLE_SHEET_ID");
            const sheetName = req.query.sheetName || envOrThrow("GOOGLE_SHEET_TAB");

            const index = await loadSheetIndex({
                spreadsheetId,
                sheetName,
                range: req.query.range || "A:Z",
                accountNameHeader: "Account Name",
                locationIdHeader: "Location Id",
            });

            // Map no se serializa bien: lo convertimos
            json(res, 200, {
                ok: true,
                sheetName: index.sheetName,
                range: index.range,
                headers: index.headers,
                accountNameCol: index.accountNameCol,
                locationIdCol: index.locationIdCol,
                // resumen
                totalRows: index.rows.length,
                indexedAccounts: Array.from(index.mapByAccountName.keys()).length,
            });
        } catch (e) {
            json(res, 500, { ok: false, error: e?.message || String(e) });
        }
    },

    /**
     * POST /sheets/location-id
     * body: { rowNumber, locationId, spreadsheetId?, sheetName? }
     */
    async setLocationId(req, res) {
        try {
            const spreadsheetId = req.body?.spreadsheetId || envOrThrow("GOOGLE_SHEET_ID");
            const sheetName = req.body?.sheetName || envOrThrow("GOOGLE_SHEET_TAB");

            const rowNumber = Number(req.body?.rowNumber);
            const locationId = String(req.body?.locationId || "").trim();
            if (!rowNumber) throw new Error("rowNumber is required");
            if (!locationId) throw new Error("locationId is required");

            // necesitamos locationIdColIndex0 => lo sacamos del index (headers)
            const idx = await loadSheetIndex({ spreadsheetId, sheetName, range: "A:Z" });

            const updated = await updateLocationIdInRow({
                spreadsheetId,
                sheetName,
                locationIdColIndex0: idx.locationIdCol,
                rowNumber,
                locationId,
            });

            json(res, 200, { ok: true, ...updated });
        } catch (e) {
            json(res, 500, { ok: false, error: e?.message || String(e) });
        }
    },

    /**
     * POST /sheets/append
     * body: { data: { "Header Name": value, ... }, spreadsheetId?, sheetName? }
     * - construye la fila con headers existentes y hace append
     */
    async append(req, res) {
        try {
            const spreadsheetId = req.body?.spreadsheetId || envOrThrow("GOOGLE_SHEET_ID");
            const sheetName = req.body?.sheetName || envOrThrow("GOOGLE_SHEET_TAB");
            const data = req.body?.data || null;
            if (!data || typeof data !== "object") throw new Error("body.data object is required");

            const idx = await loadSheetIndex({ spreadsheetId, sheetName, range: "A:Z" });
            const valuesArray = buildRowFromHeaders(idx.headers, data);

            const out = await appendRow({ spreadsheetId, sheetName, valuesArray });
            json(res, 200, { ok: true, ...out });
        } catch (e) {
            json(res, 500, { ok: false, error: e?.message || String(e) });
        }
    },

    /**
     * POST /sheets/create
     * body: { title, tabName?, headers? }
     * Crea spreadsheet + tab + headers y te devuelve el spreadsheetId
     */
    async create(req, res) {
        try {
            const title = String(req.body?.title || "").trim();
            if (!title) throw new Error("title is required");

            const tabName = String(req.body?.tabName || "Sheet1");
            const headers = Array.isArray(req.body?.headers) && req.body.headers.length
                ? req.body.headers
                : ["Account Name", "Location Id"];

            const created = await createSpreadsheet({ title });
            const spreadsheetId = created.spreadsheetId;

            await ensureSheetTab({ spreadsheetId, sheetName: tabName });
            await ensureHeaderRow({ spreadsheetId, sheetName: tabName, headers });

            json(res, 200, {
                ok: true,
                spreadsheetId,
                spreadsheetUrl: created.spreadsheetUrl,
                tabName,
                headers,
            });
        } catch (e) {
            json(res, 500, { ok: false, error: e?.message || String(e) });
        }
    },
};
