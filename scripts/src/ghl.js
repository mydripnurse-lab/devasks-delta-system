// scripts/src/ghl.js
import axios from "axios";

export function makeGhlClient() {
    // Stub: si no tienes .env listo, no rompas.
    const baseURL = process.env.GHL_BASE_URL || "https://services.leadconnectorhq.com";
    const apiKey = process.env.GHL_API_KEY || "STUB_NO_KEY";
    const version = process.env.GHL_VERSION || "2021-07-28";

    return axios.create({
        baseURL,
        timeout: 30000,
        headers: {
            Authorization: `Bearer ${apiKey}`,
            Version: version,
            "Content-Type": "application/json",
            Accept: "application/json",
        },
    });
}

// Stub: NO llama a GHL todavía. Solo simula una respuesta.
export async function createSubaccount(client, payload) {
    return {
        stub: true,
        message: "GHL not configured yet",
        payload,
    };
}
