// control-tower/src/lib/contacts/state.ts
const STATE_ABBR_TO_NAME: Record<string, string> = {
    AL: "Alabama", AK: "Alaska", AZ: "Arizona", AR: "Arkansas", CA: "California",
    CO: "Colorado", CT: "Connecticut", DE: "Delaware", DC: "District of Columbia",
    FL: "Florida", GA: "Georgia", HI: "Hawaii", ID: "Idaho", IL: "Illinois",
    IN: "Indiana", IA: "Iowa", KS: "Kansas", KY: "Kentucky", LA: "Louisiana",
    ME: "Maine", MD: "Maryland", MA: "Massachusetts", MI: "Michigan", MN: "Minnesota",
    MS: "Mississippi", MO: "Missouri", MT: "Montana", NE: "Nebraska", NV: "Nevada",
    NH: "New Hampshire", NJ: "New Jersey", NM: "New Mexico", NY: "New York",
    NC: "North Carolina", ND: "North Dakota", OH: "Ohio", OK: "Oklahoma",
    OR: "Oregon", PA: "Pennsylvania", RI: "Rhode Island", SC: "South Carolina",
    SD: "South Dakota", TN: "Tennessee", TX: "Texas", UT: "Utah", VT: "Vermont",
    VA: "Virginia", WA: "Washington", WV: "West Virginia", WI: "Wisconsin",
    WY: "Wyoming", PR: "Puerto Rico",
};

function norm(v: any) {
    return String(v ?? "").trim();
}

export function normalizeStateName(raw: any) {
    const s = norm(raw);
    if (!s) return "";
    const up = s.toUpperCase();

    if (STATE_ABBR_TO_NAME[up]) return STATE_ABBR_TO_NAME[up];
    if (up === "PUERTO RICO") return "Puerto Rico";
    if (up === "PR") return "Puerto Rico";

    // si viene "Florida" ya bien, lo dejamos
    return s;
}

function pickFirst(...vals: any[]) {
    for (const v of vals) {
        const s = norm(v);
        if (s) return s;
    }
    return "";
}

export function extractStateFromContact(contact: any) {
    return pickFirst(
        contact?.state,
        contact?.address?.state,
        contact?.contact?.state,
        contact?.contact?.address?.state,
        contact?.location?.state,
    );
}

// Heurística: source contiene el nombre/abbr del estado
export function extractStateFromOpportunitySource(source: any) {
    const s = norm(source);
    if (!s) return "";

    // match exact abbr tokens (FL, TX, etc)
    for (const abbr of Object.keys(STATE_ABBR_TO_NAME)) {
        const re = new RegExp(`\\b${abbr}\\b`, "i");
        if (re.test(s)) return abbr;
    }

    // match full state names
    for (const name of Object.values(STATE_ABBR_TO_NAME)) {
        const re = new RegExp(`\\b${name}\\b`, "i");
        if (re.test(s)) return name;
    }

    return "";
}

export function isGuestName(name: any) {
    const n = norm(name).toLowerCase();
    return n.startsWith("guest");
}

export function isLikelyChatGuest(contact: any) {
    const name = pickFirst(contact?.name, contact?.contactName, contact?.fullName);
    if (!isGuestName(name)) return false;

    const email = pickFirst(contact?.email, contact?.contact?.email);
    const phone = pickFirst(contact?.phone, contact?.phoneNumber, contact?.contact?.phone);

    // “casi lead”: guest + sin email/phone
    return !email && !phone;
}
