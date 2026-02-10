// control-tower/src/lib/ghlState.ts
export const STATE_ABBR_TO_NAME: Record<string, string> = {
    AL: "Alabama",
    AK: "Alaska",
    AZ: "Arizona",
    AR: "Arkansas",
    CA: "California",
    CO: "Colorado",
    CT: "Connecticut",
    DE: "Delaware",
    DC: "District of Columbia",
    FL: "Florida",
    GA: "Georgia",
    HI: "Hawaii",
    ID: "Idaho",
    IL: "Illinois",
    IN: "Indiana",
    IA: "Iowa",
    KS: "Kansas",
    KY: "Kentucky",
    LA: "Louisiana",
    ME: "Maine",
    MD: "Maryland",
    MA: "Massachusetts",
    MI: "Michigan",
    MN: "Minnesota",
    MS: "Mississippi",
    MO: "Missouri",
    MT: "Montana",
    NE: "Nebraska",
    NV: "Nevada",
    NH: "New Hampshire",
    NJ: "New Jersey",
    NM: "New Mexico",
    NY: "New York",
    NC: "North Carolina",
    ND: "North Dakota",
    OH: "Ohio",
    OK: "Oklahoma",
    OR: "Oregon",
    PA: "Pennsylvania",
    RI: "Rhode Island",
    SC: "South Carolina",
    SD: "South Dakota",
    TN: "Tennessee",
    TX: "Texas",
    UT: "Utah",
    VT: "Vermont",
    VA: "Virginia",
    WA: "Washington",
    WV: "West Virginia",
    WI: "Wisconsin",
    WY: "Wyoming",
    PR: "Puerto Rico",
};

export function norm(v: any) {
    return String(v ?? "").trim();
}

export function normalizeStateName(raw: any) {
    const s = norm(raw);
    if (!s) return "";
    const up = s.toUpperCase();
    const lower = s.toLowerCase();

    if (STATE_NAME_TO_CANONICAL[lower]) return STATE_NAME_TO_CANONICAL[lower];
    if (STATE_ABBR_TO_NAME[up]) return STATE_ABBR_TO_NAME[up];
    if (up === "PUERTO RICO" || up === "PR") return "Puerto Rico";

    // a veces viene "Florida (FL)" o "FL - Florida"
    const m = up.match(/\b([A-Z]{2})\b/);
    if (m?.[1] && STATE_ABBR_TO_NAME[m[1]]) return STATE_ABBR_TO_NAME[m[1]];

    return s;
}

const STATE_NAMES = Object.values(STATE_ABBR_TO_NAME);
const STATE_NAME_TO_CANONICAL: Record<string, string> = Object.fromEntries(
    STATE_NAMES.map((name) => [name.toLowerCase(), name]),
);

export function inferStateFromText(text: string) {
    const t = norm(text);
    if (!t) return "";

    // match state name
    for (const name of STATE_NAMES) {
        const re = new RegExp(`\\b${escapeRegExp(name)}\\b`, "i");
        if (re.test(t)) return name;
    }

    // match abbreviation
    const up = t.toUpperCase();
    const m = up.match(/\b([A-Z]{2})\b/);
    if (m?.[1] && STATE_ABBR_TO_NAME[m[1]]) return STATE_ABBR_TO_NAME[m[1]];

    return "";
}

function escapeRegExp(s: string) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
