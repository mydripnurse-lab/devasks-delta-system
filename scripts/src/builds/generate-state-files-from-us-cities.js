import fs from "fs/promises";
import path from "path";

const ROOT = process.cwd();
const DATASET_PATH = path.join(ROOT, "tmp", "data", "us_cities_states_counties.csv");
const OUT_DIR = path.join(ROOT, "resources", "statesFiles");

const TARGET_STATES = [
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
];

const STATE_ABBR = {
    Pennsylvania: "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    Tennessee: "TN",
    Texas: "TX",
    Utah: "UT",
    Vermont: "VT",
    Virginia: "VA",
    Washington: "WA",
    "West Virginia": "WV",
    Wisconsin: "WI",
    Wyoming: "WY",
};

const DEFAULT_TZ_BY_STATE = {
    Pennsylvania: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    "Rhode Island": { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    "South Carolina": { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    "South Dakota": { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Tennessee: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Texas: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Utah: { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" },
    Vermont: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Virginia: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Washington: { Name: "Pacific", Zone: "US/Pacific", UTC_Offset: "-08:00" },
    "West Virginia": { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Wisconsin: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Wyoming: { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" },
};

const SD_MOUNTAIN_COUNTIES = new Set([
    "Bennett",
    "Butte",
    "Corson",
    "Custer",
    "Dewey",
    "Fall River",
    "Haakon",
    "Harding",
    "Jackson",
    "Lawrence",
    "Meade",
    "Oglala Lakota",
    "Pennington",
    "Perkins",
    "Stanley",
    "Ziebach",
]);

const TN_EASTERN_COUNTIES = new Set([
    "Anderson",
    "Blount",
    "Bradley",
    "Campbell",
    "Carter",
    "Claiborne",
    "Cocke",
    "Grainger",
    "Greene",
    "Hamblen",
    "Hamilton",
    "Hancock",
    "Hawkins",
    "Jefferson",
    "Johnson",
    "Knox",
    "Loudon",
    "McMinn",
    "Meigs",
    "Monroe",
    "Morgan",
    "Polk",
    "Rhea",
    "Roane",
    "Scott",
    "Sevier",
    "Sullivan",
    "Unicoi",
    "Union",
    "Washington",
]);

const TX_MOUNTAIN_COUNTIES = new Set(["El Paso", "Hudspeth", "Culberson"]);

function slugify(input) {
    return String(input || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .trim()
        .replace(/&/g, "and")
        .replace(/[^a-z0-9\s-]/g, "")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-");
}

function toTitleCase(input) {
    return String(input || "")
        .toLowerCase()
        .split(" ")
        .filter(Boolean)
        .map((word) => {
            if (word === "mckinley") return "McKinley";
            if (word.startsWith("mc") && word.length > 2) {
                return `Mc${word[2].toUpperCase()}${word.slice(3)}`;
            }
            return `${word[0]?.toUpperCase() || ""}${word.slice(1)}`;
        })
        .join(" ");
}

function cleanCountyName(input) {
    const c = toTitleCase(input)
        .replace(/\s+County$/i, "")
        .replace(/\s+Parish$/i, "")
        .trim();
    return c;
}

function cleanCityName(input) {
    return toTitleCase(String(input || "").trim()).replace(/\s+/g, " ").trim();
}

function tzForCounty(stateName, countyName) {
    if (stateName === "South Dakota" && SD_MOUNTAIN_COUNTIES.has(countyName)) {
        return { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" };
    }
    if (stateName === "Tennessee" && TN_EASTERN_COUNTIES.has(countyName)) {
        return { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" };
    }
    if (stateName === "Texas" && TX_MOUNTAIN_COUNTIES.has(countyName)) {
        return { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" };
    }
    return DEFAULT_TZ_BY_STATE[stateName];
}

function escapeXml(s) {
    return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&apos;");
}

function buildEmbeddedSitemap(countyDomain, cityDomains, lastmod) {
    const locs = [`${countyDomain}/sitemap.xml`, ...cityDomains.map((d) => `${d}/sitemap.xml`)];
    const lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
        ...locs.flatMap((loc) => [
            "  <sitemap>",
            `    <loc>${escapeXml(loc)}</loc>`,
            `    <lastmod>${lastmod}</lastmod>`,
            "  </sitemap>",
        ]),
        "</sitemapindex>",
    ];
    return lines.join("\\n");
}

async function main() {
    const raw = await fs.readFile(DATASET_PATH, "utf8");
    const lines = raw.split(/\r?\n/).filter(Boolean);
    const rows = lines.slice(1);

    const byState = new Map();
    for (const state of TARGET_STATES) {
        byState.set(state, new Map());
    }

    for (const row of rows) {
        const parts = row.split("|");
        if (parts.length < 4) continue;
        const [cityRaw, , stateFullRaw, countyRaw] = parts;
        const stateFull = String(stateFullRaw || "").trim();
        if (!byState.has(stateFull)) continue;

        const cityName = cleanCityName(cityRaw);
        const countyName = cleanCountyName(countyRaw);
        if (!cityName || !countyName) continue;

        const countyMap = byState.get(stateFull);
        if (!countyMap.has(countyName)) countyMap.set(countyName, new Set());
        countyMap.get(countyName).add(cityName);
    }

    const lastmod = new Date().toISOString().slice(0, 10);

    for (const stateName of TARGET_STATES) {
        const abbr = STATE_ABBR[stateName].toLowerCase();
        const countyMap = byState.get(stateName);
        const counties = [...countyMap.keys()]
            .sort((a, b) => a.localeCompare(b))
            .map((countyName) => {
                const countySlug = slugify(countyName);
                const countyDomain = `https://${countySlug}-county-${abbr}.mydripnurse.com`;

                const cities = [...countyMap.get(countyName)]
                    .sort((a, b) => a.localeCompare(b))
                    .map((cityName) => {
                        const citySlug = slugify(cityName);
                        const cityDomain = `https://${citySlug}-city-${abbr}.mydripnurse.com`;
                        return {
                            cityName,
                            cityDomain,
                            citySitemap: `${cityDomain}/sitemap.xml`,
                        };
                    });

                const cityDomains = cities.map((c) => c.cityDomain);

                return {
                    countyName,
                    cities,
                    Timezone: tzForCounty(stateName, countyName),
                    countyDomain,
                    countySitemap: `${countyDomain}/sitemap.xml`,
                    sitemap: buildEmbeddedSitemap(countyDomain, cityDomains, lastmod),
                };
            });

        const out = { counties };
        const slug = slugify(stateName);
        const outPath = path.join(OUT_DIR, `${slug}.json`);
        await fs.writeFile(outPath, `${JSON.stringify(out, null, 4)}\n`, "utf8");

        console.log(
            `Generated ${path.basename(outPath)} | counties=${counties.length} | cities=${counties.reduce((n, c) => n + c.cities.length, 0)}`
        );
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
