import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, "../../..");

const DATASET_PATH = path.join(REPO_ROOT, "tmp", "data", "us_cities_states_counties.csv");
const OUT_DIR = path.join(REPO_ROOT, "resources", "statesFiles");

const DEFAULT_ROOT_DOMAIN = "mydripnurse.com";
const ROOT_DOMAIN = String(
    process.env.STATE_FILES_ROOT_DOMAIN || process.env.TENANT_ROOT_DOMAIN || DEFAULT_ROOT_DOMAIN
)
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/\/+$/g, "");

const TARGET_STATES = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
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
    "Puerto Rico",
];

const STATE_ABBR = {
    Alabama: "AL",
    Alaska: "AK",
    Arizona: "AZ",
    Arkansas: "AR",
    California: "CA",
    Colorado: "CO",
    Connecticut: "CT",
    Delaware: "DE",
    Florida: "FL",
    Georgia: "GA",
    Hawaii: "HI",
    Idaho: "ID",
    Illinois: "IL",
    Indiana: "IN",
    Iowa: "IA",
    Kansas: "KS",
    Kentucky: "KY",
    Louisiana: "LA",
    Maine: "ME",
    Maryland: "MD",
    Massachusetts: "MA",
    Michigan: "MI",
    Minnesota: "MN",
    Mississippi: "MS",
    Missouri: "MO",
    Montana: "MT",
    Nebraska: "NE",
    Nevada: "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    Ohio: "OH",
    Oklahoma: "OK",
    Oregon: "OR",
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
    "Puerto Rico": "PR",
};

const DEFAULT_TZ_BY_STATE = {
    Alabama: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Alaska: { Name: "Alaska", Zone: "US/Alaska", UTC_Offset: "-09:00" },
    Arizona: { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" },
    Arkansas: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    California: { Name: "Pacific", Zone: "US/Pacific", UTC_Offset: "-08:00" },
    Colorado: { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" },
    Connecticut: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Delaware: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Florida: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Georgia: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Hawaii: { Name: "Hawaii", Zone: "US/Hawaii", UTC_Offset: "-10:00" },
    Idaho: { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" },
    Illinois: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Indiana: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Iowa: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Kansas: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Kentucky: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Louisiana: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Maine: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Maryland: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Massachusetts: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Michigan: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Minnesota: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Mississippi: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Missouri: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Montana: { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" },
    Nebraska: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Nevada: { Name: "Pacific", Zone: "US/Pacific", UTC_Offset: "-08:00" },
    "New Hampshire": { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    "New Jersey": { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    "New Mexico": { Name: "Mountain", Zone: "US/Mountain", UTC_Offset: "-07:00" },
    "New York": { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    "North Carolina": { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    "North Dakota": { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Ohio: { Name: "Eastern", Zone: "US/Eastern", UTC_Offset: "-05:00" },
    Oklahoma: { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" },
    Oregon: { Name: "Pacific", Zone: "US/Pacific", UTC_Offset: "-08:00" },
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
    "Puerto Rico": { Name: "Atlantic", Zone: "America/Puerto_Rico", UTC_Offset: "-04:00" },
};

const FL_CENTRAL_COUNTIES = new Set([
    "Bay",
    "Calhoun",
    "Escambia",
    "Franklin",
    "Gadsden",
    "Gulf",
    "Holmes",
    "Jackson",
    "Jefferson",
    "Leon",
    "Liberty",
    "Okaloosa",
    "Santa Rosa",
    "Walton",
    "Washington",
]);

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
    return toTitleCase(input)
        .replace(/\s+County$/i, "")
        .replace(/\s+Parish$/i, "")
        .trim();
}

function cleanCityName(input) {
    return toTitleCase(String(input || "").trim()).replace(/\s+/g, " ").trim();
}

function tzForCounty(stateName, countyName) {
    if (stateName === "Florida" && FL_CENTRAL_COUNTIES.has(countyName)) {
        return { Name: "Central", Zone: "US/Central", UTC_Offset: "-06:00" };
    }
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

function buildTenantDomain(label) {
    return `https://${label}.${ROOT_DOMAIN}`;
}

function buildEmbeddedSitemap(mainSitemap, childSitemaps, lastmod) {
    const locs = [mainSitemap, ...childSitemaps];
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

function initStateBuckets() {
    const byState = new Map();
    for (const state of TARGET_STATES) byState.set(state, new Map());
    return byState;
}

function buildPuertoRicoCounties(citySet, lastmod) {
    const stateName = "Puerto Rico";
    const countyName = "Puerto Rico";
    const countyDomain = buildTenantDomain("puerto-rico");
    const countySitemap = `${countyDomain}/sitemap.xml`;

    const cities = [...citySet]
        .sort((a, b) => a.localeCompare(b))
        .map((cityName) => {
            const citySlug = slugify(cityName);
            const cityDomain = buildTenantDomain(citySlug);
            return {
                cityName,
                cityDomain,
                citySitemap: `${cityDomain}/sitemap.xml`,
            };
        });

    return [
        {
            countyName,
            cities,
            Timezone: DEFAULT_TZ_BY_STATE[stateName],
            countyDomain,
            countySitemap,
            embeddedSitemap: buildEmbeddedSitemap(
                countySitemap,
                cities.map((c) => c.citySitemap),
                lastmod
            ),
        },
    ];
}

async function main() {
    if (!ROOT_DOMAIN) {
        throw new Error("ROOT_DOMAIN is empty. Set STATE_FILES_ROOT_DOMAIN or TENANT_ROOT_DOMAIN.");
    }

    console.log(`Using root domain: ${ROOT_DOMAIN}`);
    console.log(`Using dataset: ${DATASET_PATH}`);
    console.log(`Writing to: ${OUT_DIR}`);

    const raw = await fs.readFile(DATASET_PATH, "utf8");
    const lines = raw.split(/\r?\n/).filter(Boolean);
    const rows = lines.slice(1);

    const byState = initStateBuckets();

    for (const row of rows) {
        const parts = row.split("|");
        if (parts.length < 4) continue;

        const [cityRaw, , stateFullRaw, countyRaw] = parts;
        const stateFull = String(stateFullRaw || "").trim();
        if (!byState.has(stateFull)) continue;

        const cityName = cleanCityName(cityRaw);
        if (!cityName) continue;

        if (stateFull === "Puerto Rico") {
            const stateMap = byState.get(stateFull);
            const bucket = "Puerto Rico";
            if (!stateMap.has(bucket)) stateMap.set(bucket, new Set());
            stateMap.get(bucket).add(cityName);
            continue;
        }

        const countyName = cleanCountyName(countyRaw);
        if (!countyName) continue;

        const countyMap = byState.get(stateFull);
        if (!countyMap.has(countyName)) countyMap.set(countyName, new Set());
        countyMap.get(countyName).add(cityName);
    }

    const lastmod = new Date().toISOString().slice(0, 10);
    await fs.mkdir(OUT_DIR, { recursive: true });

    for (const stateName of TARGET_STATES) {
        let counties = [];

        if (stateName === "Puerto Rico") {
            const stateMap = byState.get(stateName);
            const citySet = stateMap.get("Puerto Rico") || new Set();
            counties = buildPuertoRicoCounties(citySet, lastmod);
        } else {
            const abbr = STATE_ABBR[stateName].toLowerCase();
            const countyMap = byState.get(stateName);

            counties = [...countyMap.keys()]
                .sort((a, b) => a.localeCompare(b))
                .map((countyName) => {
                    const countySlug = slugify(countyName);
                    const countyDomain = buildTenantDomain(`${countySlug}-county-${abbr}`);
                    const countySitemap = `${countyDomain}/sitemap.xml`;

                    const cities = [...countyMap.get(countyName)]
                        .sort((a, b) => a.localeCompare(b))
                        .map((cityName) => {
                            const citySlug = slugify(cityName);
                            const cityDomain = buildTenantDomain(`${citySlug}-city-${abbr}`);
                            return {
                                cityName,
                                cityDomain,
                                citySitemap: `${cityDomain}/sitemap.xml`,
                            };
                        });

                    return {
                        countyName,
                        cities,
                        Timezone: tzForCounty(stateName, countyName),
                        countyDomain,
                        countySitemap,
                        embeddedSitemap: buildEmbeddedSitemap(
                            countySitemap,
                            cities.map((c) => c.citySitemap),
                            lastmod
                        ),
                    };
                });
        }

        const out = {
            counties,
            stateName,
        };

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
