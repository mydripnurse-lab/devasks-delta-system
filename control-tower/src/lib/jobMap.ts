// src/lib/jobMap.ts
export const JOB_MAP: Record<
    string,
    { script: string; interactive?: boolean }
> = {
    "run-delta-system": {
        script: "../scripts/run-delta-system.js",
    },
    "update-custom-values": {
        script: "../scripts/src/builds/update-custom-values.js",
    },
    "build-sheet-rows": {
        script: "../scripts/src/builds/build-sheets-counties-cities.js",
        interactive: true, // ✅ este es el que te pide state
    },
    "build-state-index": {
        script: "../scripts/src/builds/build-states-index.js",
    },
    "build-state-sitemaps": {
        script: "../scripts/src/builds/build-state-sitemaps.js",
    },
    "build-counties": {
        script: "../scripts/src/builds/build-counties.js",
    },
};
