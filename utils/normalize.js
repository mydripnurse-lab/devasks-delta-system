export function normalizeName(s) {
    return String(s || "")
        .trim()
        .replace(/\s+/g, " "); // colapsa espacios dobles
}
