# 📦 MyDripNurse – Automated GHL Subaccount Builder  
*(Counties & Cities Infrastructure)*

---

## 📌 Overview (English)

This project automates the creation and management of **GoHighLevel (GHL) subaccounts** (locations) for **Counties and Cities**, using structured JSON files as the source of truth.

The system:
- Builds deterministic JSON payloads per **State → County → City**
- Creates GHL subaccounts **one by one (safe & traceable)**
- Integrates with **Twilio** to close auto-generated subaccounts
- Uses **OAuth (Marketplace App)** for secure GHL API access
- Prevents duplicate subaccount creation via **local checkpoints**
- Measures **execution time per county and per run**
- Prepares data to be sent to **Google Sheets** (next phase)

This repository is designed for **large-scale, state-by-state rollout** without duplication or data corruption.

---

## 📌 Visión General (Español)

Este proyecto automatiza la creación y administración de **subcuentas (locations) en GoHighLevel (GHL)** para **Counties y Cities**, usando archivos JSON estructurados como fuente única de verdad.

El sistema:
- Construye JSON determinísticos por **Estado → County → City**
- Crea subcuentas en GHL **una por una (seguro y trazable)**
- Integra **Twilio** para cerrar subcuentas generadas automáticamente
- Usa **OAuth (Marketplace App)** para acceso seguro a la API de GHL
- Evita duplicados usando **checkpoints locales**
- Mide **tiempo por county y tiempo total del proceso**
- Prepara la data para enviarse a **Google Sheets** (fase siguiente)

Este repositorio está diseñado para **escalar estado por estado**, sin duplicaciones ni errores.

---

## 🗂️ Project Structure / Estructura del Proyecto

```
mydripnurse-sitemaps/
├── resources/
│   ├── statesFiles/           # Raw state JSON (counties + cities)
│   ├── customValues/          # Custom Values templates for GHL
│
├── scripts/
│   ├── src/
│   │   ├── build-counties.js  # Builds GHL payloads per county
│   │   ├── run-create-subaccounts.js # Executes GHL + Twilio flow
│   │   └── services/
│   │       ├── ghlClient.js
│   │       ├── twilioClient.js
│   │       ├── tokenStore.js
│   │
│   └── out/
│       ├── <state>/           # Build outputs per state
│       ├── checkpoints/       # Anti-duplication checkpoints
│
├── .env
├── package.json
└── README.md
```

---

## 🔐 Environment Variables (.env)

### Required (English)

```env
# GHL OAuth (Marketplace App)
CLIENT_ID=your_marketplace_client_id
CLIENT_SECRET=your_marketplace_client_secret
REDIRECT_URI=http://localhost:3000/callback

# Twilio (Master account)
TWILIO_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### Requeridas (Español)

```env
CLIENT_ID y CLIENT_SECRET deben venir del Marketplace App de GHL.
REDIRECT_URI debe coincidir EXACTAMENTE con el redirect configurado en tu app.
TWILIO_* corresponde a la cuenta master de Twilio.
```

---

## 🔑 Authentication Flow (OAuth – GHL)

### English

1. A **Marketplace App** is created in GHL (Target User: **Sub-Account**, “Agency Only” install).
2. An **authorization code** is generated via browser (interactive login/consent).
3. The code is exchanged for **access + refresh tokens**.
4. Tokens are stored locally using `tokenStore.js`.
5. All API calls are performed with `ghlFetch()`.

> Important: `POST /oauth/token` does **not** generate the `code`.  
> The `code` is only obtained through the browser-based authorization flow.

### Español

1. Se crea una **Marketplace App** en GHL (Target User: **Sub-Account**, instalación “Agency Only”).
2. Se genera un **authorization code** vía navegador (login/consentimiento).
3. Ese `code` se intercambia por **access + refresh tokens**.
4. Los tokens se guardan localmente con `tokenStore.js`.
5. Todas las llamadas se realizan usando `ghlFetch()`.

> Importante: `POST /oauth/token` **no** crea el `code`.  
> El `code` solo se obtiene mediante el flujo de autorización en el navegador.

---

## 🏗️ Step 1 – Build County Payloads / Construir Payloads de Counties

### English

Generate the JSON files that will later be used to create subaccounts (no API calls at this stage).

```bash
node scripts/src/build-counties.js
```

Output (examples):
- `scripts/out/<state>/preview-counties-XXXX.json`
- `scripts/out/<state>/ghl-create-counties-XXXX.json`
- `scripts/out/<state>/sheets-rows-counties-XXXX.json`

Each item includes:
- `countyName`
- `countyDomain`
- `countySitemap`
- `Timezone.Zone`
- `body` (payload for `POST /locations`)
- `customValuesBody` (base + dynamic custom values)

### Español

Este paso **NO llama APIs**. Solo construye la data necesaria para ejecutar el proceso real.

---

## 🚀 Step 2 – Create Subaccounts / Crear Subcuentas (Main Run)

### English

Run the creator script against the build output.

```bash
node scripts/run-create-subaccounts.js scripts/out/<state>/ghl-create-counties-XXXX.json
```

Dry run (no Twilio close):
```bash
node scripts/run-create-subaccounts.js scripts/out/<state>/ghl-create-counties-XXXX.json --dry-run
```

Resume behavior (default ON):
- The script writes checkpoints and skips already-created counties automatically.

Disable resume (not recommended):
```bash
node scripts/run-create-subaccounts.js scripts/out/<state>/ghl-create-counties-XXXX.json --no-resume
```

### Español

Ejecuta el script principal con el JSON que generó el build.

---

## 🔁 Execution Flow (Per County) / Flujo por County

### English

For each county:

1. **Create GHL Location**  
   `POST /locations` using the county payload (`it.body`)

2. **Write checkpoint immediately**  
   Prevents duplicates in re-runs even if later steps fail.

3. **Twilio step**  
   Find Twilio subaccount by `friendlyName == created.name`  
   Optionally close that subaccount (LIVE mode)

4. **(Next) Custom values** *(planned / next phase)*  
   - Generate Location Token  
   - Get custom values  
   - Update custom values one-by-one

5. **(Next) Google Sheets** *(planned / next phase)*  
   Append run results (locationId, domain activation URL, etc.)

### Español

Por cada county:

1. Crear location en GHL  
2. Guardar checkpoint local de inmediato  
3. Buscar y cerrar subcuenta en Twilio (opcional)  
4. Próximo: setear custom values  
5. Próximo: guardar en Google Sheets

---

## 🧠 Anti-Duplication System (Checkpoints) / Sistema Anti-Duplicados

### English

Checkpoints are stored in:

```
scripts/out/checkpoints/<state>.json
```

A county is considered already processed if it has a stored `locationId`.  
This makes the process **safe to re-run** without duplicating locations.

### Español

Los checkpoints evitan que vuelvas a crear la misma subcuenta si tu proceso se interrumpe o si lo vuelves a correr.

---

## ⏱️ Performance Metrics / Métricas de Tiempo

### English

The run script logs:

- Time per county
- Total execution time

Example:

```
⏱️ 3/12 | Anchorage Municipality completed in 3.42s
⏱️ TOTAL TIME: 51s (0.85 min)
```

### Español

Se imprime el tiempo por cada county y el total del run.

---

## ☎️ Twilio Integration / Integración Twilio

### English

Twilio is used to locate subaccounts by `friendlyName`.  
Common pitfalls:
- Don’t duplicate the API version in Twilio URLs (use SDK if possible)
- Ensure Twilio SDK is installed: `npm i twilio`

Environment variables:
- `TWILIO_SID`
- `TWILIO_AUTH_TOKEN`

### Español

Twilio se usa para encontrar subcuentas por `friendlyName` y opcionalmente cerrarlas.

---

## 📊 Google Sheets (Next Phase) / Google Sheets (Siguiente Fase)

### Planned (English)

We will append rows for each county with fields like:

- Account Name
- Location ID
- Company ID
- County
- State
- Domain
- Sitemap
- Timezone
- Status
- Domain URL Activation

A separate sheet will be used for Cities.

### Planificado (Español)

Luego se añadirá integración a Google Sheets. Habrá un sheet para counties y otro para cities.

---

## 🧱 Current Status / Estado Actual

### ✅ Completed / Completado

- County JSON build system
- Run script with sequential GHL creation
- Twilio integration (lookup + optional close)
- Anti-duplication via checkpoints
- Timing logs (per county + total)

### 🔜 Next / Próximo

- Full GHL OAuth operational flow:
  - capture authorization code
  - exchange code for tokens
  - generate location token
- Custom values update pipeline
- Google Sheets API integration
- City-level workflows

---

## 🧠 Design Principles / Principios

- **Deterministic builds** (same input → same payload)
- **Idempotent execution** (safe to re-run with checkpoints)
- **State-by-state isolation**
- **Auditability first** (logs + artifacts)
- **No blind retries** for side-effect requests

---

## 👤 Maintainer

Built for **My Drip Nurse**  
Infrastructure & Automation System  
GoHighLevel · Twilio · SEO · Scaling
