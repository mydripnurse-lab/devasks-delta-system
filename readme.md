# 📦 MyDripNurse – Automated GHL Subaccount & Sitemap Infrastructure
*(Counties & Cities · Multi‑Industry Ready)*

---

## 📌 Overview (English)

This repository provides a **scalable automation framework** to:

- Generate **SEO sitemaps** for **all U.S. states**, their **counties**, and **cities**
- Automatically create **GoHighLevel (GHL) subaccounts (locations)** from structured JSON
- Integrate with **Twilio** to manage auto‑generated subaccounts
- Prepare structured data to be synced into **Google Sheets**
- Serve as a **reusable blueprint for any industry**, not just healthcare

Although branded as *MyDripNurse*, this project is **industry‑agnostic**.  
You can reuse it for **Solar, IV Therapy, Construction, Legal, Real Estate, or any local service** by changing environment variables and business data.

---

## 📌 Visión General (Español)

Este repositorio provee una **infraestructura de automatización escalable** para:

- Generar **sitemaps SEO** de **todos los estados**, sus **counties** y **ciudades**
- Crear automáticamente **subcuentas (locations) en GoHighLevel**
- Integrarse con **Twilio** para manejar subcuentas generadas
- Preparar la data para **Google Sheets**
- Servir como **plantilla reutilizable para cualquier industria**

Aunque el proyecto se llama *MyDripNurse*, es **agnóstico a la industria**.  
Puede reutilizarse para **Solar, IV Therapy, Construcción, Legal, Bienes Raíces**, etc.

---

## 🗂️ Project Structure / Estructura del Proyecto

```
mydripnurse-sitemaps/
├── resources/
│   ├── statesFiles/           # Raw state JSON (counties + cities)
│   ├── customValues/          # GHL custom values templates
│
├── scripts/
│   ├── src/
│   │   ├── build-counties.js
│   │   ├── run-create-subaccounts.js
│   │   └── services/
│   │       ├── ghlClient.js
│   │       ├── twilioClient.js
│   │       ├── tokenStore.js
│   │
│   └── out/
│       ├── <state>/           # Generated sitemap + payload outputs
│       ├── checkpoints/       # Anti-duplication checkpoints
│
├── server.js                  # Local server for OAuth callback
├── .env
├── package.json
└── README.md
```

---

## 🌐 Sitemaps Scope (Important)

### English

This project already includes or generates:

- **State-level sitemaps**
- **County-level sitemaps**
- **City-level sitemaps per county**

Each sitemap can be:
- Indexed in Google Search Console
- Used for internal linking
- Reused across industries by domain replacement

### Español

Este proyecto incluye/genera:

- Sitemaps por **estado**
- Sitemaps por **county**
- Sitemaps por **ciudad dentro de cada county**

---

## 🔐 Environment Variables (.env)

```env
# GHL OAuth (Marketplace App)
CLIENT_ID=your_marketplace_client_id
CLIENT_SECRET=your_marketplace_client_secret
REDIRECT_URI=http://localhost:3000/callback

# Twilio
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxx

# Business / Domain
BASE_DOMAIN=mydripnurse.com
COMPANY_NAME=My Drip Nurse
```

---

## 🖥️ Running the Local Server (OAuth)

### English

The local server is required **only for OAuth authorization**.

1. Install dependencies:
```bash
npm install
```

2. Run the server:
```bash
npm run dev
```

3. Open browser and authorize the app:
```
http://localhost:3000/connect/ghl
```

4. GHL will redirect back with the authorization code.
5. Tokens are stored automatically.

### Español

El servidor local se usa **solo para OAuth**.

1. Instalar dependencias
2. Correr:
```bash
node server.js
```
3. Abrir:
```
http://localhost:3000/auth
```

---

## 🏗️ Step 1 – Build Counties / Construir Counties

```bash
node scripts/src/build-counties.js
```

Creates:
- Sitemap files
- GHL payload JSON
- Google Sheets row previews

⚠️ No API calls at this stage.

---

## 🚀 Step 2 – Create GHL Subaccounts

```bash
node scripts/run-create-subaccounts.js scripts/out/<state>/ghl-create-counties-XXXX.json
```

Dry run:
```bash
node scripts/run-create-subaccounts.js scripts/out/<state>/ghl-create-counties-XXXX.json --dry-run
```

---

## 🔁 Execution Flow (Per County)

1. Create GHL Location
2. Save checkpoint immediately
3. Twilio lookup + optional close
4. (Next) Custom values update
5. (Next) Google Sheets sync

---

## 🧠 Anti‑Duplication System

Checkpoints stored in:
```
scripts/out/checkpoints/<state>.json
```

Safe to re-run.

---

## ⏱️ Performance Metrics

- Time per county
- Total execution time

---

## 🔄 Reusing This Project for Any Industry

### English

To reuse this project:
1. Update `.env` business variables
2. Replace domain references
3. Replace business name and branding
4. Update sitemap base URLs
5. Adjust custom values JSON

Recommended:
- Use **VS Code Find & Replace**
- Keep structure unchanged

### Español

Este proyecto puede reutilizarse para cualquier industria:
- Solo cambia variables de entorno
- Reemplaza dominio e información del negocio
- Mantén la estructura intacta

---

## 🧱 Current Status

### ✅ Completed
- Sitemap generation (states, counties, cities)
- GHL subaccount creation
- Twilio integration
- Checkpoint system
- Timing logs

### 🔜 Next
- Google Sheets API
- City‑level subaccounts
- Parallel execution
- Reporting dashboards

---

## 🧠 Design Principles

- Deterministic builds
- Idempotent execution
- State isolation
- Auditability
- No blind retries

---

## 👤 Maintainer

Built by **My Drip Nurse**  
Automation · SEO · GHL · Scaling
