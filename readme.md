# Delta System - My Drip Nurse

Repositorio principal de automatizacion + dashboards para Delta System.

## Que incluye este repo

- Generacion y manejo de data por `State / County / City`
- Integracion con GoHighLevel (GHL)
- Integracion con Google Sheets
- Control Tower (UI principal) en Next.js
- Dashboards operativos y ejecutivos (Calls, Leads, Conversations, Transactions, Appointments, Search, GA, Ads)

## Carpetas clave

- `control-tower/` -> App principal (Next.js)
- `resources/` -> Configs, estados, archivos base
- `scripts/` -> Automatizaciones y builders
- `services/` -> Clientes y utilidades de integracion (Sheets, etc.)

## Arranque rapido

```bash
cd control-tower
npm install
cp .env.example .env.local
npm run dev
```

Abrir: [http://localhost:3001](http://localhost:3001)

## Documentacion completa de setup

La guia profesional paso a paso (API keys, Google Cloud, GSC, GHL, troubleshooting) esta aqui:

- [`control-tower/README.md`](control-tower/README.md)

## Nota importante

Si ves errores `403` en Sheets o Search Console, normalmente es tema de:

1. API no habilitada en Google Cloud
2. Service account sin permisos en el recurso (Sheet/propiedad GSC)
3. Keyfile equivocado en `.env.local`

La solucion exacta esta documentada en `control-tower/README.md`.

