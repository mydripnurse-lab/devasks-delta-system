# Database Migrations (PostgreSQL)

This folder contains SQL migrations for the multi-tenant rollout.

## Apply in pgAdmin (recommended for now)

1. Open pgAdmin and connect to your `delta_staging` database.
2. Open **Query Tool**.
3. Load and run:
   - `control-tower/db/migrations/001_multi_tenant_core.sql`
   - `control-tower/db/migrations/002_tenant_config_and_custom_values.sql`
4. Verify tables were created under schema `app`:
   - `app.users`
   - `app.organizations`
   - `app.organization_memberships`
   - `app.organization_settings`
   - `app.organization_business_profiles`
   - `app.organization_integrations`
   - `app.organization_prompts`
   - `app.organization_snapshots`
   - `app.organization_custom_values`

## Quick verification query

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'app'
ORDER BY table_name;
```
