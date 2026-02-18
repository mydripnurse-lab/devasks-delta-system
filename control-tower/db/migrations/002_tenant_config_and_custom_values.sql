-- Delta Control Tower
-- Migration: 002_tenant_config_and_custom_values
-- Purpose: Tenant-level configuration, integrations, prompts, snapshots, and GHL custom values

BEGIN;

CREATE SCHEMA IF NOT EXISTS app;
SET search_path TO app, public;

-- 1) Per-tenant general settings
CREATE TABLE IF NOT EXISTS app.organization_settings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES app.organizations(id) ON DELETE CASCADE,
  timezone TEXT NOT NULL DEFAULT 'UTC',
  locale TEXT NOT NULL DEFAULT 'en-US',
  currency TEXT NOT NULL DEFAULT 'USD',
  root_domain TEXT,
  app_display_name TEXT,
  brand_name TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT organization_settings_unique_org UNIQUE (organization_id)
);

CREATE TRIGGER trg_organization_settings_set_updated_at
BEFORE UPDATE ON app.organization_settings
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

-- 2) Per-tenant business profile (used by AI + campaign generation)
CREATE TABLE IF NOT EXISTS app.organization_business_profiles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES app.organizations(id) ON DELETE CASCADE,
  legal_name TEXT,
  display_name TEXT,
  website_url TEXT,
  industry TEXT,
  target_market TEXT,
  value_proposition TEXT,
  tone_of_voice TEXT,
  offer_summary TEXT,
  contact_email TEXT,
  contact_phone TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT organization_business_profiles_unique_org UNIQUE (organization_id)
);

CREATE TRIGGER trg_organization_business_profiles_set_updated_at
BEFORE UPDATE ON app.organization_business_profiles
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

-- 3) OAuth/API integrations per tenant (GHL, Google, Bing, etc.)
CREATE TABLE IF NOT EXISTS app.organization_integrations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES app.organizations(id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  integration_key TEXT NOT NULL DEFAULT 'default',
  status TEXT NOT NULL DEFAULT 'disconnected',
  auth_type TEXT NOT NULL DEFAULT 'api_key',
  access_token_enc TEXT,
  refresh_token_enc TEXT,
  token_expires_at TIMESTAMPTZ,
  scopes TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  external_account_id TEXT,
  external_property_id TEXT,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_sync_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT organization_integrations_status_ck CHECK (status IN ('connected', 'disconnected', 'needs_reconnect', 'error')),
  CONSTRAINT organization_integrations_auth_type_ck CHECK (auth_type IN ('oauth', 'api_key', 'service_account', 'none')),
  CONSTRAINT organization_integrations_provider_ck CHECK (
    provider IN ('ghl', 'google_search_console', 'google_analytics', 'google_ads', 'bing_webmaster', 'openai', 'facebook_ads', 'tiktok_ads', 'custom')
  ),
  CONSTRAINT organization_integrations_unique UNIQUE (organization_id, provider, integration_key)
);

CREATE INDEX IF NOT EXISTS organization_integrations_org_idx
ON app.organization_integrations (organization_id);

CREATE INDEX IF NOT EXISTS organization_integrations_provider_idx
ON app.organization_integrations (provider);

CREATE TRIGGER trg_organization_integrations_set_updated_at
BEFORE UPDATE ON app.organization_integrations
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

-- 4) Prompt registry per tenant/module/agent
CREATE TABLE IF NOT EXISTS app.organization_prompts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES app.organizations(id) ON DELETE CASCADE,
  module TEXT NOT NULL,
  agent TEXT NOT NULL,
  prompt_key TEXT NOT NULL,
  prompt_text TEXT NOT NULL,
  version INT NOT NULL DEFAULT 1,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by_user_id UUID REFERENCES app.users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT organization_prompts_unique UNIQUE (organization_id, module, agent, prompt_key, version)
);

CREATE INDEX IF NOT EXISTS organization_prompts_active_idx
ON app.organization_prompts (organization_id, module, agent, is_active);

CREATE TRIGGER trg_organization_prompts_set_updated_at
BEFORE UPDATE ON app.organization_prompts
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

-- 5) Snapshot registry per tenant/module (cache lineage + IDs)
CREATE TABLE IF NOT EXISTS app.organization_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES app.organizations(id) ON DELETE CASCADE,
  module TEXT NOT NULL,
  snapshot_key TEXT NOT NULL,
  snapshot_id TEXT,
  source TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT organization_snapshots_unique UNIQUE (organization_id, module, snapshot_key)
);

CREATE INDEX IF NOT EXISTS organization_snapshots_org_module_idx
ON app.organization_snapshots (organization_id, module, captured_at DESC);

CREATE TRIGGER trg_organization_snapshots_set_updated_at
BEFORE UPDATE ON app.organization_snapshots
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

-- 6) GHL custom values per tenant
-- Store exact key name + value mapping so each tenant can have a different setup.
CREATE TABLE IF NOT EXISTS app.organization_custom_values (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES app.organizations(id) ON DELETE CASCADE,
  provider TEXT NOT NULL DEFAULT 'ghl',
  scope TEXT NOT NULL DEFAULT 'global',
  module TEXT NOT NULL DEFAULT 'control_tower',
  key_name TEXT NOT NULL,
  key_value TEXT,
  value_type TEXT NOT NULL DEFAULT 'text',
  is_secret BOOLEAN NOT NULL DEFAULT FALSE,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  description TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_synced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT organization_custom_values_scope_ck CHECK (scope IN ('global', 'state', 'county', 'city', 'module')),
  CONSTRAINT organization_custom_values_type_ck CHECK (value_type IN ('text', 'number', 'boolean', 'json', 'secret')),
  CONSTRAINT organization_custom_values_provider_ck CHECK (provider IN ('ghl', 'google', 'bing', 'openai', 'custom')),
  CONSTRAINT organization_custom_values_unique UNIQUE (organization_id, provider, scope, module, key_name)
);

CREATE INDEX IF NOT EXISTS organization_custom_values_org_idx
ON app.organization_custom_values (organization_id);

CREATE INDEX IF NOT EXISTS organization_custom_values_lookup_idx
ON app.organization_custom_values (organization_id, provider, scope, module, is_active);

CREATE TRIGGER trg_organization_custom_values_set_updated_at
BEFORE UPDATE ON app.organization_custom_values
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

COMMIT;

