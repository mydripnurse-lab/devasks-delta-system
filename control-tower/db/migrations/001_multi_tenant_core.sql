-- Delta Control Tower
-- Migration: 001_multi_tenant_core
-- Purpose: First multi-tenant core schema for staging/prod rollout

BEGIN;

CREATE SCHEMA IF NOT EXISTS app;
SET search_path TO app, public;

-- Needed for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Keep updated_at in sync on row updates
CREATE OR REPLACE FUNCTION app.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Global user identity (can belong to multiple organizations/tenants)
CREATE TABLE IF NOT EXISTS app.users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL,
  full_name TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Case-insensitive uniqueness for email
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_uq
ON app.users (LOWER(email));

CREATE TRIGGER trg_users_set_updated_at
BEFORE UPDATE ON app.users
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

-- Tenant/business root entity
CREATE TABLE IF NOT EXISTS app.organizations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  slug TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT organizations_status_ck CHECK (status IN ('active', 'disabled'))
);

CREATE UNIQUE INDEX IF NOT EXISTS organizations_slug_uq
ON app.organizations (LOWER(slug));

CREATE TRIGGER trg_organizations_set_updated_at
BEFORE UPDATE ON app.organizations
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

-- Membership + role per organization
CREATE TABLE IF NOT EXISTS app.organization_memberships (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES app.organizations(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT organization_memberships_role_ck CHECK (role IN ('owner', 'admin', 'analyst', 'viewer')),
  CONSTRAINT organization_memberships_status_ck CHECK (status IN ('active', 'invited', 'disabled')),
  CONSTRAINT organization_memberships_unique UNIQUE (organization_id, user_id)
);

CREATE INDEX IF NOT EXISTS organization_memberships_org_idx
ON app.organization_memberships (organization_id);

CREATE INDEX IF NOT EXISTS organization_memberships_user_idx
ON app.organization_memberships (user_id);

CREATE TRIGGER trg_organization_memberships_set_updated_at
BEFORE UPDATE ON app.organization_memberships
FOR EACH ROW
EXECUTE FUNCTION app.set_updated_at();

COMMIT;

