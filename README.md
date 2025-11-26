# Pipeline: customers

Scope: Ingest B2C/B2B customers + health profiles + household preferences from Supabase Gold v3 into Neo4j v3 using a Python worker. This repo is self-contained (no shared code).

Supabase source tables: households, b2c_customers, b2b_customers, b2c_customer_health_profiles, b2c_customer_health_conditions, b2c_customer_allergens, b2c_customer_dietary_preferences, b2b_customer_health_profiles, b2b_customer_health_conditions, b2b_customer_allergens, b2b_customer_dietary_preferences, household_preferences, household_budgets, vendors
Neo4j labels touched: Household, B2CCustomer, B2BCustomer, B2CHealthProfile, B2BHealthProfile, HealthCondition, Allergen, DietaryPreference, Vendor, HouseholdPreference, HouseholdBudget
Neo4j relationships touched: BELONGS_TO_HOUSEHOLD, BELONGS_TO_VENDOR, HAS_PROFILE, HAS_CONDITION, ALLERGIC_TO, FOLLOWS_DIET, HAS_PREFERENCE, HAS_BUDGET

How it works
- Outbox-driven: worker polls `outbox_events` filtered to customer-domain tables/aggregate types (b2c_customer, b2b_customer, household), locks with `SKIP LOCKED`, and routes per aggregate.
- B2C/B2B upsert: loads full aggregate (customer core, household/vendor, health profile, conditions, allergens, dietary prefs, plus household prefs/budgets for B2C) and rebuilds relationships idempotently; missing-row DELETE events `DETACH DELETE` the customer node.
- Household upsert: updates Household node plus preferences/budgets; missing-row DELETE events remove the Household node.

Run
- Install deps: `pip install -r requirements.txt`
- Configure env: copy `.env.example` → `.env` and fill Postgres/Neo4j credentials.
- Start worker: `python -m src.workers.runner`

Folders
- docs/: domain notes, Cypher patterns, event routing
- src/: config, adapters (supabase, neo4j, queue), domain models/services, pipelines (aggregate upserts), workers (runners), utils
- tests/: placeholder for unit/integration tests
- ops/: ops templates (docker/env/sample cron jobs)
