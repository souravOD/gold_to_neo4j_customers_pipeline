from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class CustomerPipeline:
    """Upserts B2C/B2B customers (and households) with health profiles, conditions, allergies, diets."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("customer_pipeline")

    # ===================== DATA LOADERS =====================
    def load_household(self, conn, household_id: str) -> Optional[Dict]:
        sql = """
        SELECT id, household_name, household_type, account_status,
               total_members, location_country, location_region, location_city, location_postal_code,
               created_at, updated_at
        FROM households
        WHERE id = %s;
        """
        return pg.fetch_one(conn, sql, (household_id,))

    def load_household_prefs(self, conn, household_id: str) -> List[Dict]:
        sql = """
        SELECT id, preference_type, preference_value, priority, created_at
        FROM household_preferences
        WHERE household_id = %s;
        """
        return pg.fetch_all(conn, sql, (household_id,))

    def load_household_budgets(self, conn, household_id: str) -> List[Dict]:
        sql = """
        SELECT id, budget_type, amount, currency, period, start_date, end_date, is_active, created_at
        FROM household_budgets
        WHERE household_id = %s;
        """
        return pg.fetch_all(conn, sql, (household_id,))

    def load_b2c_customer(self, conn, customer_id: str) -> Optional[Dict]:
        sql = """
        SELECT c.*, h.household_name, h.household_type, h.account_status AS household_account_status
        FROM b2c_customers c
        JOIN households h ON h.id = c.household_id
        WHERE c.id = %s;
        """
        return pg.fetch_one(conn, sql, (customer_id,))

    def load_b2c_profile(self, conn, customer_id: str) -> Optional[Dict]:
        sql = """
        SELECT *
        FROM b2c_customer_health_profiles
        WHERE b2c_customer_id = %s;
        """
        return pg.fetch_one(conn, sql, (customer_id,))

    def load_b2c_conditions(self, conn, customer_id: str) -> List[Dict]:
        sql = """
        SELECT ch.condition_id AS id, hc.name, ch.severity, ch.diagnosis_date, ch.is_active, ch.notes
        FROM b2c_customer_health_conditions ch
        JOIN health_conditions hc ON hc.id = ch.condition_id
        WHERE ch.b2c_customer_id = %s;
        """
        return pg.fetch_all(conn, sql, (customer_id,))

    def load_b2c_allergens(self, conn, customer_id: str) -> List[Dict]:
        sql = """
        SELECT ca.allergen_id AS id, a.name, ca.severity, ca.diagnosis_date, ca.is_active, ca.reaction_description
        FROM b2c_customer_allergens ca
        JOIN allergens a ON a.id = ca.allergen_id
        WHERE ca.b2c_customer_id = %s;
        """
        return pg.fetch_all(conn, sql, (customer_id,))

    def load_b2c_diets(self, conn, customer_id: str) -> List[Dict]:
        sql = """
        SELECT dp.diet_id AS id, d.name, dp.strictness, dp.start_date, dp.is_active
        FROM b2c_customer_dietary_preferences dp
        JOIN dietary_preferences d ON d.id = dp.diet_id
        WHERE dp.b2c_customer_id = %s;
        """
        return pg.fetch_all(conn, sql, (customer_id,))

    def load_b2b_customer(self, conn, customer_id: str) -> Optional[Dict]:
        sql = """
        SELECT c.*, v.name AS vendor_name, v.vendor_type, v.slug AS vendor_slug
        FROM b2b_customers c
        JOIN vendors v ON v.id = c.vendor_id
        WHERE c.id = %s;
        """
        return pg.fetch_one(conn, sql, (customer_id,))

    def load_b2b_profile(self, conn, customer_id: str) -> Optional[Dict]:
        sql = """
        SELECT *
        FROM b2b_customer_health_profiles
        WHERE b2b_customer_id = %s;
        """
        return pg.fetch_one(conn, sql, (customer_id,))

    def load_b2b_conditions(self, conn, customer_id: str) -> List[Dict]:
        sql = """
        SELECT ch.condition_id AS id, hc.name, ch.severity, ch.diagnosis_date, ch.is_active, ch.notes
        FROM b2b_customer_health_conditions ch
        JOIN health_conditions hc ON hc.id = ch.condition_id
        WHERE ch.b2b_customer_id = %s;
        """
        return pg.fetch_all(conn, sql, (customer_id,))

    def load_b2b_allergens(self, conn, customer_id: str) -> List[Dict]:
        sql = """
        SELECT ca.allergen_id AS id, a.name, ca.severity, ca.diagnosis_date, ca.is_active, ca.reaction_description
        FROM b2b_customer_allergens ca
        JOIN allergens a ON a.id = ca.allergen_id
        WHERE ca.b2b_customer_id = %s;
        """
        return pg.fetch_all(conn, sql, (customer_id,))

    def load_b2b_diets(self, conn, customer_id: str) -> List[Dict]:
        sql = """
        SELECT dp.diet_id AS id, d.name, dp.strictness, dp.start_date, dp.is_active
        FROM b2b_customer_dietary_preferences dp
        JOIN dietary_preferences d ON d.id = dp.diet_id
        WHERE dp.b2b_customer_id = %s;
        """
        return pg.fetch_all(conn, sql, (customer_id,))

    # ===================== CYPHER HELPERS =====================
    def _b2c_cypher(self) -> str:
        return """
        MERGE (h:Household {id: $household.id})
        SET h.household_name = $household.household_name,
            h.household_type = $household.household_type,
            h.account_status = $household.account_status,
            h.total_members = $household.total_members,
            h.location_country = $household.location_country,
            h.location_region = $household.location_region,
            h.location_city = $household.location_city,
            h.location_postal_code = $household.location_postal_code,
            h.updated_at = datetime($household.updated_at),
            h.created_at = datetime($household.created_at)

        MERGE (c:B2CCustomer {id: $customer.id})
        SET c.full_name = $customer.full_name,
            c.first_name = $customer.first_name,
            c.last_name = $customer.last_name,
            c.email = $customer.email,
            c.phone = $customer.phone,
            c.household_role = $customer.household_role,
            c.birth_year = $customer.birth_year,
            c.birth_month = $customer.birth_month,
            c.date_of_birth = $customer.date_of_birth,
            c.age = $customer.age,
            c.gender = $customer.gender,
            c.is_profile_owner = $customer.is_profile_owner,
            c.account_status = $customer.account_status,
            c.updated_at = datetime($customer.updated_at),
            c.created_at = datetime($customer.created_at)

        MERGE (c)-[:BELONGS_TO_HOUSEHOLD]->(h)

        // Profile
        WITH c, h, $health_profile AS hp
        OPTIONAL MATCH (c)-[oldHp:HAS_PROFILE]->(:B2CHealthProfile)
        DELETE oldHp;
        FOREACH (_ IN CASE WHEN hp IS NULL THEN [] ELSE [1] END |
          MERGE (p:B2CHealthProfile {id: hp.id})
          SET p.height_cm = hp.height_cm,
              p.weight_kg = hp.weight_kg,
              p.bmi = hp.bmi,
              p.bmr = hp.bmr,
              p.tdee = hp.tdee,
              p.activity_level = hp.activity_level,
              p.health_goal = hp.health_goal,
              p.target_weight_kg = hp.target_weight_kg,
              p.target_calories = hp.target_calories,
              p.target_protein_g = hp.target_protein_g,
              p.target_carbs_g = hp.target_carbs_g,
              p.target_fat_g = hp.target_fat_g,
              p.target_fiber_g = hp.target_fiber_g,
              p.target_sodium_mg = hp.target_sodium_mg,
              p.target_sugar_g = hp.target_sugar_g,
              p.created_at = datetime(hp.created_at),
              p.updated_at = datetime(hp.updated_at)
          MERGE (c)-[:HAS_PROFILE]->(p)
        )

        // Conditions
        WITH c, h
        OPTIONAL MATCH (c)-[oldCond:HAS_CONDITION]->(:HealthCondition)
        DELETE oldCond;
        WITH c, h, $conditions AS conditions
        UNWIND conditions AS con
          MERGE (hc:HealthCondition {id: con.id})
          SET hc.name = coalesce(con.name, hc.name)
          MERGE (c)-[rel:HAS_CONDITION]->(hc)
          SET rel.severity = con.severity,
              rel.diagnosis_date = con.diagnosis_date,
              rel.is_active = con.is_active,
              rel.notes = con.notes

        // Allergens
        WITH c, h
        OPTIONAL MATCH (c)-[oldAll:ALLERGIC_TO]->(:Allergen)
        DELETE oldAll;
        WITH c, h, $allergens AS allergens
        UNWIND allergens AS al
          MERGE (a:Allergen {id: al.id})
          SET a.name = coalesce(al.name, a.name)
          MERGE (c)-[rel:ALLERGIC_TO]->(a)
          SET rel.severity = al.severity,
              rel.diagnosis_date = al.diagnosis_date,
              rel.is_active = al.is_active,
              rel.reaction_description = al.reaction_description

        // Dietary preferences
        WITH c, h
        OPTIONAL MATCH (c)-[oldDiet:FOLLOWS_DIET]->(:DietaryPreference)
        DELETE oldDiet;
        WITH c, h, $diet_prefs AS diet_prefs
        UNWIND diet_prefs AS d
          MERGE (dp:DietaryPreference {id: d.id})
          SET dp.name = coalesce(d.name, dp.name)
          MERGE (c)-[rel:FOLLOWS_DIET]->(dp)
          SET rel.strictness = d.strictness,
              rel.start_date = d.start_date,
              rel.is_active = d.is_active

        // Household preferences
        WITH h
        OPTIONAL MATCH (h)-[oldPref:HAS_PREFERENCE]->(:HouseholdPreference)
        DELETE oldPref;
        WITH h, $household_prefs AS prefs
        UNWIND prefs AS pref
          MERGE (hp:HouseholdPreference {id: pref.id})
          SET hp.preference_type = pref.preference_type,
              hp.preference_value = pref.preference_value,
              hp.priority = pref.priority,
              hp.created_at = pref.created_at
          MERGE (h)-[:HAS_PREFERENCE]->(hp)

        // Household budgets
        WITH h
        OPTIONAL MATCH (h)-[oldBudget:HAS_BUDGET]->(:HouseholdBudget)
        DELETE oldBudget;
        WITH h, $household_budgets AS budgets
        UNWIND budgets AS b
          MERGE (hb:HouseholdBudget {id: b.id})
          SET hb.budget_type = b.budget_type,
              hb.amount = b.amount,
              hb.currency = b.currency,
              hb.period = b.period,
              hb.start_date = b.start_date,
              hb.end_date = b.end_date,
              hb.is_active = b.is_active,
              hb.created_at = b.created_at
          MERGE (h)-[:HAS_BUDGET]->(hb);
        """

    def _b2b_cypher(self) -> str:
        return """
        MERGE (v:Vendor {id: $vendor.id})
        SET v.name = $vendor.vendor_name,
            v.vendor_type = $vendor.vendor_type,
            v.slug = $vendor.vendor_slug

        MERGE (c:B2BCustomer {id: $customer.id})
        SET c.full_name = $customer.full_name,
            c.email = $customer.email,
            c.phone = $customer.phone,
            c.external_id = $customer.external_id,
            c.account_status = $customer.account_status,
            c.date_of_birth = $customer.date_of_birth,
            c.gender = $customer.gender,
            c.updated_at = datetime($customer.updated_at),
            c.created_at = datetime($customer.created_at)

        MERGE (c)-[:BELONGS_TO_VENDOR]->(v)

        // Profile
        WITH c, v, $health_profile AS hp
        OPTIONAL MATCH (c)-[oldHp:HAS_PROFILE]->(:B2BHealthProfile)
        DELETE oldHp;
        FOREACH (_ IN CASE WHEN hp IS NULL THEN [] ELSE [1] END |
          MERGE (p:B2BHealthProfile {id: hp.id})
          SET p.height_cm = hp.height_cm,
              p.weight_kg = hp.weight_kg,
              p.bmi = hp.bmi,
              p.bmr = hp.bmr,
              p.tdee = hp.tdee,
              p.activity_level = hp.activity_level,
              p.health_goal = hp.health_goal,
              p.target_weight_kg = hp.target_weight_kg,
              p.target_calories = hp.target_calories,
              p.target_protein_g = hp.target_protein_g,
              p.target_carbs_g = hp.target_carbs_g,
              p.target_fat_g = hp.target_fat_g,
              p.target_fiber_g = hp.target_fiber_g,
              p.target_sodium_mg = hp.target_sodium_mg,
              p.target_sugar_g = hp.target_sugar_g,
              p.created_at = datetime(hp.created_at),
              p.updated_at = datetime(hp.updated_at)
          MERGE (c)-[:HAS_PROFILE]->(p)
        )

        // Conditions
        WITH c
        OPTIONAL MATCH (c)-[oldCond:HAS_CONDITION]->(:HealthCondition)
        DELETE oldCond;
        WITH c, $conditions AS conditions
        UNWIND conditions AS con
          MERGE (hc:HealthCondition {id: con.id})
          SET hc.name = coalesce(con.name, hc.name)
          MERGE (c)-[rel:HAS_CONDITION]->(hc)
          SET rel.severity = con.severity,
              rel.diagnosis_date = con.diagnosis_date,
              rel.is_active = con.is_active,
              rel.notes = con.notes

        // Allergens
        WITH c
        OPTIONAL MATCH (c)-[oldAll:ALLERGIC_TO]->(:Allergen)
        DELETE oldAll;
        WITH c, $allergens AS allergens
        UNWIND allergens AS al
          MERGE (a:Allergen {id: al.id})
          SET a.name = coalesce(al.name, a.name)
          MERGE (c)-[rel:ALLERGIC_TO]->(a)
          SET rel.severity = al.severity,
              rel.diagnosis_date = al.diagnosis_date,
              rel.is_active = al.is_active,
              rel.reaction_description = al.reaction_description

        // Dietary preferences
        WITH c
        OPTIONAL MATCH (c)-[oldDiet:FOLLOWS_DIET]->(:DietaryPreference)
        DELETE oldDiet;
        WITH c, $diet_prefs AS diet_prefs
        UNWIND diet_prefs AS d
          MERGE (dp:DietaryPreference {id: d.id})
          SET dp.name = coalesce(d.name, dp.name)
          MERGE (c)-[rel:FOLLOWS_DIET]->(dp)
          SET rel.strictness = d.strictness,
              rel.start_date = d.start_date,
              rel.is_active = d.is_active;
        """

    def _household_cypher(self) -> str:
        return """
        MERGE (h:Household {id: $household.id})
        SET h.household_name = $household.household_name,
            h.household_type = $household.household_type,
            h.account_status = $household.account_status,
            h.total_members = $household.total_members,
            h.location_country = $household.location_country,
            h.location_region = $household.location_region,
            h.location_city = $household.location_city,
            h.location_postal_code = $household.location_postal_code,
            h.updated_at = datetime($household.updated_at),
            h.created_at = datetime($household.created_at)

        // Preferences
        WITH h
        OPTIONAL MATCH (h)-[oldPref:HAS_PREFERENCE]->(:HouseholdPreference)
        DELETE oldPref;
        WITH h, $household_prefs AS prefs
        UNWIND prefs AS pref
          MERGE (hp:HouseholdPreference {id: pref.id})
          SET hp.preference_type = pref.preference_type,
              hp.preference_value = pref.preference_value,
              hp.priority = pref.priority,
              hp.created_at = pref.created_at
          MERGE (h)-[:HAS_PREFERENCE]->(hp)

        // Budgets
        WITH h
        OPTIONAL MATCH (h)-[oldBudget:HAS_BUDGET]->(:HouseholdBudget)
        DELETE oldBudget;
        WITH h, $household_budgets AS budgets
        UNWIND budgets AS b
          MERGE (hb:HouseholdBudget {id: b.id})
          SET hb.budget_type = b.budget_type,
              hb.amount = b.amount,
              hb.currency = b.currency,
              hb.period = b.period,
              hb.start_date = b.start_date,
              hb.end_date = b.end_date,
              hb.is_active = b.is_active,
              hb.created_at = b.created_at
          MERGE (h)-[:HAS_BUDGET]->(hb);
        """

    def _delete_cypher(self, label: str) -> str:
        return f"MATCH (n:{label} {{id: $id}}) DETACH DELETE n;"

    # ===================== OPERATIONS =====================
    def handle_b2c(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            customer = self.load_b2c_customer(conn, event.aggregate_id)

        if customer is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting B2C customer", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher("B2CCustomer"), {"id": event.aggregate_id})
            else:
                self.log.warning("B2C customer missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        household_id = customer["household_id"]
        with self.pg_pool.connection() as conn:
            household = self.load_household(conn, household_id)
            health_profile = self.load_b2c_profile(conn, event.aggregate_id)
            conditions = self.load_b2c_conditions(conn, event.aggregate_id)
            allergens = self.load_b2c_allergens(conn, event.aggregate_id)
            diets = self.load_b2c_diets(conn, event.aggregate_id)
            hh_prefs = self.load_household_prefs(conn, household_id)
            hh_budgets = self.load_household_budgets(conn, household_id)

        params = {
            "customer": customer,
            "household": household,
            "health_profile": health_profile,
            "conditions": conditions,
            "allergens": allergens,
            "diet_prefs": diets,
            "household_prefs": hh_prefs,
            "household_budgets": hh_budgets,
        }
        self.neo4j.write(self._b2c_cypher(), params)
        self.log.info(
            "Upserted B2C customer aggregate",
            extra={
                "id": event.aggregate_id,
                "conditions": len(conditions),
                "allergens": len(allergens),
                "diet_prefs": len(diets),
                "hh_prefs": len(hh_prefs),
                "hh_budgets": len(hh_budgets),
            },
        )

    def handle_b2b(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            customer = self.load_b2b_customer(conn, event.aggregate_id)

        if customer is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting B2B customer", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher("B2BCustomer"), {"id": event.aggregate_id})
            else:
                self.log.warning("B2B customer missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            health_profile = self.load_b2b_profile(conn, event.aggregate_id)
            conditions = self.load_b2b_conditions(conn, event.aggregate_id)
            allergens = self.load_b2b_allergens(conn, event.aggregate_id)
            diets = self.load_b2b_diets(conn, event.aggregate_id)

        params = {
            "customer": customer,
            "vendor": {
                "id": customer["vendor_id"],
                "vendor_name": customer["vendor_name"],
                "vendor_type": customer["vendor_type"],
                "vendor_slug": customer["vendor_slug"],
            },
            "health_profile": health_profile,
            "conditions": conditions,
            "allergens": allergens,
            "diet_prefs": diets,
        }
        self.neo4j.write(self._b2b_cypher(), params)
        self.log.info(
            "Upserted B2B customer aggregate",
            extra={
                "id": event.aggregate_id,
                "conditions": len(conditions),
                "allergens": len(allergens),
                "diet_prefs": len(diets),
            },
        )

    def handle_household(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            household = self.load_household(conn, event.aggregate_id)

        if household is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting household", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher("Household"), {"id": event.aggregate_id})
            else:
                self.log.warning("Household missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            hh_prefs = self.load_household_prefs(conn, event.aggregate_id)
            hh_budgets = self.load_household_budgets(conn, event.aggregate_id)

        params = {
            "household": household,
            "household_prefs": hh_prefs,
            "household_budgets": hh_budgets,
        }
        self.neo4j.write(self._household_cypher(), params)
        self.log.info(
            "Upserted household",
            extra={"id": event.aggregate_id, "prefs": len(hh_prefs), "budgets": len(hh_budgets)},
        )

    def handle_event(self, event: OutboxEvent) -> None:
        agg = event.aggregate_type.lower()
        if agg == "b2c_customer":
            self.handle_b2c(event)
        elif agg == "b2b_customer":
            self.handle_b2b(event)
        elif agg == "household":
            self.handle_household(event)
        else:
            self.log.warning("Unhandled aggregate type", extra={"aggregate_type": agg, "event_id": event.id})
