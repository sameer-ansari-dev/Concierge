import json
from datetime import datetime
from typing import Any, Iterable

from db import get_db_connection


INTEREST_CATALOG = [
    ("hiking", "Hiking"),
    ("fine_dining", "Fine Dining"),
    ("spa", "Spa & Wellness"),
    ("shopping", "Shopping"),
    ("cinema", "Cinema"),
    ("sports", "Sports"),
    ("tech", "Technology"),
    ("fitness", "Fitness"),
    ("music", "Music"),
    ("art", "Art & Culture"),
]

SERVICE_CATALOG = [
    ("hotel", "Hotels"),
    ("flight", "Flights"),
    ("cab", "Cabs"),
    ("technician", "Technicians"),
    ("courier", "Courier"),
]


def _normalize_slug_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        parts = [p.strip().lower() for p in values.split(",")]
        return [p for p in parts if p]
    if isinstance(values, (list, tuple, set)):
        out: list[str] = []
        for v in values:
            s = str(v).strip().lower()
            if s:
                out.append(s)
        return out
    return []


def ensure_preference_schema() -> None:
    """Best-effort schema creation for normalized preference tables.

    This project doesn't use migrations. We create missing tables/columns at runtime
    (idempotent). If the DB user lacks DDL permissions, the app will continue running
    with legacy string behavior.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Catalog tables
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS lifestyle_interest_types (
              id SERIAL PRIMARY KEY,
              slug TEXT NOT NULL UNIQUE,
              label TEXT NOT NULL,
              is_active BOOLEAN NOT NULL DEFAULT TRUE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS lifestyle_service_types (
              id SERIAL PRIMARY KEY,
              slug TEXT NOT NULL UNIQUE,
              label TEXT NOT NULL,
              is_active BOOLEAN NOT NULL DEFAULT TRUE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        # Join tables
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_lifestyle_interests (
              user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              interest_type_id INT NOT NULL REFERENCES lifestyle_interest_types(id),
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              PRIMARY KEY (user_id, interest_type_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_lifestyle_preferred_services (
              user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              service_type_id INT NOT NULL REFERENCES lifestyle_service_types(id),
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              PRIMARY KEY (user_id, service_type_id)
            )
            """
        )

        # Helpful indexes (IF NOT EXISTS supported on PG 9.5+)
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS user_lifestyle_interests_interest_type_id_idx
            ON user_lifestyle_interests (interest_type_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS user_lifestyle_preferred_services_service_type_id_idx
            ON user_lifestyle_preferred_services (service_type_id)
            """
        )

        # profile_updated_at (explicit staleness signal)
        cur.execute(
            """
            ALTER TABLE lifestyle_profiles
              ADD COLUMN IF NOT EXISTS profile_updated_at TIMESTAMPTZ
            """
        )
        cur.execute(
            """
            UPDATE lifestyle_profiles
            SET profile_updated_at = COALESCE(updated_at, created_at, NOW())
            WHERE profile_updated_at IS NULL
            """
        )

        # ai_recommendations cache metadata
        cur.execute(
            """
            ALTER TABLE ai_recommendations
              ADD COLUMN IF NOT EXISTS generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            """
        )
        cur.execute(
            """
            ALTER TABLE ai_recommendations
              ADD COLUMN IF NOT EXISTS source_profile_updated_at TIMESTAMPTZ
            """
        )
        cur.execute(
            """
            ALTER TABLE ai_recommendations
              ADD COLUMN IF NOT EXISTS algorithm_version TEXT
            """
        )

        # Seed catalogs
        for slug, label in INTEREST_CATALOG:
            cur.execute(
                """
                INSERT INTO lifestyle_interest_types (slug, label)
                VALUES (%s, %s)
                ON CONFLICT (slug) DO UPDATE SET label = EXCLUDED.label
                """,
                (slug, label),
            )
        for slug, label in SERVICE_CATALOG:
            cur.execute(
                """
                INSERT INTO lifestyle_service_types (slug, label)
                VALUES (%s, %s)
                ON CONFLICT (slug) DO UPDATE SET label = EXCLUDED.label
                """,
                (slug, label),
            )

        conn.commit()
    except Exception:
        # Best-effort only. If this fails, callers should gracefully fall back.
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def get_profile_updated_at(user_id: int | str) -> datetime | None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT profile_updated_at
            FROM lifestyle_profiles
            WHERE user_id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()


def set_profile_updated_now(user_id: int | str) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE lifestyle_profiles
            SET profile_updated_at = NOW(), updated_at = NOW()
            WHERE user_id = %s
            """,
            (user_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def get_user_interest_slugs(user_id: int | str) -> list[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT it.slug
            FROM user_lifestyle_interests uli
            JOIN lifestyle_interest_types it ON it.id = uli.interest_type_id
            WHERE uli.user_id = %s
            ORDER BY it.slug
            """,
            (user_id,),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def get_user_preferred_service_slugs(user_id: int | str) -> list[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT st.slug
            FROM user_lifestyle_preferred_services ulps
            JOIN lifestyle_service_types st ON st.id = ulps.service_type_id
            WHERE ulps.user_id = %s
            ORDER BY st.slug
            """,
            (user_id,),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def replace_user_interests(user_id: int | str, interest_slugs: Iterable[str]) -> None:
    slugs = _normalize_slug_list(list(interest_slugs))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM user_lifestyle_interests WHERE user_id = %s", (user_id,))
        if slugs:
            cur.execute(
                """
                INSERT INTO user_lifestyle_interests (user_id, interest_type_id)
                SELECT %s, id
                FROM lifestyle_interest_types
                WHERE slug = ANY(%s)
                """,
                (user_id, slugs),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def replace_user_preferred_services(user_id: int | str, service_slugs: Iterable[str]) -> None:
    slugs = _normalize_slug_list(list(service_slugs))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM user_lifestyle_preferred_services WHERE user_id = %s", (user_id,))
        if slugs:
            cur.execute(
                """
                INSERT INTO user_lifestyle_preferred_services (user_id, service_type_id)
                SELECT %s, id
                FROM lifestyle_service_types
                WHERE slug = ANY(%s)
                """,
                (user_id, slugs),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def fetch_past_service_counts(user_id: int | str) -> dict[str, int]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT service_type, COUNT(*)
            FROM requests
            WHERE user_id = %s
            GROUP BY service_type
            """,
            (user_id,),
        )
        out: dict[str, int] = {}
        for service_type, cnt in cur.fetchall():
            out[str(service_type)] = int(cnt)
        return out
    finally:
        cur.close()
        conn.close()


def fetch_cached_recommendations(user_id: int | str) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT service_type, title, description, reason, match_score, metadata,
                   generated_at, source_profile_updated_at, algorithm_version
            FROM ai_recommendations
            WHERE user_id = %s AND is_dismissed = FALSE
            ORDER BY match_score DESC
            """,
            (user_id,),
        )
        rows = cur.fetchall()
        if not rows:
            return None

        recs: list[dict[str, Any]] = []
        max_source_profile_updated_at = None
        generated_at = None
        algorithm_version = None

        for r in rows:
            metadata = r[5] if isinstance(r[5], dict) else json.loads(r[5] or "{}")
            recs.append(
                {
                    "service_type": r[0],
                    "title": r[1],
                    "description": r[2],
                    "reason": r[3],
                    "match_score": int(r[4]),
                    "metadata": metadata,
                }
            )
            generated_at = r[6] or generated_at
            if r[7] is not None:
                max_source_profile_updated_at = (
                    r[7]
                    if max_source_profile_updated_at is None
                    else max(max_source_profile_updated_at, r[7])
                )
            algorithm_version = r[8] or algorithm_version

        meta = {
            "generated_at": generated_at,
            "source_profile_updated_at": max_source_profile_updated_at,
            "algorithm_version": algorithm_version,
        }
        return recs, meta
    finally:
        cur.close()
        conn.close()


def save_recommendations(
    user_id: int | str,
    recommendations: list[dict[str, Any]],
    *,
    source_profile_updated_at: datetime | None,
    algorithm_version: str,
) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM ai_recommendations WHERE user_id = %s", (user_id,))
        for rec in recommendations:
            cur.execute(
                """
                INSERT INTO ai_recommendations (
                    user_id, service_type, title, description, reason, match_score, metadata,
                    created_at, generated_at, source_profile_updated_at, algorithm_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, %s)
                """,
                (
                    user_id,
                    rec.get("service_type"),
                    rec.get("title") or rec.get("service_type"),
                    rec.get("description") or rec.get("reason") or "",
                    rec.get("reason") or "",
                    int(rec.get("match_score") or 0),
                    json.dumps(rec.get("metadata") or {}),
                    source_profile_updated_at,
                    algorithm_version,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def backfill_join_tables_from_legacy() -> dict[str, int]:
    """One-time backfill from lifestyle_profiles comma strings.

    Returns counts for visibility.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    inserted_interests = 0
    inserted_services = 0
    try:
        cur.execute(
            """
            SELECT user_id, interests, preferred_services
            FROM lifestyle_profiles
            """
        )
        rows = cur.fetchall()
        for user_id, interests_raw, services_raw in rows:
            interests = _normalize_slug_list(interests_raw)
            services = _normalize_slug_list(services_raw)

            # interests
            cur.execute("DELETE FROM user_lifestyle_interests WHERE user_id = %s", (user_id,))
            if interests:
                cur.execute(
                    """
                    INSERT INTO user_lifestyle_interests (user_id, interest_type_id)
                    SELECT %s, id
                    FROM lifestyle_interest_types
                    WHERE slug = ANY(%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (user_id, interests),
                )
                inserted_interests += cur.rowcount

            # services
            cur.execute(
                "DELETE FROM user_lifestyle_preferred_services WHERE user_id = %s",
                (user_id,),
            )
            if services:
                cur.execute(
                    """
                    INSERT INTO user_lifestyle_preferred_services (user_id, service_type_id)
                    SELECT %s, id
                    FROM lifestyle_service_types
                    WHERE slug = ANY(%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (user_id, services),
                )
                inserted_services += cur.rowcount

        conn.commit()
        return {"inserted_interests": inserted_interests, "inserted_services": inserted_services}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
