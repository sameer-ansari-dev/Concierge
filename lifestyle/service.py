from __future__ import annotations

from datetime import datetime
from typing import Any

from lifestyle import engine
from lifestyle import repository


def recompute_recommendations(
    user_id: int | str,
    *,
    force: bool = False,
    algorithm_version: str = "v1",
) -> dict[str, Any]:
    """Return recommendations, recomputing if stale.

    Freshness rule: cached recs are valid when:
      cached.source_profile_updated_at >= lifestyle_profiles.profile_updated_at
    """
    # Best-effort schema ensure; if DDL fails, we'll fall back to legacy behavior.
    try:
        repository.ensure_preference_schema()
    except Exception:
        # Can't create/alter tables in this environment; proceed without normalization
        pass

    profile_updated_at = None
    try:
        profile_updated_at = repository.get_profile_updated_at(user_id)
    except Exception:
        profile_updated_at = None

    cached = None
    try:
        cached = repository.fetch_cached_recommendations(user_id)
    except Exception:
        cached = None

    if not force and cached is not None and profile_updated_at is not None:
        recs, meta = cached
        cached_profile_updated_at = meta.get("source_profile_updated_at")
        if cached_profile_updated_at is not None and cached_profile_updated_at >= profile_updated_at:
            return {
                "has_profile": True,
                "source": "database",
                "recommendations": recs,
                "profile_updated_at": profile_updated_at.isoformat() if profile_updated_at else None,
                "generated_at": meta.get("generated_at").isoformat() if meta.get("generated_at") else None,
                "algorithm_version": meta.get("algorithm_version"),
            }

    # Need to generate
    from db import get_user_profile  # local import to avoid circular

    profile = get_user_profile(user_id)
    if not profile:
        return {
            "has_profile": False,
            "source": "generated",
            "recommendations": [],
            "profile_updated_at": None,
            "generated_at": None,
            "algorithm_version": algorithm_version,
        }

    # Normalize/obtain interests + preferred_services
    interests: list[str] = []
    preferred_services: list[str] = []
    try:
        repository.ensure_preference_schema()
        interests = repository.get_user_interest_slugs(user_id)
        preferred_services = repository.get_user_preferred_service_slugs(user_id)
    except Exception:
        interests = []
        preferred_services = []

    # Fallback to legacy string fields
    if not interests:
        interests = repository._normalize_slug_list(profile.get("interests"))
    if not preferred_services:
        preferred_services = repository._normalize_slug_list(profile.get("preferred_services"))

    past_counts = {}
    try:
        past_counts = repository.fetch_past_service_counts(user_id)
    except Exception:
        past_counts = {}

    recs = engine.generate_recommendations(
        profile,
        interests=interests,
        preferred_services=preferred_services,
        past_services_counts=past_counts,
        now=datetime.now(),
    )

    # Persist recommendations with staleness metadata
    try:
        if profile_updated_at is None:
            try:
                profile_updated_at = repository.get_profile_updated_at(user_id)
            except Exception:
                profile_updated_at = None
        repository.save_recommendations(
            user_id,
            recs,
            source_profile_updated_at=profile_updated_at,
            algorithm_version=algorithm_version,
        )
    except Exception:
        # Persistence failure shouldn't break API response
        pass

    return {
        "has_profile": True,
        "source": "generated",
        "recommendations": recs,
        "profile_updated_at": profile_updated_at.isoformat() if profile_updated_at else None,
        "generated_at": datetime.now().isoformat(),
        "algorithm_version": algorithm_version,
    }
