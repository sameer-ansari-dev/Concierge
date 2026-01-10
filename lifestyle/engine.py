from __future__ import annotations

from datetime import datetime
from typing import Any


def _dynamic_price_info(service_type: str, base_price_min: int, base_price_max: int, now: datetime) -> tuple[str, str]:
    hour = now.hour
    weekday = now.weekday()  # 0=Mon, 6=Sun

    multiplier = 1.0
    reasons: list[str] = []

    # Time-based Logic (copied from existing app.py logic)
    if service_type in ["Car Booking", "Luxury Cabs"]:
        if hour in [8, 9, 10, 17, 18, 19]:
            multiplier += 0.4
            reasons.append("Peak Traffic")
        elif hour >= 22 or hour <= 5:
            multiplier += 0.2
            reasons.append("Night Fare")

    elif service_type == "Hotel Booking":
        if weekday in [4, 5, 6]:  # Fri-Sun
            multiplier += 0.3
            reasons.append("Weekend Demand")
        elif hour >= 20:
            multiplier -= 0.1
            reasons.append("Late Night Deal")

    elif service_type == "Flight Booking":
        if weekday in [4, 5, 6]:
            multiplier += 0.2
            reasons.append("Weekend Travel")
        if hour <= 6:
            multiplier -= 0.1
            reasons.append("Early Bird")

    elif service_type == "Technician Booking":
        if weekday == 6:
            multiplier += 0.5
            reasons.append("Sunday Service")
        elif hour >= 18:
            multiplier += 0.25
            reasons.append("After Hours")

    final_min = int(base_price_min * multiplier)
    final_max = int(base_price_max * multiplier)

    price_str = f"₹{final_min:,}-{final_max:,}"
    if "night" in service_type.lower() or service_type == "Hotel Booking":
        price_str += "/night"
    elif "car" in service_type.lower() or "cab" in service_type.lower():
        price_str += "/trip"

    if reasons and multiplier > 1.0:
        reason_str = f"{', '.join(reasons)} (+{int((multiplier - 1) * 100)}%)"
    elif reasons and multiplier < 1.0:
        reason_str = f"{', '.join(reasons)} ({int((multiplier - 1) * 100)}%)"
    else:
        reason_str = ""

    return price_str, reason_str


def generate_recommendations(
    profile: dict[str, Any],
    *,
    interests: list[str],
    preferred_services: list[str],
    past_services_counts: dict[str, int],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Generate recommendation dicts.

    This intentionally mirrors the current behavior in app.py:/api/lifestyle-recommendations.
    """
    now = now or datetime.now()

    travel_frequency = profile.get("travel_frequency", "monthly")
    travel_style = profile.get("travel_style", "comfort")
    lifestyle_type = profile.get("lifestyle_type", "comfort")
    monthly_budget = profile.get("monthly_budget", "medium")
    typical_group_size = int(profile.get("typical_group_size", 1) or 1)
    preferred_cab_type = profile.get("preferred_cab_type", "sedan")
    home_owner = bool(profile.get("home_owner", False))
    city = profile.get("city", "")
    profession = profile.get("profession", "")

    recs: list[dict[str, Any]] = []

    # 1) Hotel
    hotel_score = 0
    hotel_reasons: list[str] = []

    if travel_frequency in ["monthly", "weekly", "frequent"]:
        hotel_score += 25
        hotel_reasons.append("frequent traveler")

    if lifestyle_type == "luxury":
        hotel_score += 30
        hotel_reasons.append("luxury lifestyle")
    elif lifestyle_type == "comfort":
        hotel_score += 20

    matched_interests = [i for i in ["fine_dining", "spa", "shopping", "fitness"] if i in interests]
    if matched_interests:
        hotel_score += len(matched_interests) * 10
        hotel_reasons.append(f"interests: {', '.join(matched_interests)}")

    if "hotel" in preferred_services:
        hotel_score += 25
        hotel_reasons.append("preferred service")

    hist_count = past_services_counts.get("Hotel Booking", 0)
    if hist_count > 0:
        hotel_score += min(30, 10 + (hist_count * 5))
        hotel_reasons.append(f"booked {hist_count} times")

    hotel_budget_ok = True
    
    # STRICT Budget tiers aligned with lifestyle_form.html:
    # - low: Under Rs 25,000/month → Strictly max Rs 3,000/night
    # - medium: Rs 25,000-75,000/month → max ~Rs 6,000/night
    # - high: Rs 75,000-1,50,000/month → max ~Rs 15,000/night
    # - premium: Above Rs 1,50,000/month → luxury range
    
    if monthly_budget == "low":
        # Strictly no luxury for low budget
        if lifestyle_type == "luxury" or travel_style == "luxury":
            hotel_budget_ok = False
        base_min, base_max = 1200, 2500 
        hotel_type = "Economy Hotels"
        max_recommended_nights = 2
    elif monthly_budget == "medium":
        if lifestyle_type == "luxury" and "fine_dining" not in interests:
            hotel_score -= 20 # Stronger penalty
        base_min, base_max = 2500, 5500
        hotel_type = "Comfort Hotels"
        max_recommended_nights = 3
    elif monthly_budget == "high":
        if hotel_score > 0:
            hotel_score += 15
            hotel_reasons.append("premium budget")
        base_min, base_max = 6000, 15000 
        hotel_type = "Premium Hotels"
        max_recommended_nights = 5
    else:  # premium
        if hotel_score > 0:
            hotel_score += 20
            hotel_reasons.append("unlimited budget")
        base_min, base_max = 15000, 45000 
        hotel_type = "Ultra-Luxury Resorts"
        max_recommended_nights = 10

    # ONLY recommend hotel if user selected it in preferred_services
    if "hotel" in preferred_services and hotel_score >= 40 and hotel_budget_ok:
        price_str, price_reason = _dynamic_price_info("Hotel Booking", base_min, base_max, now)

        # Calculate estimated total for typical group size
        rooms_needed = max(1, (typical_group_size + 1) // 2)  # ~2 guests per room
        estimated_total_min = base_min * rooms_needed * 2  # 2 nights minimum
        estimated_total_max = base_max * rooms_needed * max_recommended_nights

        recs.append(
            {
                "service_type": "Hotel Booking",
                "title": "Hotel Booking",
                "description": f"Perfect for {', '.join(hotel_reasons[:2])}.",
                "reason": f"Perfect for {', '.join(hotel_reasons[:2])}.",
                "match_score": min(95, hotel_score),
                "metadata": {
                    "price": price_str,
                    "price_reason": price_reason,
                    "hotel_type": hotel_type,
                    "location": city or "Major Cities",
                    "amenities": "Matched to your preferences",
                    "guests": typical_group_size,
                    "rooms_suggested": rooms_needed,
                    "max_nights_recommended": max_recommended_nights,
                    "estimated_trip_cost": f"₹{estimated_total_min:,}-{estimated_total_max:,}",
                },
            }
        )

    # 2) Flight
    flight_score = 0
    flight_reasons: list[str] = []

    if travel_frequency in ["weekly", "frequent"]:
        flight_score += 40
        flight_reasons.append("frequent flyer")
    elif travel_frequency == "monthly":
        flight_score += 25
        flight_reasons.append("monthly traveler")

    if "flight" in preferred_services:
        flight_score += 30
        flight_reasons.append("preferred service")

    hist_count = past_services_counts.get("Flight Booking", 0)
    if hist_count > 0:
        flight_score += min(30, 10 + (hist_count * 5))
        flight_reasons.append(f"booked {hist_count} times")

    if travel_style == "business":
        flight_score += 20
        flight_reasons.append("business travel")

    flight_budget_ok = True
    base_min, base_max = 2500, 6000

    if monthly_budget == "low":
        if travel_style in ["business", "luxury"] or lifestyle_type == "luxury":
            flight_score -= 40
            flight_budget_ok = False
        base_min, base_max = 2000, 4500
    elif monthly_budget == "medium" and lifestyle_type == "luxury":
        flight_score -= 10

    # ONLY recommend flight if user selected it in preferred_services
    if "flight" in preferred_services and flight_score >= 40 and flight_budget_ok:
        if monthly_budget in ["high", "premium"] and (travel_style == "business" or lifestyle_type == "luxury"):
            travel_class = "Business Class"
            base_min, base_max = 12000, 35000
        elif monthly_budget in ["medium", "high"] or travel_style == "comfort":
            travel_class = "Premium Economy"
            base_min, base_max = 6000, 12000
        else:
            travel_class = "Economy"
            base_min, base_max = 2500, 6000

        price_str, price_reason = _dynamic_price_info("Flight Booking", base_min, base_max, now)
        recs.append(
            {
                "service_type": "Flight Booking",
                "title": "Flight Booking",
                "description": f"Ideal for {', '.join(flight_reasons[:2])}.",
                "reason": f"Ideal for {', '.join(flight_reasons[:2])}.",
                "match_score": min(90, flight_score),
                "metadata": {
                    "price": price_str,
                    "price_reason": price_reason,
                    "class": travel_class,
                    "routes": "Domestic & International",
                    "passengers": typical_group_size,
                },
            }
        )

    # 3) Car
    car_score = 0
    car_reasons: list[str] = []

    if typical_group_size > 3:
        car_score += 25
        car_reasons.append(f"group of {typical_group_size}")

    if preferred_cab_type == "luxury" or lifestyle_type == "luxury":
        car_score += 30
        car_reasons.append("luxury preference")
    elif preferred_cab_type in ["suv", "sedan"]:
        car_score += 20
        car_reasons.append(f"{preferred_cab_type} preference")

    if "cab" in preferred_services:
        car_score += 25
        car_reasons.append("preferred service")

    hist_count = past_services_counts.get("Car Booking", 0)
    if hist_count > 0:
        car_score += min(30, 10 + (hist_count * 5))
        car_reasons.append(f"booked {hist_count} times")

    car_budget_ok = True
    if monthly_budget == "low":
        if preferred_cab_type == "luxury" or lifestyle_type == "luxury":
            car_score -= 40
            car_budget_ok = False
        base_min, base_max = 400, 1000
    elif monthly_budget == "medium" and preferred_cab_type == "luxury":
        car_score -= 10

    # ONLY recommend car/cab if user selected it in preferred_services
    if "cab" in preferred_services and car_score >= 40 and car_budget_ok:
        if monthly_budget in ["high", "premium"] and (preferred_cab_type == "luxury" or lifestyle_type == "luxury"):
            cab_type = "Luxury Cabs (BMW/Merc)"
            base_min, base_max = 3000, 7000
        elif monthly_budget != "low" and (preferred_cab_type == "suv" or typical_group_size > 3):
            cab_type = "Premium SUV"
            base_min, base_max = 1800, 3500
        elif monthly_budget == "low":
            cab_type = "Budget Sedan"
            base_min, base_max = 400, 1000
        else:
            cab_type = "Comfort Sedan"
            base_min, base_max = 800, 1800

        price_str, price_reason = _dynamic_price_info("Car Booking", base_min, base_max, now)
        recs.append(
            {
                "service_type": "Car Booking",
                "title": "Car Booking",
                "description": f"Best for {', '.join(car_reasons)}.",
                "reason": f"Best for {', '.join(car_reasons)}.",
                "match_score": min(85, car_score),
                "metadata": {
                    "price": price_str,
                    "price_reason": price_reason,
                    "vehicle": cab_type,
                    "capacity": f"Up to {max(4, typical_group_size)} passengers",
                },
            }
        )

    # 4) Technician - ONLY recommend if user selected it in preferred_services
    if "technician" in preferred_services and home_owner:
        tech_score = 60
        tech_reasons = ["home owner"]

        if any(i in interests for i in ["tech", "fitness", "music", "art"]):
            tech_score += 15
            tech_reasons.append("home maintenance needs")

        if "technician" in preferred_services:
            tech_score += 20
            tech_reasons.append("preferred service")

        hist_count = past_services_counts.get("Technician Booking", 0)
        if hist_count > 0:
            tech_score += min(30, 10 + (hist_count * 5))
            tech_reasons.append(f"booked {hist_count} times")

        price_str, price_reason = _dynamic_price_info("Technician Booking", 500, 2000, now)
        recs.append(
            {
                "service_type": "Technician Booking",
                "title": "Technician Booking",
                "description": f"Essential for {', '.join(tech_reasons)}.",
                "reason": f"Essential for {', '.join(tech_reasons)}.",
                "match_score": min(90, tech_score),
                "metadata": {
                    "price": price_str,
                    "price_reason": price_reason,
                    "availability": "Same-day & Emergency",
                    "services": "AC, Plumbing, Electrical, Carpentry",
                },
            }
        )

    # 5) Courier
    courier_score = 0
    courier_reasons: list[str] = []

    if "courier" in preferred_services:
        courier_score += 40
        courier_reasons.append("preferred service")

    if travel_style == "business" or str(profession).lower() in ["business", "working", "freelancer"]:
        courier_score += 25
        courier_reasons.append(f"{profession} needs")

    delivery_type = "Standard Delivery"
    base_min, base_max = 100, 300

    if monthly_budget in ["high", "premium"]:
        courier_score += 15
        courier_reasons.append("express delivery budget")
        delivery_type = "Express Delivery"
        base_min, base_max = 300, 800
    elif monthly_budget == "medium":
        delivery_type = "Standard/Express"
        base_min, base_max = 150, 500

    hist_count = past_services_counts.get("Courier Booking", 0)
    if hist_count > 0:
        courier_score += min(30, 10 + (hist_count * 5))
        courier_reasons.append(f"booked {hist_count} times")

    # ONLY recommend courier if user selected it in preferred_services
    if "courier" in preferred_services and courier_score >= 40:
        price_str, price_reason = _dynamic_price_info("Courier Booking", base_min, base_max, now)
        recs.append(
            {
                "service_type": "Courier Booking",
                "title": "Courier Booking",
                "description": f"Useful for {', '.join(courier_reasons)}.",
                "reason": f"Useful for {', '.join(courier_reasons)}.",
                "match_score": min(80, courier_score),
                "metadata": {
                    "price": price_str,
                    "price_reason": price_reason,
                    "delivery": delivery_type,
                    "tracking": "Real-time GPS Tracking",
                },
            }
        )

    # If no recommendations match, show only the services user selected in their preferences
    if not recs and preferred_services:
        # Generate basic recommendations ONLY for user's preferred services
        for service in preferred_services:
            if service == "hotel":
                recs.append({
                    "service_type": "Hotel Booking",
                    "title": "Hotel Booking",
                    "description": "Great for weekend getaways and business trips",
                    "reason": "Based on your service preference",
                    "match_score": 70,
                    "metadata": {"price": "₹3,000-15,000/night", "location": "Popular Destinations", "amenities": "Basic to Premium"},
                })
            elif service == "flight":
                recs.append({
                    "service_type": "Flight Booking",
                    "title": "Flight Booking",
                    "description": "Perfect for domestic and international travel",
                    "reason": "Based on your service preference",
                    "match_score": 70,
                    "metadata": {"price": "₹2,500-12,000/person", "class": "Economy to Business", "routes": "All destinations"},
                })
            elif service == "cab":
                recs.append({
                    "service_type": "Car Booking",
                    "title": "Car Booking",
                    "description": "Convenient for local travel and airport transfers",
                    "reason": "Based on your service preference",
                    "match_score": 70,
                    "metadata": {"price": "₹800-3,000/trip", "vehicle": "Standard to Luxury", "capacity": "Up to 4 passengers"},
                })
            elif service == "technician":
                recs.append({
                    "service_type": "Technician Booking",
                    "title": "Technician Booking",
                    "description": "Home repair and maintenance services",
                    "reason": "Based on your service preference",
                    "match_score": 70,
                    "metadata": {"price": "₹500-2,000/service", "availability": "Same-day available", "services": "AC, Plumbing, Electrical"},
                })
            elif service == "courier":
                recs.append({
                    "service_type": "Courier Booking",
                    "title": "Courier Booking",
                    "description": "Fast and reliable package delivery",
                    "reason": "Based on your service preference",
                    "match_score": 70,
                    "metadata": {"price": "₹150-500/package", "delivery": "Standard/Express", "tracking": "Real-time tracking"},
                })

    recs.sort(key=lambda x: int(x.get("match_score") or 0), reverse=True)
    return recs[:5]
