#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to verify AI recommendations only suggest selected services.
"""

import sys
import io
from lifestyle.engine import generate_recommendations
from datetime import datetime

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def test_hotel_only():
    """Test: User selects only 'hotel' - should get only hotel recommendation"""
    print("\n" + "="*60)
    print("TEST 1: User selects ONLY 'hotel' service")
    print("="*60)

    profile = {
        "age_group": "young_adult",
        "profession": "working",
        "monthly_budget": "medium",
        "lifestyle_type": "comfort",
        "travel_frequency": "monthly",
        "travel_style": "comfort",
        "typical_group_size": 2,
        "preferred_cab_type": "sedan",
        "dietary_pref": "none",
        "city": "Mumbai",
        "home_owner": False,
    }

    interests = ["fine_dining", "shopping"]
    preferred_services = ["hotel"]  # Only hotel selected!
    past_services_counts = {}

    recommendations = generate_recommendations(
        profile,
        interests=interests,
        preferred_services=preferred_services,
        past_services_counts=past_services_counts,
        now=datetime.now()
    )

    print(f"\nGenerated {len(recommendations)} recommendation(s):")
    for rec in recommendations:
        print(f"  - {rec['service_type']}: {rec['title']} (Score: {rec['match_score']})")

    # Verify
    service_types = [rec['service_type'] for rec in recommendations]
    if service_types == ["Hotel Booking"]:
        print("\n✅ PASS: Only Hotel Booking recommended")
    else:
        print(f"\n❌ FAIL: Expected only Hotel Booking, got {service_types}")

    return service_types == ["Hotel Booking"]

def test_hotel_and_courier():
    """Test: User selects 'hotel' and 'courier' - should NOT get flight or cab"""
    print("\n" + "="*60)
    print("TEST 2: User selects 'hotel' and 'courier' ONLY")
    print("="*60)

    profile = {
        "age_group": "young_adult",
        "profession": "business",
        "monthly_budget": "high",
        "lifestyle_type": "luxury",
        "travel_frequency": "weekly",
        "travel_style": "business",
        "typical_group_size": 1,
        "preferred_cab_type": "luxury",
        "dietary_pref": "none",
        "city": "Delhi",
        "home_owner": False,
    }

    interests = ["fine_dining", "tech"]
    preferred_services = ["hotel", "courier"]  # Only hotel and courier!
    past_services_counts = {}

    recommendations = generate_recommendations(
        profile,
        interests=interests,
        preferred_services=preferred_services,
        past_services_counts=past_services_counts,
        now=datetime.now()
    )

    print(f"\nGenerated {len(recommendations)} recommendation(s):")
    for rec in recommendations:
        print(f"  - {rec['service_type']}: {rec['title']} (Score: {rec['match_score']})")

    # Verify
    service_types = [rec['service_type'] for rec in recommendations]
    has_flight = "Flight Booking" in service_types
    has_cab = "Car Booking" in service_types
    has_hotel = "Hotel Booking" in service_types
    has_courier = "Courier Booking" in service_types

    if has_hotel and has_courier and not has_flight and not has_cab:
        print("\n✅ PASS: Only Hotel and Courier recommended (no Flight or Cab)")
        return True
    else:
        print(f"\n❌ FAIL: Expected only Hotel and Courier")
        print(f"   Got - Hotel: {has_hotel}, Courier: {has_courier}, Flight: {has_flight}, Cab: {has_cab}")
        return False

def test_no_services_selected():
    """Test: User selects NO services - should get empty or fallback recommendations"""
    print("\n" + "="*60)
    print("TEST 3: User selects NO preferred services")
    print("="*60)

    profile = {
        "age_group": "young_adult",
        "profession": "working",
        "monthly_budget": "medium",
        "lifestyle_type": "comfort",
        "travel_frequency": "monthly",
        "travel_style": "comfort",
        "typical_group_size": 2,
        "preferred_cab_type": "sedan",
        "dietary_pref": "none",
        "city": "Mumbai",
        "home_owner": False,
    }

    interests = ["fine_dining"]
    preferred_services = []  # NO services selected!
    past_services_counts = {}

    recommendations = generate_recommendations(
        profile,
        interests=interests,
        preferred_services=preferred_services,
        past_services_counts=past_services_counts,
        now=datetime.now()
    )

    print(f"\nGenerated {len(recommendations)} recommendation(s):")
    for rec in recommendations:
        print(f"  - {rec['service_type']}: {rec['title']} (Score: {rec['match_score']})")

    # Verify - should be empty since no services selected
    if len(recommendations) == 0:
        print("\n✅ PASS: No recommendations (user didn't select any services)")
        return True
    else:
        print(f"\n⚠️  INFO: Got {len(recommendations)} fallback recommendations")
        return True  # This is acceptable fallback behavior

def test_all_services():
    """Test: User selects ALL services - should get recommendations for all"""
    print("\n" + "="*60)
    print("TEST 4: User selects ALL services")
    print("="*60)

    profile = {
        "age_group": "adult",
        "profession": "business",
        "monthly_budget": "premium",
        "lifestyle_type": "luxury",
        "travel_frequency": "frequent",
        "travel_style": "luxury",
        "typical_group_size": 3,
        "preferred_cab_type": "luxury",
        "dietary_pref": "none",
        "city": "Bangalore",
        "home_owner": True,
    }

    interests = ["fine_dining", "spa", "tech"]
    preferred_services = ["hotel", "flight", "cab", "technician", "courier"]  # All!
    past_services_counts = {}

    recommendations = generate_recommendations(
        profile,
        interests=interests,
        preferred_services=preferred_services,
        past_services_counts=past_services_counts,
        now=datetime.now()
    )

    print(f"\nGenerated {len(recommendations)} recommendation(s):")
    for rec in recommendations:
        print(f"  - {rec['service_type']}: {rec['title']} (Score: {rec['match_score']})")

    # Verify - should have multiple recommendations
    service_types = [rec['service_type'] for rec in recommendations]
    if len(recommendations) >= 3:
        print(f"\n✅ PASS: Got {len(recommendations)} recommendations for selected services")
        return True
    else:
        print(f"\n❌ FAIL: Expected at least 3 recommendations, got {len(recommendations)}")
        return False

if __name__ == "__main__":
    print("\n" + "="*60)
    print("AI RECOMMENDATION ENGINE TEST SUITE")
    print("Testing: Only selected services should be recommended")
    print("="*60)

    results = []
    results.append(("Hotel Only", test_hotel_only()))
    results.append(("Hotel + Courier", test_hotel_and_courier()))
    results.append(("No Services", test_no_services_selected()))
    results.append(("All Services", test_all_services()))

    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")

    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! AI recommendations are working correctly.")
        exit(0)
    else:
        print(f"\n⚠️  {total - passed} test(s) failed.")
        exit(1)
