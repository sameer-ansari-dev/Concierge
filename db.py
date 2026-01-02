# db.py - UPDATED VERSION
import psycopg2

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="concierge_db",
        user="postgres",
        password="password"
    )

def save_user_profile_comprehensive(user_id, profile_data):
    """Save or update comprehensive user lifestyle profile to lifestyle_profiles table"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Check if profile exists
        cur.execute("SELECT id FROM lifestyle_profiles WHERE user_id = %s", (user_id,))
        existing = cur.fetchone()
        
        if existing:
            # Update existing profile
            cur.execute("""
                UPDATE lifestyle_profiles SET
                    age_group = %s,
                    profession = %s,
                    monthly_budget = %s,
                    lifestyle_type = %s,
                    travel_frequency = %s,
                    travel_style = %s,
                    typical_group_size = %s,
                    preferred_cab_type = %s,
                    dietary_pref = %s,
                    city = %s,
                    area = %s,
                    latitude = %s,
                    longitude = %s,
                    home_owner = %s,
                    interests = %s,
                    preferred_services = %s,
                    updated_at = NOW()
                WHERE user_id = %s
            """, (
                profile_data.get('age_group'),
                profile_data.get('profession'),
                profile_data.get('monthly_budget'),
                profile_data.get('lifestyle_type'),
                profile_data.get('travel_frequency'),
                profile_data.get('travel_style'),
                profile_data.get('typical_group_size'),
                profile_data.get('preferred_cab_type'),
                profile_data.get('dietary_pref'),
                profile_data.get('city'),
                profile_data.get('area'),
                profile_data.get('latitude'),
                profile_data.get('longitude'),
                profile_data.get('home_owner'),
                profile_data.get('interests'),
                profile_data.get('preferred_services'),
                user_id
            ))
        else:
            # Insert new profile
            cur.execute("""
                INSERT INTO lifestyle_profiles (
                    user_id, age_group, profession, monthly_budget, lifestyle_type,
                    travel_frequency, travel_style, typical_group_size, preferred_cab_type,
                    dietary_pref, city, area, latitude, longitude, home_owner,
                    interests, preferred_services
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user_id,
                profile_data.get('age_group'),
                profile_data.get('profession'),
                profile_data.get('monthly_budget'),
                profile_data.get('lifestyle_type'),
                profile_data.get('travel_frequency'),
                profile_data.get('travel_style'),
                profile_data.get('typical_group_size'),
                profile_data.get('preferred_cab_type'),
                profile_data.get('dietary_pref'),
                profile_data.get('city'),
                profile_data.get('area'),
                profile_data.get('latitude'),
                profile_data.get('longitude'),
                profile_data.get('home_owner'),
                profile_data.get('interests'),
                profile_data.get('preferred_services')
            ))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error saving comprehensive profile: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def get_user_profile(user_id):
    """Get comprehensive user lifestyle profile - UPDATED to remove latitude/longitude"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # REMOVED latitude and longitude from the SELECT query
        cur.execute("""
            SELECT 
                age_group, profession, monthly_budget, lifestyle_type,
                travel_frequency, travel_style, typical_group_size, preferred_cab_type,
                dietary_pref, city, area, home_owner,
                interests, preferred_services, created_at, updated_at
            FROM lifestyle_profiles 
            WHERE user_id = %s
        """, (user_id,))
        
        profile_data = cur.fetchone()
        
        if profile_data:
            return {
                'age_group': profile_data[0],
                'profession': profile_data[1],
                'monthly_budget': profile_data[2],
                'lifestyle_type': profile_data[3],
                'travel_frequency': profile_data[4],
                'travel_style': profile_data[5],
                'typical_group_size': profile_data[6],
                'preferred_cab_type': profile_data[7],
                'dietary_pref': profile_data[8],
                'city': profile_data[9],
                'area': profile_data[10],
                # Note: latitude and longitude are removed since they don't exist in the table
                'home_owner': profile_data[11],
                'interests': profile_data[12],
                'preferred_services': profile_data[13],
                'created_at': profile_data[14],
                'updated_at': profile_data[15]
            }
        return None
        
    except Exception as e:
        print(f"Error getting user profile: {e}")
        return None
    finally:
        cur.close()
        conn.close()