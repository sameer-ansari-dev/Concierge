# db.py - CLEAN VERSION
import psycopg2

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="concierge_db",
        user="postgres",
        password="password"
    )

def save_user_profile(user_id, interests, travel_style, dietary, group_size, cab_type, home_owner):
    """Save or update user lifestyle profile"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Check if profile exists
        cur.execute("SELECT id FROM user_profile WHERE user_id = %s", (user_id,))
        existing = cur.fetchone()
        
        if existing:
            # Update
            cur.execute("""
                UPDATE user_profile SET interests=%s, travel_style=%s, dietary_pref=%s,
                typical_group_size=%s, preferred_cab_type=%s, home_owner=%s WHERE user_id=%s
            """, (interests, travel_style, dietary, group_size, cab_type, home_owner, user_id))
        else:
            # Insert
            cur.execute("""
                INSERT INTO user_profile (user_id, interests, travel_style, dietary_pref, 
                typical_group_size, preferred_cab_type, home_owner)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (user_id, interests, travel_style, dietary, group_size, cab_type, home_owner))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error saving profile: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def get_user_profile(user_id):
    """Get user lifestyle profile"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT interests, travel_style, dietary_pref, 
                   typical_group_size, preferred_cab_type, home_owner
            FROM user_profile WHERE user_id = %s
        """, (user_id,))
        
        profile = cur.fetchone()
        
        if profile:
            return {
                'interests': profile[0].split(',') if profile[0] else [],
                'travel_style': profile[1],
                'dietary_pref': profile[2],
                'group_size': profile[3],
                'cab_type': profile[4],
                'home_owner': profile[5]
            }
        return None
    except Exception as e:
        print(f"Error getting profile: {e}")
        return None
    finally:
        cur.close()
        conn.close()