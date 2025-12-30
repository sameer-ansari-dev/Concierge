from flask import (
    Flask, render_template, request, redirect, url_for, flash, session,
    make_response, jsonify, send_from_directory, current_app
)
from flask_socketio import SocketIO, emit, join_room
import random
from db import save_user_profile, get_user_profile
from datetime import datetime, date, timedelta
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from db import get_db_connection
import psycopg2
import json
import os
from pathlib import Path
import logging
import threading
import time
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
import io
import base64

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'your_secret_key'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ---------------------- Flask-Login ----------------------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    pass

@login_manager.user_loader
def load_user(user_id):
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        return None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, username FROM users WHERE id = %s", (user_id,))
        user_data = cur.fetchone()
        if user_data:
            user = User()
            user.id = str(user_data[0])
            return user
    except Exception as e:
        logger.error(f"load_user error: {e}")
    finally:
        cur.close()
        conn.close()
    return None

@app.context_processor
def inject_common_variables():
    """Inject common variables into all templates automatically"""
    if current_user.is_authenticated:
        user_id = current_user.get_id()
        
        # Get user data
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT username, full_name FROM users WHERE id = %s", (user_id,))
            user_data = cur.fetchone()
            username = user_data[1] if user_data and user_data[1] else (user_data[0] if user_data else 'User')
        except:
            username = 'User'
        finally:
            cur.close()
            conn.close()
        
        # Get notifications and counts
        notifications = get_user_notifications(user_id)
        unread_count = get_unread_count(user_id)
        
        # Get requests
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
                FROM requests 
                WHERE user_id = %s 
                ORDER BY created_at DESC
            """, (user_id,))
            user_requests_raw = cur.fetchall()
            
            requests = []
            for req in user_requests_raw:
                try:
                    details = json.loads(req[3]) if isinstance(req[3], str) else req[3]
                except:
                    details = {}
                
                requests.append({
                    "id": req[0],
                    "booking_id": req[1],
                    "service_type": req[2],
                    "details": details,
                    "payment_status": req[4],
                    "admin_confirmation": req[5],
                })
        except:
            requests = []
        finally:
            cur.close()
            conn.close()
        
        # Check lifestyle profile
        profile = get_user_profile(user_id)
        has_profile = profile is not None and profile.get('interests')
        
        # Get contact info
        conn = get_db_connection()
        cur = conn.cursor()
        contact = {}
        try:
            cur.execute("""
                SELECT full_name, email, phone, address, whatsapp, instagram, facebook
                FROM users WHERE id = %s
            """, (user_id,))
            contact_data = cur.fetchone()
            if contact_data:
                contact = {
                    'name': contact_data[0] or '',
                    'email': contact_data[1] or '',
                    'phone': contact_data[2] or '',
                    'address': contact_data[3] or '',
                    'whatsapp': contact_data[4] or '',
                    'instagram': contact_data[5] or '',
                    'facebook': contact_data[6] or ''
                }
        except:
            pass
        finally:
            cur.close()
            conn.close()
        
        return {
            'current_user_id': user_id,
            'user': username,
            'unread_count': unread_count,
            'notifications': notifications,
            'has_lifestyle_profile': has_profile,
            'contact': contact,
            'requests': requests,
            'request_count': len(requests)
        }
    
    # Not authenticated
    return {
        'current_user_id': None,
        'user': 'Guest',
        'unread_count': 0,
        'notifications': [],
        'has_lifestyle_profile': False,
        'contact': {},
        'requests': [],
        'request_count': 0
    }

# ---------------------- Lifestyle Profile Routes ----------------------
@app.route('/lifestyle_form')
@login_required
def lifestyle_form():
    """Display the lifestyle form"""
    profile = get_user_profile(current_user.get_id())
    return render_template('lifestyle_form.html', profile=profile)

@app.route('/save_lifestyle', methods=['POST'])
@login_required
def save_lifestyle():
    """Save lifestyle data to database"""
    try:
        user_id = current_user.get_id()
        
        interests = request.form.getlist('interests')
        travel_style = request.form.get('travel_style', '')
        dietary = request.form.get('dietary', 'none')
        group_size = request.form.get('group_size', 1, type=int)
        cab_type = request.form.get('cab_type', 'economy')
        home_owner = request.form.get('home_owner') == 'yes'
        
        interests_str = ','.join(interests) if interests else ''
        
        success = save_user_profile(
            user_id=user_id,
            interests=interests_str,
            travel_style=travel_style,
            dietary=dietary,
            group_size=group_size,
            cab_type=cab_type,
            home_owner=home_owner
        )
        
        if success:
            flash('✅ Your lifestyle profile has been saved! You will now get personalized suggestions.', 'success')
        else:
            flash('❌ Error saving profile. Please try again.', 'error')
            
    except Exception as e:
        print(f"Error in save_lifestyle: {e}")
        flash('⚠️ An unexpected error occurred.', 'error')
    
    return redirect(url_for('dashboard'))

# ---------------------- Static dirs ----------------------

def get_tickets_dir():
    static_folder = app.static_folder or os.path.join(app.root_path, 'static')
    tickets_dir = Path(static_folder) / "tickets"
    tickets_dir.mkdir(parents=True, exist_ok=True)
    return tickets_dir

TICKETS_DIR = get_tickets_dir()

# ---------------------- PDF Ticket Generation ----------------------
@app.route('/admin/generate-ticket', methods=['POST'])
def admin_generate_ticket():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    data = request.get_json()
    request_id = data.get('request_id')
    booking_id = data.get('booking_id')
    service_type = data.get('service_type')
    user_id = data.get('user_id')
    
    if not all([request_id, booking_id, service_type, user_id]):
        return jsonify({"error": "Missing parameters"}), 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT details FROM requests WHERE id = %s", (request_id,))
        request_data = cur.fetchone()
        
        if not request_data:
            return jsonify({"error": "Request not found"}), 404
        
        details = request_data[0]
        try:
            details_obj = json.loads(details) if isinstance(details, str) else details
        except:
            details_obj = {"raw": str(details)}
        
        pdf_filename = create_pdf_ticket_for_booking(booking_id, service_type, details_obj, user_id)
        
        details_obj["ticket_pdf_url"] = f"/static/tickets/{pdf_filename}"
        details_obj["ticket_generated_at"] = datetime.now().isoformat()
        
        cur.execute("UPDATE requests SET details = %s::jsonb WHERE id = %s",
                   (json.dumps(details_obj), request_id))
        conn.commit()
        
        return jsonify({
            "success": True,
            "message": "PDF ticket generated successfully",
            "ticket_url": f"/static/tickets/{pdf_filename}",
            "booking_id": booking_id
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()
        
def generate_pdf_ticket(booking_id, service_type, details, user_id):
    """Generate professional PDF ticket"""
    filename = f"ticket_{booking_id}.pdf"
    filepath = TICKETS_DIR / filename
    
    doc = SimpleDocTemplate(str(filepath), pagesize=A4)
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'TitleStyle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#2c3e50'),
        spaceAfter=20,
        alignment=TA_CENTER
    )
    
    header_style = ParagraphStyle(
        'HeaderStyle',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=colors.HexColor('#34495e'),
        spaceAfter=10
    )
    
    normal_style = ParagraphStyle(
        'NormalStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2c3e50')
    )
    
    story.append(Paragraph("CONCIERGE LIFESTYLE", title_style))
    story.append(Paragraph(f"{service_type} - Booking Ticket", header_style))
    story.append(Spacer(1, 20))
    
    booking_data = [
        ["Booking ID:", booking_id],
        ["Generated On:", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        ["Ticket Status:", "CONFIRMED"]
    ]
    
    booking_table = Table(booking_data, colWidths=[200, 300])
    booking_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#ecf0f1')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#2c3e50')),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
    ]))
    story.append(booking_table)
    story.append(Spacer(1, 30))
    
    story.append(Paragraph("Booking Details", header_style))
    story.append(Spacer(1, 10))
    
    if service_type == 'Car Booking':
        car_data = [
            ["Car Model:", details.get('car_model', 'N/A')],
            ["Cab Class:", details.get('cab_class', 'Standard')],
            ["Pickup Location:", details.get('pickup', 'N/A')],
            ["Drop-off Location:", details.get('dropoff', 'N/A')],
            ["Pickup Date:", details.get('pickup_date', 'N/A')],
            ["Pickup Time:", details.get('pickup_time', 'N/A')],
            ["Passengers:", str(details.get('passengers', 1))],
            ["Total Amount:", f"₹{details.get('total_price', 0)}"],
            ["Booking Status:", "Confirmed"]
        ]
        
        table = Table(car_data, colWidths=[200, 300])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#3498db')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.white),
            ('BACKGROUND', (1, 0), (1, -1), colors.HexColor('#ecf0f1')),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#2c3e50')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
        ]))
        story.append(table)
        
        otp = random.randint(1000, 9999)
        story.append(Spacer(1, 30))
        story.append(Paragraph("Driver Verification", header_style))
        story.append(Spacer(1, 10))
        story.append(Paragraph(f"Verification Code: <font size='20' color='#e74c3c'><b>{otp}</b></font>", 
                              ParagraphStyle('OTPStyle', parent=styles['Normal'], fontSize=12)))
        story.append(Paragraph("Show this code to your driver for verification", normal_style))
        
    elif service_type == 'Hotel Booking':
        hotel_data = [
            ["Hotel Name:", details.get('hotel_name', 'N/A')],
            ["Check-in Date:", details.get('checkin', 'N/A')],
            ["Check-out Date:", details.get('checkout', 'N/A')],
            ["Rooms:", str(details.get('rooms', 1))],
            ["Guests:", str(details.get('guests', 1))],
            ["Total Amount:", f"₹{details.get('total_amount', 0)}"],
            ["Booking Status:", "Confirmed"],
            ["Confirmation Number:", f"HL-{random.randint(100000, 999999)}"]
        ]
        
        table = Table(hotel_data, colWidths=[200, 300])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#27ae60')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.white),
            ('BACKGROUND', (1, 0), (1, -1), colors.HexColor('#ecf0f1')),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#2c3e50')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
        ]))
        story.append(table)
        
    elif service_type == 'Flight Booking':
        flight_data = [
            ["Airline:", details.get('airline', 'N/A')],
            ["Flight Number:", details.get('flight_no', details.get('flight', {}).get('flight_no', 'N/A'))],
            ["Departure:", f"{details.get('origin', 'N/A')} ({details.get('origin_code', 'XXX')})"],
            ["Arrival:", f"{details.get('destination', 'N/A')} ({details.get('destination_code', 'YYY')})"],
            ["Departure Time:", details.get('departure_time', 'N/A')],
            ["Arrival Time:", details.get('arrival_time', 'N/A')],
            ["Travel Class:", details.get('travel_class', 'Economy').title()],
            ["Duration:", details.get('duration', 'N/A')],
            ["Baggage Allowance:", details.get('baggage_allowance', '20kg')],
            ["Total Amount:", f"₹{details.get('price', 0)}"],
            ["PNR:", f"{random.choice(['AI', '6E', 'SG', 'UK'])}-{random.randint(1000000, 9999999)}"]
        ]
        
        table = Table(flight_data, colWidths=[200, 300])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#9b59b6')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.white),
            ('BACKGROUND', (1, 0), (1, -1), colors.HexColor('#ecf0f1')),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#2c3e50')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
        ]))
        story.append(table)
        
    elif service_type == 'Technician Booking':
        tech_data = [
            ["Service Type:", details.get('service_type', 'N/A').replace('_', ' ').title()],
            ["Technician:", details.get('name', 'Assigned Technician')],
            ["Service Date:", details.get('service_date', 'N/A')],
            ["Service Time:", details.get('service_time', 'N/A')],
            ["Location:", details.get('location', 'N/A')],
            ["Issue Description:", details.get('description', 'N/A')],
            ["Urgency:", details.get('urgency', 'Normal').title()],
            ["Service Charge:", f"₹{details.get('total_price', 0)}"],
            ["Technician ID:", details.get('technician_id', 'N/A')]
        ]
        
        table = Table(tech_data, colWidths=[200, 300])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e67e22')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.white),
            ('BACKGROUND', (1, 0), (1, -1), colors.HexColor('#ecf0f1')),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#2c3e50')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
        ]))
        story.append(table)
        
    elif service_type == 'Courier Booking':
        courier_data = [
            ["Courier Service:", details.get('courier_name', 'N/A')],
            ["Pickup Location:", details.get('pickup_location', 'N/A')],
            ["Delivery Location:", details.get('dropoff_location', 'N/A')],
            ["Pickup Date:", details.get('pickup_date', 'N/A')],
            ["Pickup Time:", details.get('pickup_time', 'N/A')],
            ["Package Weight:", f"{details.get('package_weight_kg', 0)} kg"],
            ["Courier Type:", details.get('courier_type', 'Standard').title()],
            ["Delivery Duration:", details.get('delivery_duration', 'N/A')],
            ["Shipping Cost:", f"₹{details.get('total_price_inr', 0)}"],
            ["Tracking ID:", f"TRK-{random.randint(1000000000, 9999999999)}"]
        ]
        
        table = Table(courier_data, colWidths=[200, 300])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#1abc9c')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.white),
            ('BACKGROUND', (1, 0), (1, -1), colors.HexColor('#ecf0f1')),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#2c3e50')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
        ]))
        story.append(table)
    
    story.append(Spacer(1, 30))
    story.append(Paragraph("Terms & Conditions", header_style))
    terms = [
        "1. This ticket is non-transferable and valid only for the booked service.",
        "2. Please present this ticket for verification at the time of service.",
        "3. Cancellations must be made at least 24 hours in advance for full refund.",
        "4. Concierge Lifestyle is not responsible for delays due to traffic, weather, or other unforeseen circumstances.",
        "5. For any issues, contact support@conciergelifestyle.com or call +91-9876543210."
    ]
    
    for term in terms:
        story.append(Paragraph(f"• {term}", normal_style))
        story.append(Spacer(1, 3))
    
    story.append(Spacer(1, 20))
    footer = Paragraph(
        "Thank you for choosing Concierge Lifestyle<br/>"
        "Your trusted partner for premium services<br/>"
        "www.conciergelifestyle.com | support@conciergelifestyle.com",
        ParagraphStyle('FooterStyle', parent=styles['Normal'], fontSize=9, 
                      textColor=colors.HexColor('#7f8c8d'), alignment=TA_CENTER)
    )
    story.append(footer)
    
    doc.build(story)
    
    return filename

def create_pdf_ticket_for_booking(booking_id, service_type, details, user_id):
    """Create PDF ticket and return the filename"""
    filename = generate_pdf_ticket(booking_id, service_type, details, user_id)
    
    details['ticket_pdf_url'] = f"/static/tickets/{filename}"
    details['ticket_generated_at'] = datetime.now().isoformat()
    
    return filename

# ---------------------- Static Mock Data ----------------------
hotels_data = {
    "Mumbai": [
        {"name": "The Taj Mahal Palace", "address": "Apollo Bunder Road, Colaba, Mumbai – 400001, Maharashtra", "couple_friendly": True, "free_wifi": True, "rating": 4.7, "price": 18425, "image": "images/mumbai/mumbai1.jpg"},
        {"name": "The Oberoi, Mumbai", "address": "Nariman Point, Marine Drive, Mumbai – 400021, Maharashtra", "couple_friendly": True, "free_wifi": True, "rating": 4.9, "price": 11904, "image": "images/mumbai/mumbai2.jpg"},
        {"name": "Trident Nariman Point", "address": "Nariman Point, Mumbai – 400021, Maharashtra", "couple_friendly": True, "free_wifi": True, "rating": 4.6, "price": 9440, "image": "images/mumbai/mumbai3.jpg"},
    ],
    "Pune": [
        {"name": "JW Marriott Hotel Pune", "address": "Senapati Bapat Road, Pune – 411053, Maharashtra", "couple_friendly": True, "free_wifi": True, "rating": 4.6, "price": 9800, "image": "images/pune/pune1.jpg"},
        {"name": "Conrad Pune", "address": "7 Mangaldas Road, Pune – 411001, Maharashtra", "couple_friendly": True, "free_wifi": True, "rating": 4.7, "price": 8900, "image": "images/pune/pune2.jpg"},
    ]
}

cars_data = [
    {"model": "Toyota Etios", "seats": 4, "luggage": 2, "fuel_type": "CNG/Petrol/Diesel", "price": 936, "cab_class": "Standard", "pickup_time": "10:00", "dropoff_time": "12:00", "duration": "2h", "status": "Available"},
    {"model": "Honda City", "seats": 4, "luggage": 3, "fuel_type": "Petrol", "price": 1200, "cab_class": "Standard", "pickup_time": "09:00", "dropoff_time": "11:30", "duration": "2h 30m", "status": "Available"},
    {"model": "Toyota Fortuner", "seats": 6, "luggage": 4, "fuel_type": "Diesel", "price": 1500, "cab_class": "SUV", "pickup_time": "11:00", "dropoff_time": "13:00", "duration": "2h", "status": "Available"},
    {"model": "BMW 5 Series", "seats": 4, "luggage": 2, "fuel_type": "Petrol", "price": 2000, "cab_class": "Luxury", "pickup_time": "08:00", "dropoff_time": "10:00", "duration": "2h", "status": "Available"},
    {"model": "Maruti Eeco", "seats": 7, "luggage": 5, "fuel_type": "CNG", "price": 1800, "cab_class": "Standard", "pickup_time": "12:00", "dropoff_time": "14:30", "duration": "2h 30m", "status": "Available"},
    {"model": "Hyundai Creta", "seats": 5, "luggage": 3, "fuel_type": "Diesel", "price": 1400, "cab_class": "SUV", "pickup_time": "10:30", "dropoff_time": "12:30", "duration": "2h", "status": "Available"},
]

technicians_data = [
    {"id": "T001", "name": "Amit Sharma", "service_type": "ac_repair", "experience": 5, "rating": 4.8, "price": 800, "availability": "Available", "location": "Mumbai"},
    {"id": "T002", "name": "Rahul Patel", "service_type": "plumbing", "experience": 7, "rating": 4.6, "price": 600, "availability": "Available", "location": "Mumbai"},
    {"id": "T003", "name": "Sanjay Kumar", "service_type": "electrical", "experience": 10, "rating": 4.9, "price": 900, "availability": "Available", "location": "Mumbai"},
    {"id": "T004", "name": "Vikram Singh", "service_type": "carpentry", "experience": 4, "rating": 4.5, "price": 700, "availability": "Available", "location": "Mumbai"},
    {"id": "T005", "name": "Deepak Yadav", "service_type": "ac_repair", "experience": 6, "rating": 4.7, "price": 850, "availability": "Available", "location": "Pune"},
    {"id": "T006", "name": "Ravi Gupta", "service_type": "plumbing", "experience": 8, "rating": 4.8, "price": 650, "availability": "Available", "location": "Pune"},
]

# ---------------------- JSON helpers ----------------------
def delete_notification(notification_id, user_id):
    """Delete a specific notification for a user"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM notifications WHERE id = %s AND user_id = %s", (notification_id, user_id))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to delete notification: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def save_notification(user_id, title, message, icon='notifications', type='info'):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO notifications (user_id, title, message, icon, type)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, title, message, icon, type))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save notification: {e}")
    finally:
        cur.close()
        conn.close()

def get_user_notifications(user_id, limit=50):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, title, message, icon, type, created_at, is_read
            FROM notifications
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (user_id, limit))
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "title": r[1],
                "message": r[2],
                "icon": r[3],
                "type": r[4],
                "time": r[5].strftime("%I:%M %p") if r[5] else "Just now",
                "time_ago": time_ago(r[5]) if r[5] else "Just now",
                "is_read": r[6]
            } for r in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching notifications: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def mark_notifications_read(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE notifications SET is_read = TRUE WHERE user_id = %s AND is_read = FALSE", (user_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_unread_count(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM notifications WHERE user_id = %s AND is_read = FALSE", (user_id,))
        return cur.fetchone()[0]
    except:
        return 0
    finally:
        cur.close()
        conn.close()

def time_ago(dt):
    if not dt:
        return "Just now"
    now = datetime.now()
    diff = now - dt
    if diff.total_seconds() < 60:
        return "Just now"
    elif diff.total_seconds() < 3600:
        mins = int(diff.total_seconds() // 60)
        return f"{mins} minute{'s' if mins != 1 else ''} ago"
    elif diff.total_seconds() < 86400:
        hours = int(diff.total_seconds() // 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    else:
        days = diff.days
        return f"{days} day{'s' if days != 1 else ''} ago"
    
def _parse_details(val):
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return val
    return val

def _to_iso(dt):
    if isinstance(dt, (datetime, date)):
        return dt.isoformat()
    return dt

def _row_to_json_safe(row):
    if not row:
        return None
    r = list(row)
    if len(r) > 4:
        r[4] = _parse_details(r[4])
    if len(r) > 7:
        r[7] = _to_iso(r[7])
    return r

def get_requests_json():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
        return [_row_to_json_safe(r) for r in rows]
    except Exception as e:
        logger.error(f"get_requests_json error: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def get_last_request_json():
    if not current_user.is_authenticated:
        return None
    user_id = current_user.get_id()
    if not user_id:
        return None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests
            WHERE user_id = %s
            ORDER BY id DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        return _row_to_json_safe(row) if row else None
    except Exception as e:
        logger.error(f"get_last_request_json error: {e}")
        return None
    finally:
        cur.close()
        conn.close()

# ---------------------- Live Updates ----------------------
def get_active_users():
    """Get list of active user IDs (users with activity in last 24 hours)"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT user_id 
            FROM requests 
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """)
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting active users: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def get_analytics_data():
    """Get comprehensive analytics data"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM users WHERE DATE(created_at) = CURRENT_DATE")
        new_users_today = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM requests")
        total_requests = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM requests WHERE payment_status = 'Pending'")
        pending_requests = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM requests WHERE admin_confirmation = 'Confirmed'")
        confirmed_requests = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM requests WHERE DATE(created_at) = CURRENT_DATE")
        active_requests_today = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM requests WHERE DATE(created_at) = CURRENT_DATE")
        active_users_today = cur.fetchone()[0]
        
        cur.execute("""
            SELECT service_type, COUNT(*) 
            FROM requests 
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY service_type
        """)
        service_data = cur.fetchall()
        
        timeline_labels = []
        timeline_data = []
        for i in range(6, -1, -1):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            cur.execute("SELECT COUNT(*) FROM requests WHERE DATE(created_at) = %s", (date,))
            count = cur.fetchone()[0]
            timeline_labels.append(date)
            timeline_data.append(count)
        
        return {
            "total_users": total_users,
            "new_users_today": new_users_today,
            "active_users_today": active_users_today,
            "total_requests": total_requests,
            "pending_requests": pending_requests,
            "confirmed_requests": confirmed_requests,
            "active_requests_today": active_requests_today,
            "service_distribution": {
                "labels": [item[0] for item in service_data],
                "data": [item[1] for item in service_data]
            },
            "timeline": {
                "labels": timeline_labels,
                "data": timeline_data
            }
        }
    except Exception as e:
        logger.error(f"Error getting analytics data: {e}")
        return {}
    finally:
        cur.close()
        conn.close()

def schedule_live_updates():
    """Start background thread for live data updates"""
    def send_live_update():
        while True:
            try:
                with app.app_context():
                    analytics_data = get_analytics_data()
                    socketio.emit('analytics_update', {
                        'analytics': analytics_data,
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    active_users = get_active_users()
                    socketio.emit('user_activity', {
                        'active_users': active_users,
                        'active_count': len(active_users),
                        'timestamp': datetime.now().isoformat()
                    })
            except Exception as e:
                logger.error(f"Error in live update: {e}")
            time.sleep(10)
    
    thread = threading.Thread(target=send_live_update, daemon=True)
    thread.start()
    logger.info("Live updates scheduler started")

# ---------------------- App Startup ----------------------
app_started = False

@app.before_request
def initialize_app():
    global app_started
    if not app_started:
        schedule_live_updates()
        app_started = True

# ---------------------- Routes ----------------------
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        full_name = request.form['fullname']
        email = request.form['email']
        username = request.form['username']
        password = request.form['password']
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO users (full_name, email, username, password) VALUES (%s, %s, %s, %s)",
                (full_name, email, username, password)
            )
            conn.commit()
            flash("Account created successfully! Please login.")
            return redirect('/login')
        except Exception as e:
            conn.rollback()
            flash(f"Error: {str(e)}")
        finally:
            cur.close()
            conn.close()
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username == 'admin' and password == 'password':
            session['is_admin'] = True
            flash('Admin login successful!')
            return redirect('/admin')

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id, username FROM users WHERE username = %s AND password = %s",
                (username, password)
            )
            user_data = cur.fetchone()
            if user_data:
                user = User()
                user.id = str(user_data[0])
                login_user(user)
                session['user_id'] = user_data[0]
                session['username'] = username
                return redirect('/dashboard')
            else:
                flash('Invalid username or password.')
        except Exception as e:
            flash(f"Login error: {e}")
        finally:
            cur.close()
            conn.close()
    return render_template('login.html')

@app.route('/admin')
def admin():
    if not session.get('is_admin'):
        flash("Access denied. Admin only.", "danger")
        return redirect(url_for('login'))
    
    analytics_data = get_analytics_data()
    active_users = get_active_users()
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, full_name, email, username FROM users")
        users = cur.fetchall()
        cur.execute("""
            SELECT id, user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests
            ORDER BY created_at DESC
        """)
        requests = cur.fetchall()
    except psycopg2.Error as e:
        requests = []
        users = []
        flash(f"DB Error: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    
    return render_template('admin.html', 
                         users=users, 
                         requests=requests,
                         analytics=analytics_data,
                         active_users_count=len(active_users))

# ---------------------- Socket.IO ----------------------
@socketio.on('connect')
def handle_connect(auth):
    try:
        if session.get('is_admin'):
            emit('update_requests', {'requests': get_requests_json()}, broadcast=True)
            analytics_data = get_analytics_data()
            emit('analytics_update', {'analytics': analytics_data})
            
            active_users = get_active_users()
            emit('user_activity', {
                'active_users': active_users,
                'active_count': len(active_users)
            })
    except Exception as e:
        logger.error(f"connect error: {e}")

@socketio.on('user_connect')
def handle_user_connect(data):
    """Handle user connection for real-time updates"""
    user_id = current_user.get_id()
    if user_id:
        join_room(f"user_{user_id}")
        emit('user_connected', {'user_id': user_id, 'status': 'connected'})

@socketio.on('approve_request')
def handle_approve_request(data):
    request_id = data.get('request_id')
    if not request_id:
        emit('error', {'message': 'request_id is required'})
        return

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE requests 
            SET admin_confirmation = 'Confirmed' 
            WHERE id = %s 
            RETURNING booking_id, service_type, user_id
        """, (request_id,))
        
        result = cur.fetchone()
        if not result:
            emit('error', {'message': 'Request not found'})
            return
            
        booking_id, service_type, user_id = result
        conn.commit()

        save_notification(
            user_id=user_id,
            title=f"{service_type} Approved!",
            message=f"Your booking {booking_id} has been approved by admin. PDF ticket will be generated separately.",
            icon="check_circle",
            type="success"
        )

        socketio.emit('request_approved', {
            'request_id': request_id,
            'booking_id': booking_id,
            'service_type': service_type,
            'message': f'Your {service_type} has been approved!'
        }, room=f"user_{user_id}")

        emit('update_requests', {'requests': get_requests_json()}, broadcast=True)
        
        emit('approve_success', {
            'request_id': request_id,
            'message': 'Request approved successfully!'
        })

    except Exception as e:
        conn.rollback()
        emit('error', {'message': str(e)})
    finally:
        cur.close()
        conn.close()

@socketio.on('join')
def handle_join(data):
    user_id = data.get('user_id')
    if user_id:
        join_room(f"user_{user_id}")

@socketio.on('send_ticket')
def handle_send_ticket(data):
    """Admin manually sends ticket to user"""
    request_id = data.get('request_id')
    if not request_id:
        emit('error', {'message': 'request_id is required'})
        return

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT booking_id, service_type, user_id, details FROM requests WHERE id = %s", (request_id,))
        row = cur.fetchone()
        if not row:
            emit('error', {'message': 'Request not found'})
            return

        booking_id, service_type, user_id, details = row

        try:
            details_obj = json.loads(details) if isinstance(details, str) and details else (details or {})
        except Exception:
            details_obj = {"raw": str(details)}

        pdf_filename = create_pdf_ticket_for_booking(booking_id, service_type, details_obj, user_id)
        details_obj["ticket_pdf_url"] = f"/static/tickets/{pdf_filename}"
        cur.execute("UPDATE requests SET details = %s::jsonb WHERE id = %s",
                    (json.dumps(details_obj), request_id))

        conn.commit()

        emit('ticket_received', {
            'booking_id': booking_id,
            'service_type': service_type,
            'ticket_pdf_url': f"/static/tickets/{pdf_filename}",
            'message': 'Your PDF ticket has been generated! Download it now.'
        }, room=f"user_{user_id}")

        emit('ticket_sent', {
            'request_id': request_id,
            'message': f'PDF ticket sent to user {user_id}'
        })

    except Exception as e:
        conn.rollback()
        emit('error', {'message': str(e)})
    finally:
        cur.close()
        conn.close()

@socketio.on('confirm_payment')
def handle_confirm_payment(data):
    request_id = data.get('request_id')
    if not request_id:
        emit('error', {'message': 'request_id is required'})
        return

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE requests SET payment_status = 'Confirmed' WHERE id = %s", (request_id,))
        conn.commit()
        emit('update_requests', {'requests': get_requests_json()}, broadcast=True)
    except Exception as e:
        emit('error', {'message': str(e)})
    finally:
        cur.close()
        conn.close()

@socketio.on('delete_request')
def handle_delete_request(data):
    request_id = data.get('request_id')
    if not request_id:
        emit('delete_error', {'message': 'request_id is required'})
        return

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT user_id, booking_id, service_type 
            FROM requests 
            WHERE id = %s
        """, (request_id,))
        request_data = cur.fetchone()
        
        if not request_data:
            emit('delete_error', {'message': 'Request not found'})
            return

        user_id, booking_id, service_type = request_data
        
        try:
            ticket_file = TICKETS_DIR / f"ticket_{booking_id}.pdf"
            if ticket_file.exists():
                ticket_file.unlink()
        except Exception as e:
            logger.warning(f"Could not delete ticket file: {e}")

        cur.execute("DELETE FROM requests WHERE id = %s", (request_id,))
        conn.commit()

        save_notification(
            user_id=user_id,
            title=f"{service_type} Cancelled",
            message=f"Your booking {booking_id} has been cancelled by admin.",
            icon="cancel",
            type="error"
        )

        emit('request_deleted', {
            'request_id': request_id,
            'booking_id': booking_id,
            'user_id': user_id,
            'message': f'Request #{request_id} deleted successfully'
        }, broadcast=True)
        
        emit('update_requests', {'requests': get_requests_json()}, broadcast=True)

    except Exception as e:
        conn.rollback()
        emit('delete_error', {'message': str(e)})
        logger.error(f"Error deleting request {request_id}: {e}")
    finally:
        cur.close()
        conn.close()

# ---------------------- Admin API Routes ----------------------
@app.route('/admin/users')
def admin_users():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, full_name, email, username, 
                   COALESCE(phone, 'Not provided') as phone,
                   COALESCE(address, 'Not provided') as address,
                   COALESCE(whatsapp, 'Not provided') as whatsapp,
                   COALESCE(instagram, 'Not provided') as instagram,
                   COALESCE(facebook, 'Not provided') as facebook,
                   created_at
            FROM users 
            ORDER BY created_at DESC
        """)
        users = cur.fetchall()
        
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM users WHERE DATE(created_at) = CURRENT_DATE")
        new_users_today = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT user_id) 
            FROM requests 
            WHERE DATE(created_at) = CURRENT_DATE
        """)
        active_users_today = cur.fetchone()[0]
        
        return jsonify({
            "users": [
                {
                    "id": user[0],
                    "full_name": user[1],
                    "email": user[2],
                    "username": user[3],
                    "phone": user[4],
                    "address": user[5],
                    "whatsapp": user[6],
                    "instagram": user[7],
                    "facebook": user[8],
                    "registration_date": user[9].strftime('%Y-%m-%d %H:%M:%S') if user[9] else 'N/A'
                } for user in users
            ],
            "stats": {
                "total_users": total_users,
                "new_users_today": new_users_today,
                "active_users_today": active_users_today
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/admin/user/<int:user_id>')
def admin_user_details(user_id):
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, full_name, email, username, phone, address, whatsapp, instagram, facebook, created_at
            FROM users WHERE id = %s
        """, (user_id,))
        user = cur.fetchone()
        
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        cur.execute("""
            SELECT 
                COUNT(*) as total_requests,
                COUNT(CASE WHEN payment_status = 'Confirmed' THEN 1 END) as confirmed_requests,
                COUNT(CASE WHEN payment_status = 'Pending' THEN 1 END) as pending_requests,
                MAX(created_at) as last_activity
            FROM requests WHERE user_id = %s
        """, (user_id,))
        
        stats = cur.fetchone()
        
        return jsonify({
            "id": user[0],
            "full_name": user[1],
            "email": user[2],
            "username": user[3],
            "phone": user[4],
            "address": user[5],
            "whatsapp": user[6],
            "instagram": user[7],
            "facebook": user[8],
            "registration_date": user[9].strftime('%Y-%m-%d %H:%M:%S') if user[9] else 'N/A',
            "total_requests": stats[0] if stats else 0,
            "confirmed_requests": stats[1] if stats else 0,
            "pending_requests": stats[2] if stats else 0,
            "last_active": stats[3].strftime('%Y-%m-%d %H:%M:%S') if stats and stats[3] else 'Never'
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/admin/user/<int:user_id>/request-count')
def admin_user_request_count(user_id):
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM requests WHERE user_id = %s", (user_id,))
        count = cur.fetchone()[0]
        return jsonify({"count": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/admin/send-ticket', methods=['POST'])
def admin_send_ticket():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    data = request.get_json()
    request_id = data.get('request_id')
    user_id = data.get('user_id')
    
    if not request_id or not user_id:
        return jsonify({"error": "Missing parameters"}), 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT booking_id, service_type, details FROM requests WHERE id = %s", (request_id,))
        request_data = cur.fetchone()
        
        if not request_data:
            return jsonify({"error": "Request not found"}), 404
        
        booking_id, service_type, details = request_data
        
        try:
            details_obj = json.loads(details) if isinstance(details, str) else details
        except:
            details_obj = {"raw": str(details)}
        
        pdf_filename = create_pdf_ticket_for_booking(booking_id, service_type, details_obj, user_id)
        
        socketio.emit('ticket_ready', {
            'booking_id': booking_id,
            'service_type': service_type,
            'ticket_pdf_url': f"/static/tickets/{pdf_filename}",
            'message': 'Your PDF ticket has been generated!'
        }, room=f"user_{user_id}")
        
        return jsonify({"success": True, "message": "PDF ticket sent to user"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@socketio.on('send_ticket_to_user')
def handle_send_ticket_to_user(data):
    request_id = data.get('request_id')
    user_id = data.get('user_id')
    
    if not request_id or not user_id:
        emit('error', {'message': 'Missing parameters'})
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT booking_id, service_type, details FROM requests WHERE id = %s", (request_id,))
        request_data = cur.fetchone()
        
        if not request_data:
            emit('error', {'message': 'Request not found'})
            return
        
        booking_id, service_type, details = request_data
        
        try:
            details_obj = json.loads(details) if isinstance(details, str) else details
        except:
            details_obj = {"raw": str(details)}
        
        pdf_filename = create_pdf_ticket_for_booking(booking_id, service_type, details_obj, user_id)
        
        emit('ticket_sent_to_user', {
            'user_id': user_id,
            'booking_id': booking_id,
            'ticket_pdf_url': f"/static/tickets/{pdf_filename}"
        }, broadcast=True)
        
        emit('ticket_ready', {
            'booking_id': booking_id,
            'service_type': service_type,
            'ticket_pdf_url': f"/static/tickets/{pdf_filename}",
            'message': 'Your PDF ticket has been generated! Click to download.'
        }, room=f"user_{user_id}")
        
    except Exception as e:
        emit('error', {'message': str(e)})
    finally:
        cur.close()
        conn.close()

@socketio.on('send_report_to_user')
def handle_send_report_to_user(data):
    user_id = data.get('user_id')
    
    if not user_id:
        emit('error', {'message': 'Missing user_id'})
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT full_name, email FROM users WHERE id = %s", (user_id,))
        user_data = cur.fetchone()
        
        if not user_data:
            emit('error', {'message': 'User not found'})
            return
        
        full_name, email = user_data
        
        cur.execute("""
            SELECT 
                COUNT(*) as total_requests,
                COUNT(CASE WHEN payment_status = 'Confirmed' THEN 1 END) as confirmed_requests,
                COUNT(CASE WHEN payment_status = 'Pending' THEN 1 END) as pending_requests
            FROM requests WHERE user_id = %s
        """, (user_id,))
        stats = cur.fetchone()
        
        emit('report_sent', {
            'user_id': user_id,
            'message': f'Your activity report has been generated and sent to {email}',
            'stats': {
                'total_requests': stats[0] if stats else 0,
                'confirmed_requests': stats[1] if stats else 0,
                'pending_requests': stats[2] if stats else 0
            }
        }, room=f"user_{user_id}")
        
    except Exception as e:
        emit('error', {'message': str(e)})
    finally:
        cur.close()
        conn.close()

@app.route('/admin/requests')
def admin_requests():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests
            ORDER BY created_at DESC
        """)
        requests = cur.fetchall()
        
        cur.execute("SELECT COUNT(*) FROM requests")
        total_requests = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM requests WHERE payment_status = 'Pending'")
        pending_requests = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM requests WHERE admin_confirmation = 'Confirmed'")
        confirmed_requests = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM requests WHERE DATE(created_at) = CURRENT_DATE")
        active_requests_today = cur.fetchone()[0]
        
        return jsonify({
            "requests": [
                {
                    "id": req[0],
                    "user_id": req[1],
                    "booking_id": req[2],
                    "service_type": req[3],
                    "details": req[4],
                    "payment_status": req[5],
                    "admin_confirmation": req[6],
                    "created_at": req[7].strftime('%Y-%m-%d %H:%M:%S') if req[7] else 'N/A'
                } for req in requests
            ],
            "stats": {
                "total_requests": total_requests,
                "pending_requests": pending_requests,
                "confirmed_requests": confirmed_requests,
                "active_requests_today": active_requests_today
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/admin/request/<int:request_id>')
def admin_request_details(request_id):
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests WHERE id = %s
        """, (request_id,))
        request_data = cur.fetchone()
        
        if not request_data:
            return jsonify({"error": "Request not found"}), 404
        
        return jsonify({
            "id": request_data[0],
            "user_id": request_data[1],
            "booking_id": request_data[2],
            "service_type": request_data[3],
            "details": request_data[4],
            "payment_status": request_data[5],
            "admin_confirmation": request_data[6],
            "created_at": request_data[7].strftime('%Y-%m-%d %H:%M:%S') if request_data[7] else 'N/A'
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/admin/ticket-requests')
def admin_ticket_requests():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    service_type = request.args.get('service_type', 'all')
    status = request.args.get('status', 'confirmed')
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        query = """
            SELECT id, user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests WHERE 1=1
        """
        params = []
        
        if service_type != 'all':
            query += " AND service_type = %s"
            params.append(service_type)
        
        if status != 'all':
            if status == 'confirmed':
                query += " AND (payment_status = 'Confirmed' OR admin_confirmation = 'Confirmed')"
            elif status == 'pending':
                query += " AND payment_status = 'Pending'"
        
        query += " ORDER BY created_at DESC"
        
        cur.execute(query, params)
        requests = cur.fetchall()
        
        return jsonify({
            "requests": [
                {
                    "id": req[0],
                    "user_id": req[1],
                    "booking_id": req[2],
                    "service_type": req[3],
                    "details": req[4],
                    "payment_status": req[5],
                    "admin_confirmation": req[6],
                    "created_at": req[7].strftime('%Y-%m-d %H:%M:%S') if req[7] else 'N/A'
                } for req in requests
            ]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# ---------------------- User Requests API ----------------------
@app.route('/user/requests')
@login_required
def user_requests():
    """Get all requests for the current user"""
    user_id = current_user.get_id()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests 
            WHERE user_id = %s 
            ORDER BY created_at DESC
        """, (user_id,))
        requests = cur.fetchall()
        
        formatted_requests = []
        for req in requests:
            try:
                details = json.loads(req[3]) if isinstance(req[3], str) else req[3]
            except:
                details = {"raw": str(req[3])}
                
            formatted_requests.append({
                "id": req[0],
                "booking_id": req[1],
                "service_type": req[2],
                "details": details,
                "payment_status": req[4],
                "admin_confirmation": req[5],
                "created_at": req[6].strftime('%Y-%m-%d %H:%M:%S') if req[6] else 'N/A'
            })
        
        return jsonify({"requests": formatted_requests})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/user/request/<int:request_id>')
@login_required
def user_request_details(request_id):
    """Get detailed information for a specific request"""
    user_id = current_user.get_id()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests 
            WHERE id = %s AND user_id = %s
        """, (request_id, user_id))
        request_data = cur.fetchone()
        
        if not request_data:
            return jsonify({"error": "Request not found"}), 404
            
        try:
            details = json.loads(request_data[3]) if isinstance(request_data[3], str) else request_data[3]
        except:
            details = {"raw": str(request_data[3])}
            
        return jsonify({
            "id": request_data[0],
            "booking_id": request_data[1],
            "service_type": request_data[2],
            "details": details,
            "payment_status": request_data[4],
            "admin_confirmation": request_data[5],
            "created_at": request_data[6].strftime('%Y-%m-%d %H:%M:%S') if request_data[6] else 'N/A'
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@socketio.on('get_live_data')
def handle_get_live_data():
    if not session.get('is_admin'):
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT user_id 
            FROM requests 
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """)
        active_users = [row[0] for row in cur.fetchall()]
        
        emit('live_data_update', {
            'active_users': active_users,
            'active_count': len(active_users),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting live data: {e}")
    finally:
        cur.close()
        conn.close()
    
@socketio.on('send_broadcast')
def handle_send_broadcast(data):
    """Handle broadcast notifications from admin"""
    try:
        target = data.get('target')
        user_id = data.get('user_id')
        title = data.get('title', 'Notification')
        message = data.get('message', '')
        icon = data.get('icon', 'notifications_active')
        notification_type = data.get('type', 'info')
        
        if not title or not message:
            emit('broadcast_error', {'message': 'Title and message are required'})
            return
            
        if target == 'specific':
            if not user_id:
                emit('broadcast_error', {'message': 'User ID is required for specific user'})
                return
            
            save_notification(user_id, title, message, icon, notification_type)
            
            emit('broadcast_notification', {
                'title': title,
                'message': message,
                'icon': icon,
                'type': notification_type,
                'timestamp': datetime.now().isoformat()
            }, room=f"user_{user_id}")
            
        else:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute("SELECT id FROM users")
                all_users = cur.fetchall()
                
                for user in all_users:
                    user_id = user[0]
                    save_notification(user_id, title, message, icon, notification_type)
                    
                    emit('broadcast_notification', {
                        'title': title,
                        'message': message,
                        'icon': icon,
                        'type': notification_type,
                        'timestamp': datetime.now().isoformat()
                    }, room=f"user_{user_id}")
                    
            finally:
                cur.close()
                conn.close()
        
        emit('broadcast_success', {'message': 'Notification sent successfully'})
        
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        emit('broadcast_error', {'message': str(e)})

@socketio.on('delete_notification')
def handle_delete_notification(data):
    """Handle deletion of a specific notification"""
    try:
        notification_id = data.get('notification_id')
        user_id = data.get('user_id')
        
        if not notification_id or not user_id:
            emit('notification_error', {'message': 'Missing parameters'})
            return
        
        if delete_notification(notification_id, user_id):
            emit('notification_deleted', {
                'notification_id': notification_id,
                'message': 'Notification deleted successfully'
            }, room=f"user_{user_id}")
        else:
            emit('notification_error', {'message': 'Failed to delete notification'})
            
    except Exception as e:
        logger.error(f"Error deleting notification: {e}")
        emit('notification_error', {'message': str(e)})

@socketio.on('mark_all_read')
def handle_mark_all_read(data):
    """Mark all notifications as read for a user"""
    try:
        user_id = data.get('user_id')
        if not user_id:
            emit('notification_error', {'message': 'User ID is required'})
            return
        
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE notifications 
                SET is_read = TRUE 
                WHERE user_id = %s AND is_read = FALSE
            """, (user_id,))
            conn.commit()
            
            emit('all_notifications_read', {
                'user_id': user_id,
                'message': 'All notifications marked as read'
            }, room=f"user_{user_id}")
            
        except Exception as e:
            conn.rollback()
            emit('notification_error', {'message': str(e)})
        finally:
            cur.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error marking all as read: {e}")
        emit('notification_error', {'message': str(e)})
        
# ---------------------- Simulated PAYMENT (HOTEL) ----------------------
@app.route('/confirm-booking', methods=['POST'])
@login_required
def confirm_booking():
    data = request.get_json(silent=True) or {}
    hotel_name = data.get('hotel') or 'N/A'
    amount = data.get('amount') or 0
    rooms = data.get('rooms') or 1
    guests = data.get('guests') or 1
    checkin = data.get('checkin')
    checkout = data.get('checkout')
    email = data.get('email')
    mobile = data.get('mobile')

    booking_id = f"HOTEL-{random.randint(1000, 9999)}"
    details_obj = {
        "hotel_name": hotel_name,
        "total_amount": amount,
        "rooms": rooms,
        "guests": guests,
        "checkin": checkin,
        "checkout": checkout,
        "email": email,
        "mobile": mobile,
        "simulated_payment": True,
        "simulated_payment_at": datetime.now().isoformat()
    }

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            RETURNING id
        """, (
            current_user.get_id(),
            booking_id,
            'Hotel Booking',
            json.dumps(details_obj),
            'Confirmed',
            'Pending'
        ))
        new_id = cur.fetchone()[0]
        conn.commit()

        last_row = get_last_request_json()
        if last_row:
            socketio.emit('new_request', {'request': last_row}, to=None)

        return jsonify({"success": True, "booking_id": booking_id, "request_id": new_id})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# ---------------------- Simulated PAYMENT (CAR) ----------------------
@app.route('/confirm-car-booking', methods=['POST'])
@login_required
def confirm_car_booking():
    data = request.get_json(silent=True) or {}
    
    car_model = data.get('car_model', 'N/A')
    total_price = data.get('total_price', 0)
    pickup_date = data.get('pickup_date')
    pickup_time = data.get('pickup_time')
    email = data.get('email')
    mobile = data.get('mobile')
    booking_id = data.get('booking_id', f"CAR-{random.randint(1000, 9999)}")

    if not all([car_model, total_price, pickup_date, email, mobile]):
        return jsonify({"success": False, "error": "Missing essential booking data."}), 400

    details_obj = {
        "car_model": car_model,
        "cab_class": data.get('cab_class'),
        "base_price_per_day": data.get('base_price_per_day'),
        "total_price": total_price,
        "passengers": data.get('passengers'),
        "pickup": data.get('pickup'),
        "dropoff": data.get('dropoff'),
        "pickup_date": pickup_date,
        "pickup_time": pickup_time,
        "return_date": data.get('return_date'),
        "return_time": data.get('return_time'),
        "email": email,
        "mobile": mobile,
        "passengers_details": data.get('passengers_details', []),
        "special_instructions": data.get('special_instructions', ''),
        "options": data.get('options', {}),
        "booking_source": "Car Booking Result Page"
    }

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            RETURNING id
        """, (
            current_user.get_id(),
            booking_id,
            'Car Booking',
            json.dumps(details_obj),
            'Confirmed',
            'Pending'
        ))
        new_id = cur.fetchone()[0]
        conn.commit()

        last_row = get_last_request_json()
        if last_row:
            socketio.emit('new_request', {'request': last_row}, to=None)

        return jsonify({"success": True, "booking_id": booking_id, "request_id": new_id})
    except Exception as e:
        conn.rollback()
        logger.error(f"Car booking confirmation error: {e}")
        return jsonify({"success": False, "error": f"Database error saving booking: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()


# --------------------------------------------------------------
#  Car Booking Routes
# --------------------------------------------------------------
@app.route('/submit_car_booking', methods=['POST'])
@login_required
def submit_car_booking():
    try:
        pickup = request.form.get('pickup', '').strip().title()
        dropoff = request.form.get('dropoff', '').strip().title()
        pickup_date = request.form.get('pickup_date', '').strip()
        pickup_time = request.form.get('pickup_time', '').strip()
        passengers = request.form.get('passengers', '1').strip()
        car_class = request.form.get('cab_class', 'standard').strip().lower()
        special_requests = request.form.get('special_requests', '').strip()
        is_initial = request.form.get('is_initial', 'false') == 'true'

        if not all([pickup, dropoff, pickup_date, pickup_time]):
            flash("Please fill all required fields.", "danger")
            return redirect(url_for('dashboard'))

        if len(pickup_time.split(':')) == 3:
            time_format = '%H:%M:%S'
        else:
            time_format = '%H:%M'

        try:
            pickup_dt = datetime.strptime(f"{pickup_date} {pickup_time}", f'%Y-%m-%d {time_format}')
            if pickup_dt < datetime.now():
                flash("Pickup time cannot be in the past.", "danger")
                return redirect(url_for('dashboard'))
        except ValueError as e:
            print("Date/time parse error:", e)
            flash("Invalid date or time format.", "danger")
            return redirect(url_for('dashboard'))

        try:
            passengers = int(passengers)
            if not 1 <= passengers <= 9:
                raise ValueError
        except:
            flash("Passengers must be 1–9.", "danger")
            return redirect(url_for('dashboard'))

        class_map = {
            'economy': 'Standard',
            'standard': 'Standard', 
            'luxury': 'Luxury',
            'suv': 'SUV'
        }
        target_class = class_map.get(car_class, 'Standard')

        filtered_cars = []
        for car in cars_data:
            cab_class = car.get('cab_class', '').lower()
            if target_class.lower() == 'standard' and cab_class == 'standard':
                if car.get('seats', 4) >= passengers:
                    filtered_cars.append(car)
            elif cab_class == target_class.lower():
                if car.get('seats', 4) >= passengers:
                    filtered_cars.append(car)

        if not filtered_cars:
            filtered_cars = [car for car in cars_data 
                           if car.get('cab_class', '').lower() == target_class.lower()]
            
            if not filtered_cars:
                flash(f"No {target_class} cars available right now – showing popular options!", "info")
                filtered_cars = [c for c in cars_data if c.get('cab_class') in ['Standard', 'SUV', 'Luxury']]

        selected = filtered_cars[:6]
        if len(filtered_cars) > 6:
            selected = random.sample(filtered_cars, 6)

        enhanced_cars = []
        for idx, car in enumerate(selected):
            enhanced_car = car.copy()
            
            if car.get('cab_class') == 'Luxury':
                transmission = 'Automatic'
            elif car.get('model', '').lower().__contains__('premium'):
                transmission = 'Automatic'
            else:
                transmission = 'Manual'
            
            enhanced_car.update({
                'id': idx + 1,
                'transmission': transmission,
                'pickup': pickup,
                'dropoff': dropoff,
                'pickup_date': pickup_date,
                'pickup_time': pickup_time.split(':')[0] + ':' + pickup_time.split(':')[1],
                'passengers': passengers,
                'booking_for': pickup_dt.strftime('%b %d, %Y at %I:%M %p')
            })
            enhanced_cars.append(enhanced_car)

        return render_template(
            'car_results.html',
            cars=enhanced_cars,
            pickup=pickup,
            dropoff=dropoff,
            pickup_date=pickup_date,
            pickup_time=pickup_time.split(':')[0] + ':' + pickup_time.split(':')[1],
            passengers=passengers,
            car_class=target_class
        )

    except Exception as e:
        import traceback
        print("FATAL ERROR in submit_car_booking:")
        traceback.print_exc()
        flash("Something went wrong. Please try again.", "danger")
        return redirect(url_for('dashboard'))

# ---------------------- Technician / Courier / Travel ----------------------
@app.route('/submit-technician-booking', methods=['POST'])
@login_required
def submit_technician_booking():
    service_type = (request.form.get('service_type') or '').strip().lower()
    location = (request.form.get('location') or '').strip()
    service_date = request.form.get('service_date')
    service_time = request.form.get('service_time')
    urgency = request.form.get('urgency', 'normal')
    description = request.form.get('description', '').strip()
    is_initial = request.form.get('is_initial', 'false') == 'true'

    if not all([service_type, location, service_date, service_time, description]):
        flash("Please provide valid technician booking details.", "danger")
        return redirect(url_for('dashboard'))

    try:
        service_datetime = datetime.strptime(f"{service_date} {service_time}", '%Y-%m-%d %H:%M')
        if service_datetime < datetime.now():
            flash("Service time cannot be in the past.", "danger")
            return redirect(url_for('dashboard'))
    except ValueError:
        flash("Invalid date or time format. Use YYYY-MM-DD and HH:MM.", "danger")
        return redirect(url_for('dashboard'))

    normalized_location = location.strip()
    if normalized_location:
        normalized_location = normalized_location.title()

    filtered_technicians = [
        tech for tech in technicians_data
        if tech.get('service_type', '').lower() == service_type and tech.get('location', '').lower() == normalized_location.lower()
    ]

    fallback_reason = None
    if not filtered_technicians:
        fallback_reason = "no_exact_location"
        filtered_technicians = [
            tech for tech in technicians_data
            if tech.get('service_type', '').lower() == service_type
        ]

    if not filtered_technicians:
        fallback_reason = "no_service_type"
        sorted_by_rating = sorted(technicians_data, key=lambda t: t.get('rating', 0), reverse=True)
        filtered_technicians = sorted_by_rating[:6]

    num_to_show = min(max(5, len(filtered_technicians)), len(filtered_technicians))
    if len(filtered_technicians) > num_to_show:
        selected_technicians = random.sample(filtered_technicians, num_to_show)
    else:
        selected_technicians = filtered_technicians

    for tech in selected_technicians:
        tech['service_time'] = service_time
        tech['location'] = normalized_location

    if is_initial:
        booking_id = f"TECH-{random.randint(1000, 9999)}"
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            payload = json.dumps({
                'service_type': service_type,
                'location': normalized_location,
                'service_date': service_date,
                'service_time': service_time,
                'urgency': urgency,
                'description': description
            })
            cur.execute("""
                INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at)
                VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s)
            """, (
                current_user.get_id(),
                booking_id,
                'Technician Booking',
                payload,
                'Pending',
                'Pending',
                datetime.now()
            ))
            conn.commit()
            socketio.emit('new_request', {'request': get_last_request_json()})
            flash("Technician booking request submitted successfully!", "success")
        except psycopg2.Error as e:
            conn.rollback()
            flash(f"Database error: {str(e)}. Please ensure the requests table exists.", "danger")
        finally:
            cur.close()
            conn.close()
    else:
        if fallback_reason == "no_exact_location":
            flash("No technicians found in that exact location. Showing technicians for the selected service type.", "info")
        elif fallback_reason == "no_service_type":
            flash("No technicians found for that service type. Showing top available technicians.", "info")
        else:
            flash("Search modified! Showing updated results.", "info")

    return render_template('technician_results.html',
                           technicians=selected_technicians,
                           service_type=service_type,
                           location=normalized_location,
                           service_date=service_date,
                           service_time=service_time,
                           urgency=urgency,
                           description=description)

@app.route('/technician/confirm', methods=['POST'])
@login_required
def confirm_technician():
    data = request.get_json(silent=True) or {}

    booking_id = f"TECH-{random.randint(1000, 9999)}"
    payload = json.dumps({
        "technician_id": data.get("technician_id"),
        "name": data.get("name"),
        "service_type": data.get("service_type"),
        "location": data.get("location"),
        "service_date": data.get("service_date"),
        "service_time": data.get("service_time"),
        "description": data.get("description"),
        "total_price": data.get("total_price"),
        "email": data.get("email"),
        "mobile": data.get("mobile")
    })

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s)
        """, (
            current_user.get_id(),
            booking_id,
            'Technician Booking',
            payload,
            'Confirmed',
            'Pending',
            datetime.now()
        ))
        conn.commit()

        last_row = get_last_request_json()
        if last_row:
            try:
                socketio.emit('new_request', {'request': last_row})
            except Exception as e:
                app.logger.exception("socketio.emit new_request failed: %s", e)

            try:
                socketio.emit('payment_confirmed', {
                    "request_id": last_row[0],
                    "booking_id": booking_id
                })
            except Exception as e:
                app.logger.exception("socketio.emit payment_confirmed failed: %s", e)

        return jsonify({"success": True, "booking_id": booking_id})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/submit_courier_booking', methods=['POST'])
@login_required
def submit_courier_booking():
    pickup          = request.form.get('pickup', '').strip().title()
    dropoff         = request.form.get('dropoff', '').strip().title()
    pickup_date     = request.form.get('pickup_date', '').strip()
    pickup_time     = request.form.get('pickup_time', '').strip()
    package_weight  = request.form.get('package_weight', '1.0').strip()
    courier_type    = request.form.get('courier_type', 'standard').strip().lower()
    special_requests = request.form.get('special_requests', '').strip()
    is_initial      = request.form.get('is_initial', 'false') == 'true'

    try:
        weight = float(package_weight)
        if weight < 0.1:
            raise ValueError()
    except ValueError:
        flash("Package weight must be at least 0.1 kg.", "danger")
        return redirect(url_for('dashboard'))

    if not all([pickup, dropoff, pickup_date, pickup_time]):
        flash("Please fill all required fields.", "danger")
        return redirect(url_for('dashboard'))

    try:
        pickup_dt = datetime.strptime(f"{pickup_date} {pickup_time}", '%Y-%m-%d %H:%M')
        if pickup_dt < datetime.now():
            flash("Pickup time cannot be in the past.", "danger")
            return redirect(url_for('dashboard'))
    except ValueError:
        flash("Invalid date or time format.", "danger")
        return redirect(url_for('dashboard'))

    services = {
        "standard":   {"price_per_kg": 50,  "delivery_time": "2–3 days"},
        "express":    {"price_per_kg": 100, "delivery_time": "Same day"},
        "overnight":  {"price_per_kg": 200, "delivery_time": "Next day"}
    }
    svc = services.get(courier_type, services["standard"])
    price_per_kg = svc["price_per_kg"]
    delivery_time = svc["delivery_time"]

    if is_initial:
        booking_id = f"COURIER-{random.randint(1000, 9999)}"
        payload = json.dumps({
            "pickup": pickup, "dropoff": dropoff, "pickup_date": pickup_date,
            "pickup_time": pickup_time, "weight": weight, "courier_type": courier_type,
            "special_requests": special_requests, "total_price": weight * price_per_kg
        })
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO requests
                (user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at)
                VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s)
            """, (current_user.get_id(), booking_id, 'Courier Booking', payload, 'Pending', 'Pending', datetime.now()))
            conn.commit()
            socketio.emit('new_request', {'request': get_last_request_json()}, to=None)
            flash("Initial request saved.", "success")
        except Exception as e:
            conn.rollback()
            flash(f"DB error: {e}", "danger")
        finally:
            cur.close()
            conn.close()
    else:
        flash("Search updated – showing 5–6 couriers.", "info")

    all_couriers = []
    base_names = [
        "SwiftFly", "NinjaPost", "TurboShip", "SpeedyWing", "FlashCargo",
        "ZoomX", "RocketMail", "BlitzSend", "JetPack", "HyperCourier",
        "LightningDrop", "VortexShip", "CometCarry", "MeteorMove", "AstroPost"
    ]

    for _ in range(25):
        name = random.choice(base_names)
        hours_offset = random.randint(1, 6)
        est_drop = (pickup_dt + timedelta(hours=hours_offset)).strftime("%H:%M")
        is_express = random.random() < 0.4

        all_couriers.append({
            "id": f"{'EXP' if is_express else 'COU'}-{random.randint(100, 9999)}",
            "name": name,
            "pickup": pickup,
            "dropoff": dropoff,
            "pickup_time": pickup_time,
            "dropoff_time": est_drop,
            "courier_type": "Express" if is_express else courier_type.capitalize(),
            "max_weight": random.choice([20, 25, 30, 40, 50]),
            "rating": round(random.uniform(4.0, 5.0), 1),
            "availability": "Available",
            "duration": "Same day" if is_express else delivery_time,
            "price": 100 if is_express else price_per_kg
        })

    display_count = random.choice([5, 6])
    couriers = random.sample(all_couriers, display_count)

    return render_template(
        'courier_results.html',
        pickup=pickup,
        dropoff=dropoff,
        pickup_date=pickup_date,
        pickup_time=pickup_time,
        package_weight=weight,
        courier_type=courier_type.capitalize(),
        special_requests=special_requests,
        couriers=couriers,
        current_user_id=current_user.get_id()
    )

@app.route('/courier/confirm', methods=['POST'])
@login_required
def confirm_courier():
    data = request.get_json(silent=True) or {}

    required_fields = [
        'courier_id', 'name', 'pickup', 'dropoff',
        'pickup_date', 'pickup_time', 'courier_type',
        'package_weight', 'duration', 'total_price',
        'email', 'mobile',
        'sender_name', 'sender_address', 'receiver_name',
        'receiver_phone', 'receiver_address', 'package_description'
    ]

    missing = [f for f in required_fields if not data.get(f)]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    try:
        package_weight = float(data['package_weight'])
        total_price = float(data['total_price'])
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid weight or price"}), 400

    import re
    phone_pattern = re.compile(r'^\d{10}$')
    if not phone_pattern.match(data['mobile']):
        return jsonify({"error": "Your mobile must be 10 digits"}), 400
    if data.get('sender_phone') and not phone_pattern.match(data['sender_phone']):
        return jsonify({"error": "Sender phone must be 10 digits"}), 400
    if not phone_pattern.match(data['receiver_phone']):
        return jsonify({"error": "Receiver phone must be 10 digits"}), 400

    booking_id = f"COURIER-{random.randint(1000, 9999)}"

    payload = {
        "courier_id": data['courier_id'],
        "courier_name": data['name'],
        "pickup_location": data['pickup'],
        "dropoff_location": data['dropoff'],
        "pickup_date": data['pickup_date'],
        "pickup_time": data['pickup_time'],
        "courier_type": data['courier_type'],
        "package_weight_kg": package_weight,
        "delivery_duration": data['duration'],
        "base_price_per_kg": data.get('price'),
        "total_price_inr": total_price,
        "customer_email": data['email'],
        "customer_mobile": data['mobile'],
        "sender": {
            "name": data['sender_name'].strip(),
            "phone": data.get('sender_phone', '').strip(),
            "full_address": data['sender_address'].strip()
        },
        "receiver": {
            "name": data['receiver_name'].strip(),
            "phone": data['receiver_phone'].strip(),
            "full_address": data['receiver_address'].strip()
        },
        "package": {
            "description": data['package_description'].strip(),
            "weight_kg": package_weight
        },
        "booking_timestamp": datetime.now().isoformat()
    }

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO requests
            (user_id, booking_id, service_type, details, payment_status, admin_confirmation)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
        """, (
            current_user.get_id(),
            booking_id,
            'Courier Booking',
            json.dumps(payload),
            'Confirmed',
            'Pending'
        ))
        conn.commit()

        row = get_last_request_json()
        if row:
            socketio.emit('new_request', {'request': row}, to=None)
            socketio.emit('payment_confirmed', {
                "request_id": row[0],
                "booking_id": booking_id,
                "service_type": 'Courier Booking'
            }, to=None)

        return jsonify({
            "success": True,
            "booking_id": booking_id,
            "message": "Booking confirmed!"
        })

    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Booking DB error: {e}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()

# --------------------------------------------------------------
# 3. USER DETAILS (prefill email/phone)
# --------------------------------------------------------------
@app.route('/get_user_details', methods=['GET'])
@login_required
def get_user_details():
    user_id = current_user.get_id()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT email, phone FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        return jsonify({
            "email": row[0] if row else "",
            "phone": row[1] if row else ""
        })
    except Exception as e:
        return jsonify({"email": "", "phone": "", "error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# ---------------------- Dashboard / Profile ----------------------
@app.route('/dashboard')
@login_required
def dashboard():
    user_id = current_user.get_id()
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'users' 
            AND table_schema = 'public'
        """)
        existing_columns = [row[0] for row in cur.fetchall()]
        
        select_fields = ["full_name", "email", "username"]
        
        optional_fields = ['address', 'phone', 'whatsapp', 'instagram', 'facebook']
        for field in optional_fields:
            if field in existing_columns:
                select_fields.append(field)
        
        query = f"SELECT {', '.join(select_fields)} FROM users WHERE id = %s"
        
        cur.execute(query, (user_id,))
        user_data = cur.fetchone()

        cur.execute("""
            SELECT id, booking_id, service_type, details, payment_status, admin_confirmation, created_at
            FROM requests 
            WHERE user_id = %s 
            ORDER BY created_at DESC
        """, (user_id,))
        user_requests_raw = cur.fetchall()
        
        requests = []
        for req in user_requests_raw:
            req_id, booking_id, service_type, details_json, payment_status, admin_confirmation, created_at = req
            
            try:
                details = json.loads(details_json) if isinstance(details_json, str) else details_json
            except Exception:
                details = {"raw": str(details_json)}

            requests.append({
                "id": req_id,
                "booking_id": booking_id,
                "service_type": service_type,
                "details": details,
                "payment_status": payment_status,
                "admin_confirmation": admin_confirmation,
                "created_at": created_at.strftime('%Y-%m-%d %H:%M:%S') if created_at else 'N/A'
            })
        
        if user_data:
            contact = {}
            field_mapping = {
                'full_name': 'name',
                'address': 'address', 
                'phone': 'phone',
                'whatsapp': 'whatsapp',
                'email': 'email',
                'instagram': 'instagram',
                'facebook': 'facebook'
            }
            
            for i, field in enumerate(select_fields):
                contact_key = field_mapping.get(field, field)
                contact[contact_key] = user_data[i] or ''
            
            username = contact.get('name') or contact.get('email', '').split('@')[0] or 'User'
            
            notifications = get_user_notifications(user_id)
            unread_count = get_unread_count(user_id)

            return render_template('dashboard.html',
                       user=username,
                       contact=contact,
                       requests=requests,
                       notifications=notifications,
                       unread_count=unread_count,
                       current_user_id=user_id)
        else:
            flash("User data not found.", "danger")
            return redirect(url_for('login'))
            
    except Exception as e:
        print(f"Error in dashboard route: {e}")
        flash(f"Error loading dashboard: {str(e)}", "danger")
        return redirect(url_for('login'))
    finally:
        cur.close()
        conn.close()

@app.route('/save_contact', methods=['POST'])
@login_required
def save_contact():
    user_id = current_user.get_id()
    
    name = request.form.get('name', '').strip()
    address = request.form.get('address', '').strip()
    phone = request.form.get('phone', '').strip()
    whatsapp = request.form.get('whatsapp', '').strip()
    email = request.form.get('email', '').strip()
    instagram = request.form.get('instagram', '').strip()
    facebook = request.form.get('facebook', '').strip()

    if not name or not email:
        flash("Name and email are required fields.", "danger")
        return redirect(url_for('dashboard'))

    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'users' 
            AND table_schema = 'public'
        """)
        existing_columns = [row[0] for row in cur.fetchall()]
        
        update_fields = []
        values = []
        
        field_mapping = {
            'full_name': name,
            'address': address,
            'phone': phone, 
            'whatsapp': whatsapp,
            'email': email,
            'instagram': instagram,
            'facebook': facebook
        }
        
        for db_field, value in field_mapping.items():
            if db_field in existing_columns:
                update_fields.append(f"{db_field} = %s")
                values.append(value)
        
        if not update_fields:
            flash("No valid fields to update.", "danger")
            return redirect(url_for('dashboard'))
        
        values.append(user_id)
        
        query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"
        
        cur.execute(query, values)
        conn.commit()
        
        flash("Contact details updated successfully!", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"Error saving contact: {e}")
        flash(f"Error saving contact details: {str(e)}", "danger")
    finally:
        cur.close()
        conn.close()
    
    return redirect(url_for('dashboard'))

# ---------------------- Hotel / Travel ----------------------
@app.route('/hotel')
@login_required
def hotel_booking():
    return render_template("hotel.html", user=session['username'])

@app.route('/submit-hotel-booking', methods=['POST'])
@login_required
def submit_hotel_booking():
    destination = request.form['destination'].strip().capitalize()
    check_in = request.form['checkin']
    check_out = request.form['checkout']
    rooms = int(request.form.get('rooms', 1))
    guests = int(request.form.get('guests', 1))

    if not all([destination, check_in, check_out]) or rooms < 1 or guests < 1:
        flash("Please provide valid booking details.", "danger")
        return redirect(url_for('hotel_booking'))

    try:
        check_in_date = datetime.strptime(check_in, '%Y-%m-%d')
        check_out_date = datetime.strptime(check_out, '%Y-%m-%d')
        if check_out_date <= check_in_date:
            flash("Check-out date must be after check-in date.", "danger")
            return redirect(url_for('hotel_booking'))
    except ValueError:
        flash("Invalid date format. Use YYYY-MM-DD.", "danger")
        return redirect(url_for('hotel_booking'))

    allowed_cities = list(hotels_data.keys())
    if destination not in allowed_cities:
        flash("Hotel bookings are not available for this city yet.", "warning")
        return render_template('hotel_results.html', message=f"Coming soon in {destination}", destination=destination)

    all_hotels = hotels_data[destination]
    random.shuffle(all_hotels)
    selected_hotels = all_hotels[:min(len(all_hotels), 5)]

    return render_template('hotel_results.html',
                         hotels=selected_hotels,
                         destination=destination,
                         checkin=check_in,
                         checkout=check_out,
                         rooms=rooms,
                         guests=guests)

@app.route('/submit-travel-booking', methods=['POST'])
@login_required
def submit_travel_booking():
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    origin = request.form.get('origin', '').strip().title()
    destination = request.form.get('destination', '').strip().title()
    departure_date = request.form.get('departure_date')
    return_date = request.form.get('return_date')
    adults = request.form.get('adults', 1)
    children = request.form.get('children', 0)
    infants = request.form.get('infants', 0)
    travel_class = request.form.get('class', 'economy')
    is_initial = request.form.get('is_initial', 'false') == 'true'

    logger.debug(f"Received form data: origin={origin}, destination={destination}, departure_date={departure_date}, "
                 f"return_date={return_date}, adults={adults}, children={children}, infants={infants}, travel_class={travel_class}, is_initial={is_initial}")

    if not all([origin, destination, departure_date]):
        logger.warning("Validation failed: Missing required fields")
        flash("Please provide valid travel details.", "danger")
        return redirect(url_for('dashboard'))

    if origin.lower() == destination.lower():
        logger.warning("Validation failed: Origin and destination are the same")
        flash("Origin and destination cannot be the same.", "danger")
        return redirect(url_for('dashboard'))

    try:
        adults = int(adults)
        children = int(children)
        infants = int(infants)
        if adults < 1:
            logger.warning("Validation failed: Adults less than 1")
            flash("At least one adult is required.", "danger")
            return redirect(url_for('dashboard'))
    except ValueError:
        logger.warning("Validation failed: Invalid number format for passengers")
        flash("Invalid number of passengers.", "danger")
        return redirect(url_for('dashboard'))

    try:
        departure = datetime.strptime(departure_date, '%Y-%m-%d')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if departure < today:
            logger.warning("Validation failed: Departure date in the past")
            flash("Departure date cannot be in the past.", "danger")
            return redirect(url_for('dashboard'))
        if return_date:
            return_date_obj = datetime.strptime(return_date, '%Y-%m-%d')
            if return_date_obj <= departure:
                logger.warning("Validation failed: Return date not after departure date")
                flash("Return date must be after departure date.", "danger")
                return redirect(url_for('dashboard'))
    except ValueError as e:
        logger.error(f"Validation failed: Invalid date format - {str(e)}")
        flash("Invalid date format. Use YYYY-MM-DD.", "danger")
        return redirect(url_for('dashboard'))

    flights = generate_flight_data(origin, destination, travel_class)
    
    display_count = random.choice([5, 6])
    if len(flights) > display_count:
        displayed_flights = random.sample(flights, display_count)
    else:
        displayed_flights = flights

    arrival_date = departure_date

    if is_initial:
        booking_id = f"FLIGHT-{random.randint(1000, 9999)}"
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            payload = json.dumps({
                'origin': origin,
                'destination': destination,
                'departure_date': departure_date,
                'return_date': return_date,
                'adults': adults,
                'children': children,
                'infants': infants,
                'travel_class': travel_class
            })
            cur.execute("""
                INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at)
                VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s)
            """, (
                current_user.get_id(),
                booking_id,
                'Flight Booking',
                payload,
                'Pending',
                'Pending',
                datetime.now()
            ))
            conn.commit()
            socketio.emit('new_request', {'request': get_last_request_json()}, to=None)
            logger.debug(f"Flight booking saved: booking_id={booking_id}")
            flash("Flight search submitted successfully!", "success")
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"Error saving flight booking: {str(e)}")
            flash(f"Database error: {str(e)}. Please ensure the requests table exists.", "danger")
        finally:
            cur.close()
            conn.close()
    else:
        flash("Search updated – showing available flights.", "info")

    logger.debug("Rendering travel_results.html")
    return render_template('travel_results.html',
                         origin=origin,
                         destination=destination,
                         departure_date=departure_date,
                         return_date=return_date,
                         arrival_date=arrival_date,
                         adults=adults,
                         children=children,
                         infants=infants,
                         travel_class=travel_class,
                         flights=displayed_flights,
                         current_user_id=current_user.get_id())

def generate_flight_data(origin, destination, travel_class):
    """Generate realistic flight data"""
    flights = []
    
    airlines = [
        {'name': 'IndiGo', 'code': '6E', 'hub': 'DEL'},
        {'name': 'Global Airlines', 'code': 'GA', 'hub': 'BOM'},
        {'name': 'SpiceJet', 'code': 'SG', 'hub': 'DEL'},
        {'name': 'Air India', 'code': 'AI', 'hub': 'DEL'},
        {'name': 'Vistara', 'code': 'UK', 'hub': 'DEL'},
        {'name': 'Silver Wings', 'code': 'SW', 'hub': 'BOM'},
        {'name': 'Red Sky', 'code': 'RS', 'hub': 'BLR'},
        {'name': 'Golden Route', 'code': 'GR', 'hub': 'MAA'},
        {'name': 'Air India Express', 'code': 'IX', 'hub': 'CCJ'},
        {'name': 'Crystal Jets', 'code': 'CJ', 'hub': 'HYD'}
    ]
    
    airport_codes = {
        'Mumbai': 'BOM',
        'Delhi': 'DEL',
        'Bangalore': 'BLR',
        'Chennai': 'MAA',
        'Hyderabad': 'HYD',
        'Kolkata': 'CCU',
        'Pune': 'PNQ',
        'Goa': 'GOI',
        'Jaipur': 'JAI',
        'Ahmedabad': 'AMD'
    }
    
    for i in range(random.randint(10, 12)):
        airline = random.choice(airlines)
        origin_code = airport_codes.get(origin, 'XXX')
        destination_code = airport_codes.get(destination, 'YYY')

        flight_number = f"{airline['code']}{random.randint(100, 999)}"

        flight_name = f"{airline['name']} {flight_number}"
        
        departure_hour = random.randint(6, 22)
        departure_minute = random.choice([0, 15, 30, 45])
        
        duration_hours = random.randint(1, 6)
        duration_minutes = random.randint(0, 59)
        
        arrival_hour = (departure_hour + duration_hours) % 24
        arrival_minute = (departure_minute + duration_minutes) % 60
        if departure_minute + duration_minutes >= 60:
            arrival_hour = (arrival_hour + 1) % 24
        
        base_price = {
            'economy': random.randint(3000, 8000),
            'premium_economy': random.randint(6000, 12000),
            'business': random.randint(15000, 30000),
            'first': random.randint(25000, 50000)
        }.get(travel_class, random.randint(3000, 8000))
        
        baggage_allowance = {
            'economy': f"{random.choice([15, 20])}kg",
            'premium_economy': f"{random.choice([20, 25])}kg",
            'business': f"{random.choice([30, 35])}kg",
            'first': f"{random.choice([40, 50])}kg"
        }.get(travel_class, "20kg")
        
        stops = random.choice([0, 0, 0, 1, 1, 2])
        
        flight_data = {
            'airline': airline['name'],
            'departure_time': f'{departure_hour:02d}:{departure_minute:02d}',
            'arrival_time': f'{arrival_hour:02d}:{arrival_minute:02d}',
            'origin': origin,
            'destination': destination,
            'origin_code': origin_code,
            'destination_code': destination_code,
            'flight_no': flight_number,
            'flight_name': flight_name,
            'duration': f'{duration_hours}h {duration_minutes}m',
            'travel_class': travel_class,
            'seats_available': random.randint(2, 20),
            'price': base_price,
            'status': random.choice(['On Time', 'On Time', 'On Time', 'Delayed', 'Boarding']),
            'baggage_allowance': baggage_allowance,
            'meal_included': travel_class in ['business', 'first'] or random.choice([True, False]),
            'wifi_available': travel_class in ['business', 'first'] or random.choice([True, False]),
            'stops': stops,
            'refundable': random.choice([True, False]),
            'deal': random.choice(['', '', '', 'Fastest', 'Cheapest', 'Best Deal']),
            'seats': random.randint(5, 40)
        }
        flights.append(flight_data)
    
    flights.sort(key=lambda x: x['price'])
    
    return flights

# ---------------------- PDF Ticket download ----------------------
@app.route('/download-ticket/<booking_id>')
@login_required
def download_ticket(booking_id):
    filename = f"ticket_{booking_id}.pdf"
    path = TICKETS_DIR / filename
    if not path.exists():
        flash("PDF ticket not available yet.", "danger")
        return redirect(url_for('dashboard'))
    return send_from_directory(str(TICKETS_DIR), filename, as_attachment=True)

# ---------------------- Flight: confirm (mock payment) ----------------------
@app.route('/confirm-flight', methods=['POST'])
@login_required
def confirm_flight():
    data = request.get_json(silent=True) or {}
    
    logger.info(f"Received flight booking data: {json.dumps(data, indent=2)}")
    
    flight_data = data.get('flight')
    amount = data.get('amount')
    departure_date = data.get('departure_date')
    email = data.get('email')
    booking_id = data.get('booking_id') or f"FLIGHT-{random.randint(100000, 999999)}-{random.randint(100, 999)}"
    
    if not all([flight_data, amount, departure_date, email]):
        logger.error(f"Missing flight data: {data}")
        return jsonify({"success": False, "error": "Missing essential booking data."}), 400
    
    details_obj = {
        "airline": None,
        "flight_no": None,
        "origin": data.get('origin', 'N/A'),
        "destination": data.get('destination', 'N/A'),
        "departure_time": None,
        "arrival_time": None,
        "travel_class": data.get('travel_class', 'economy'),
        "duration": None,
        "baggage_allowance": None,
        "seats_available": None,
        
        "price": amount,
        "departure_date": departure_date,
        "return_date": data.get('return_date'),
        "passengers": {
            "adults": data.get('adults', 1),
            "children": data.get('children', 0),
            "infants": data.get('infants', 0)
        },
        "customer_email": email,
        "customer_mobile": data.get('mobile', ''),
        "booking_timestamp": datetime.now().isoformat(),
        "status": "Confirmed",
        "booking_id": booking_id,
        
        "traveller_details": data.get('traveller_details', []),
        "seat_preference": data.get('seat_preference', 'no_preference'),
        "special_requests": data.get('special_requests', '')
    }
    
    if isinstance(flight_data, dict):
        details_obj.update({
            "airline": flight_data.get('airline', 'N/A'),
            "flight_no": flight_data.get('flight_no', f"FL-{random.randint(1000, 9999)}"),
            "origin": flight_data.get('origin', details_obj['origin']),
            "destination": flight_data.get('destination', details_obj['destination']),
            "origin_code": flight_data.get('origin_code', 'XXX'),
            "destination_code": flight_data.get('destination_code', 'YYY'),
            "departure_time": flight_data.get('departure_time', 'N/A'),
            "arrival_time": flight_data.get('arrival_time', 'N/A'),
            "duration": flight_data.get('duration', 'N/A'),
            "baggage_allowance": flight_data.get('baggage', flight_data.get('baggage_allowance', '20kg')),
            "seats_available": flight_data.get('seats_available', 1),
            "stops": flight_data.get('stops', 0),
            "refundable": flight_data.get('refundable', False)
        })
    elif isinstance(flight_data, str):
        details_obj.update({
            "flight_no": flight_data,
            "airline": data.get('airline', 'Unknown Airline')
        })
    else:
        details_obj.update({
            "flight_no": f"FL-{random.randint(1000, 9999)}",
            "airline": "Airline"
        })
    
    if not details_obj['flight_no'] or details_obj['flight_no'] == 'N/A':
        airline = details_obj['airline']
        airline_codes = {
            'IndiGo': '6E',
            'SpiceJet': 'SG',
            'Air India': 'AI',
            'Vistara': 'UK',
            'Crystal Jets': 'CJ',
            'Global Airlines': 'GA'
        }
        code = 'FL'
        for name, airline_code in airline_codes.items():
            if name.lower() in airline.lower():
                code = airline_code
                break
        details_obj['flight_no'] = f"{code}{random.randint(100, 999)}"
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            RETURNING id
        """, (
            current_user.get_id(),
            booking_id,
            'Flight Booking',
            json.dumps(details_obj),
            'Confirmed',
            'Pending'
        ))
        new_id = cur.fetchone()[0]
        conn.commit()
        
        return jsonify({
            "success": True, 
            "booking_id": booking_id, 
            "request_id": new_id,
            "flight_no": details_obj['flight_no'],
            "message": "Flight booking confirmed successfully!"
        })
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Flight booking confirmation error: {e}")
        return jsonify({"success": False, "error": f"Database error: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()

# ---------------------- Auth misc ----------------------
@app.route('/mark-notifications-read', methods=['POST'])
@login_required
def mark_notifications_read_route():
    mark_notifications_read(current_user.get_id())
    return jsonify({"success": True})

@app.route('/get-unread-count')
@login_required
def get_unread_count_route():
    count = get_unread_count(current_user.get_id())
    return jsonify({"unread_count": count})

@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.clear()
    flash("You have been logged out successfully.")
    return redirect(url_for('login'))

@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        username = request.form.get('username')

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT password FROM users WHERE username = %s", (username,))
        result = cur.fetchone()
        cur.close()
        conn.close()

        if result:
            password = result[0]
            flash(f"Your password is: {password}")
        else:
            flash("Username not found. Please try again.")

        return redirect(url_for('forgot_password'))
    return render_template('forgot_password.html')

# ---------------------- Debug Route ----------------------
@app.route('/debug-db')
def debug_db():
    """Temporary route to check database structure"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'users' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        
        cur.execute("SELECT id, username, email FROM users LIMIT 5")
        sample_data = cur.fetchall()
        
        return f"""
        <h1>Database Debug Info</h1>
        <h2>Users Table Columns:</h2>
        <pre>{columns}</pre>
        <h2>Sample Data:</h2>
        <pre>{sample_data}</pre>
        """
        
    except Exception as e:
        return f"Error: {e}"
    finally:
        cur.close()
        conn.close()

def fix_existing_flight_numbers():
    """Fix existing flight bookings that have N/A flight numbers"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT id, details 
            FROM requests 
            WHERE service_type = 'Flight Booking' 
            AND (details::text LIKE '%N/A%' OR details::text NOT LIKE '%flight_no%')
        """)
        rows = cur.fetchall()
        
        for row in rows:
            request_id, details_json = row
            
            try:
                if isinstance(details_json, str):
                    details = json.loads(details_json)
                else:
                    details = details_json or {}
                
                if not details.get('flight_no') or details.get('flight_no') == 'N/A':
                    airline_codes = ['6E', 'SG', 'AI', 'UK', 'GA', 'SW', 'RS', 'GR', 'IX', 'CJ']
                    airline = random.choice(airline_codes)
                    flight_no = f"{airline}{random.randint(100, 999)}"
                    
                    details['flight_no'] = flight_no
                    
                    cur.execute("""
                        UPDATE requests 
                        SET details = %s::jsonb 
                        WHERE id = %s
                    """, (json.dumps(details), request_id))
                    print(f"Updated request {request_id} with flight number {flight_no}")
            
            except Exception as e:
                print(f"Error processing request {request_id}: {e}")
                continue
        
        conn.commit()
        print(f"Updated {len(rows)} flight bookings")
        
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

# ---------------------- AI Recommendations API ----------------------

@app.route('/api/save-location', methods=['POST'])
@login_required
def api_save_location():
    """Save user location"""
    data = request.json
    user_id = current_user.get_id()
    
    city = data.get('city')
    state = data.get('state')
    country = data.get('country')
    latitude = data.get('latitude')
    longitude = data.get('longitude')
    
    # Save to session for now (can be saved to database later)
    session['user_location'] = {
        'city': city,
        'state': state,
        'country': country,
        'latitude': latitude,
        'longitude': longitude,
        'updated_at': datetime.now().isoformat()
    }
    
    logger.info(f"Saved location for user {user_id}: {city}, {state}, {country}")
    
    return jsonify({'success': True, 'message': 'Location saved successfully'})


@app.route('/api/dismiss-recommendation', methods=['POST'])
@login_required
def api_dismiss_recommendation():
    """Dismiss a recommendation"""
    data = request.json
    recommendation_id = data.get('recommendation_id')
    user_id = current_user.get_id()
    
    # In production, save dismissed recommendations to database
    # For now, just acknowledge
    logger.info(f"User {user_id} dismissed recommendation {recommendation_id}")
    
    return jsonify({'success': True, 'message': 'Recommendation dismissed'})

# ---------------------- IMPROVED AI Recommendations API ----------------------
@app.route('/api/lifestyle-recommendations')
@login_required
def api_lifestyle_recommendations():
    """Get AI-powered recommendations for user - FIXED VERSION"""
    try:
        user_id = current_user.get_id()
        
        # Get user's lifestyle profile
        profile = get_user_profile(user_id)
        
        if not profile or not profile.get('interests'):
            return jsonify({
                'success': True,
                'recommendations': [],
                'message': 'Please complete your lifestyle profile to get recommendations',
                'has_profile': False
            })
        
        # Generate recommendations based on profile
        recommendations = []
        interests = profile.get('interests', '').split(',') if profile.get('interests') else []
        travel_style = profile.get('travel_style', 'comfort')
        group_size = profile.get('group_size', 1)
        cab_type = profile.get('cab_type', 'economy')
        home_owner = profile.get('home_owner', False)
        
        logger.info(f"Generating recommendations for user {user_id} with profile: {profile}")
        
        # Hotel recommendations
        if any(interest in ['travel', 'luxury', 'business'] for interest in interests):
            recommendations.append({
                'id': 1,
                'service_type': 'Hotel Booking',
                'reason': f'Based on your {travel_style} travel style, we recommend luxury hotels with premium amenities',
                'match_score': 95,
                'metadata': {
                    'price': '₹5,000-15,000/night',
                    'location': 'Major Cities',
                    'amenities': 'Pool, Spa, Restaurant'
                }
            })
        
        # Flight recommendations
        if any(interest in ['travel', 'adventure', 'business'] for interest in interests):
            recommendations.append({
                'id': 2,
                'service_type': 'Flight Booking',
                'reason': 'Frequent traveler? Get the best flight deals for your next adventure',
                'match_score': 90,
                'metadata': {
                    'price': '₹3,000-20,000',
                    'duration': 'Domestic & International',
                    'class': travel_style.title()
                }
            })
        
        # Car recommendations
        if cab_type in ['luxury', 'premium', 'suv'] or group_size > 4:
            recommendations.append({
                'id': 3,
                'service_type': 'Car Booking',
                'reason': f'Perfect for groups of {group_size}. {cab_type.title()} cabs available',
                'match_score': 88,
                'metadata': {
                    'price': '₹1,500-3,000/trip',
                    'vehicle': cab_type.title(),
                    'capacity': f'{group_size}+ passengers'
                }
            })
        
        # Technician recommendations
        if home_owner:
            recommendations.append({
                'id': 4,
                'service_type': 'Technician Booking',
                'reason': 'As a homeowner, get verified technicians for AC, plumbing, electrical work',
                'match_score': 85,
                'metadata': {
                    'price': '₹500-1,500',
                    'availability': 'Same Day Service',
                    'verified': 'Yes'
                }
            })
        
        # Courier recommendations
        if any(interest in ['business', 'shopping'] for interest in interests):
            recommendations.append({
                'id': 5,
                'service_type': 'Courier Booking',
                'reason': 'Fast and reliable courier services for your needs',
                'match_score': 82,
                'metadata': {
                    'price': '₹50-200/kg',
                    'delivery': 'Same Day Available',
                    'tracking': 'Real-time'
                }
            })
        
        logger.info(f"Generated {len(recommendations)} recommendations for user {user_id}")
        
        return jsonify({
            'success': True,
            'recommendations': recommendations[:5],  # Limit to top 5
            'new_recommendations': len(recommendations) > 0,
            'has_profile': True
        })
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        import traceback
        traceback.print_exc()  # Print full error for debugging
        return jsonify({
            'success': False,
            'error': str(e),
            'recommendations': [],
            'message': 'Error generating recommendations. Please try again.'
        }), 500


# ---------------------- IMPROVED Nearby Services API ----------------------
@app.route('/api/nearby-services', methods=['POST'])
@login_required
def api_nearby_services():
    """Get services near user location with PROPER booking data"""
    try:
        data = request.json
        lat = data.get('lat')
        lng = data.get('lng')
        
        if not lat or not lng:
            return jsonify({'success': False, 'error': 'Location required'}), 400
        
        logger.info(f"Finding services near: {lat}, {lng}")
        
        # Get user location from OpenStreetMap (reverse geocode)
        try:
            import requests
            response = requests.get(
                f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lng}&zoom=10",
                headers={'User-Agent': 'ConciergeLifestyle/1.0'}
            )
            location_data = response.json()
            city = location_data.get('address', {}).get('city') or location_data.get('address', {}).get('town') or 'Mumbai'
        except:
            city = 'Mumbai'  # Fallback
        
        services = []
        
        # Hotels - with actual booking data
        hotels = [
            {
                'id': 1,
                'name': 'The Taj Majestic',
                'type': 'Hotel Booking',
                'description': 'Luxury 5-star hotel with world-class amenities and spa',
                'distance': round(random.uniform(0.5, 3.0), 1),
                'lat': lat + random.uniform(-0.02, 0.02),
                'lng': lng + random.uniform(-0.02, 0.02),
                'rating': 4.8,
                'price': '₹8,000/night',
                'address': f'{city} Downtown',
                'booking_data': {
                    'destination': city,
                    'hotel_name': 'The Taj Majestic',
                    'price_per_night': 8000,
                    'available_rooms': 15
                }
            },
            {
                'id': 2,
                'name': 'Grand Plaza Hotel',
                'type': 'Hotel Booking',
                'description': 'Business hotel with conference facilities',
                'distance': round(random.uniform(1.0, 4.0), 1),
                'lat': lat + random.uniform(-0.03, 0.03),
                'lng': lng + random.uniform(-0.03, 0.03),
                'rating': 4.4,
                'price': '₹4,500/night',
                'address': f'{city} City Center',
                'booking_data': {
                    'destination': city,
                    'hotel_name': 'Grand Plaza Hotel',
                    'price_per_night': 4500,
                    'available_rooms': 20
                }
            }
        ]
        
        # Technicians - with booking data
        technicians = [
            {
                'id': 3,
                'name': 'QuickFix Home Services',
                'type': 'Technician Booking',
                'description': 'AC repair, plumbing, electrical - Available 24/7',
                'distance': round(random.uniform(0.3, 2.0), 1),
                'lat': lat + random.uniform(-0.015, 0.015),
                'lng': lng + random.uniform(-0.015, 0.015),
                'rating': 4.6,
                'price': '₹500-1,200',
                'address': f'{city} Residential Area',
                'booking_data': {
                    'technician_id': 'TECH-001',
                    'service_types': ['AC Repair', 'Plumbing', 'Electrical'],
                    'location': f'{city} Area',
                    'hourly_rate': 800
                }
            },
            {
                'id': 4,
                'name': 'Expert Repairs',
                'type': 'Technician Booking',
                'description': 'Professional technicians for all home repairs',
                'distance': round(random.uniform(0.5, 2.5), 1),
                'lat': lat + random.uniform(-0.02, 0.02),
                'lng': lng + random.uniform(-0.02, 0.02),
                'rating': 4.7,
                'price': '₹600-1,500',
                'address': f'{city} Service Area',
                'booking_data': {
                    'technician_id': 'TECH-002',
                    'service_types': ['Carpentry', 'Painting', 'General Repair'],
                    'location': f'{city} Area',
                    'hourly_rate': 900
                }
            }
        ]
        
        # Cars - with booking data
        cars = [
            {
                'id': 5,
                'name': 'Premium Cab Services',
                'type': 'Car Booking',
                'description': 'Luxury cabs with professional drivers',
                'distance': round(random.uniform(1.0, 5.0), 1),
                'lat': lat + random.uniform(-0.04, 0.04),
                'lng': lng + random.uniform(-0.04, 0.04),
                'rating': 4.7,
                'price': '₹1,500-3,000',
                'address': f'{city} Main Road',
                'booking_data': {
                    'cab_class': 'luxury',
                    'vehicle_model': 'BMW 5 Series',
                    'pickup_location': f'{city}',
                    'base_fare': 1500
                }
            }
        ]
        
        # Couriers - with booking data
        couriers = [
            {
                'id': 6,
                'name': 'Express Courier Hub',
                'type': 'Courier Booking',
                'description': 'Same-day delivery across the city',
                'distance': round(random.uniform(0.8, 3.5), 1),
                'lat': lat + random.uniform(-0.025, 0.025),
                'lng': lng + random.uniform(-0.025, 0.025),
                'rating': 4.5,
                'price': '₹100-500',
                'address': f'{city} Commercial District',
                'booking_data': {
                    'courier_type': 'express',
                    'max_weight': 20,
                    'pickup_location': f'{city}',
                    'price_per_kg': 50
                }
            }
        ]
        
        # Combine all services
        services = hotels + technicians + cars + couriers
        
        # Sort by distance
        services.sort(key=lambda x: x['distance'])
        
        logger.info(f"Found {len(services)} services near {city}")
        
        return jsonify({
            'success': True,
            'services': services[:8],
            'user_location': {'lat': lat, 'lng': lng, 'city': city}
        })
        
    except Exception as e:
        logger.error(f"Error finding nearby services: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error finding nearby services. Please try again.'
        }), 500


# ---------------------- AI CHATBOT API ----------------------
@app.route('/api/chatbot', methods=['POST'])
@login_required
def api_chatbot():
    """Handle chatbot conversations - IMPROVED VERSION"""
    try:
        data = request.json
        user_message = data.get('message', '').strip()
        user_id = current_user.get_id()
        
        if not user_message:
            return jsonify({
                'success': False,
                'error': 'Message is required'
            }), 400
        
        logger.info(f"Chatbot message from user {user_id}: {user_message}")
        
        # Get user profile for context
        profile = get_user_profile(user_id)
        
        # Improved response system
        message_lower = user_message.lower()
        
        # Greeting responses
        if any(word in message_lower for word in ['hi', 'hello', 'hey', 'greetings', 'good morning', 'good afternoon', 'good evening']):
            responses = [
                "Hello! 👋 I'm your Concierge Lifestyle assistant. How can I help you today?",
                "Hi there! 🌟 Ready to book something amazing? Ask me anything!",
                "Hey! 😊 I'm here to help with hotels, flights, cabs, and more. What do you need?",
                "Good day! 🎯 What can I assist you with today?"
            ]
            bot_response = random.choice(responses)
            
        # Hotel queries
        elif any(word in message_lower for word in ['hotel', 'accommodation', 'stay', 'room', 'lodge']):
            bot_response = "🏨 Looking for a hotel? I can help! Which city are you planning to visit? We have luxury and budget options in Mumbai, Pune, and many more cities. Just let me know your destination!"
            
        # Flight queries
        elif any(word in message_lower for word in ['flight', 'fly', 'airplane', 'plane', 'travel', 'trip', 'ticket']):
            bot_response = "✈️ Planning a trip? Let me help you find the perfect flight! Where would you like to go? I can search domestic and international flights with great deals."
            
        # Car/Cab queries
        elif any(word in message_lower for word in ['cab', 'car', 'taxi', 'ride', 'driver', 'transport']):
            bot_response = "🚗 Need a ride? We have economy, luxury, and SUV options available. Where would you like to go? I can arrange pickup and drop-off anywhere!"
            
        # Technician queries
        elif any(word in message_lower for word in ['technician', 'repair', 'fix', 'plumb', 'electric', 'ac', 'broken', 'maintenance']):
            bot_response = "🔧 Having technical issues at home? Our verified technicians can help with AC repair, plumbing, electrical work, carpentry, and more. What needs fixing?"
            
        # Courier queries
        elif any(word in message_lower for word in ['courier', 'delivery', 'send', 'package', 'parcel', 'ship']):
            bot_response = "📦 Need to send something? Our courier services offer same-day delivery! Where do you want to send your package?"
            
        # Price queries
        elif any(word in message_lower for word in ['price', 'cost', 'expensive', 'cheap', 'rate', 'charge', 'fee']):
            bot_response = "💰 Prices vary by service:\n• Hotels: ₹2,000-15,000/night\n• Flights: ₹3,000-50,000\n• Cabs: ₹1,000-3,000/trip\n• Technicians: ₹500-2,000\n• Courier: ₹50-500\n\nWhich service interests you?"
            
        # Recommendation queries
        elif any(word in message_lower for word in ['recommend', 'suggest', 'what should', 'advice', 'best']):
            if profile and profile.get('interests'):
                bot_response = "💡 Based on your lifestyle profile, I have personalized recommendations for you! Check out the 'AI Suggestions' section for tailored services just for you."
            else:
                bot_response = "💡 I'd love to give you personalized recommendations! Please complete your lifestyle profile first in the 'AI Suggestions' section, then I can suggest the best services based on your preferences."
        
        # Profile queries
        elif any(word in message_lower for word in ['profile', 'lifestyle', 'preferences', 'setting', 'account']):
            if profile and profile.get('interests'):
                interests = profile.get('interests', '').split(',')
                bot_response = f"📋 Your profile shows you're interested in: {', '.join(interests)}. You can update it anytime from the 'AI Suggestions' section!"
            else:
                bot_response = "📋 You haven't completed your lifestyle profile yet. Click on 'AI Suggestions' and then 'Complete Profile' to get personalized recommendations tailored to your interests!"
                
        # Booking status queries
        elif any(word in message_lower for word in ['booking', 'reservation', 'status', 'order', 'request']):
            bot_response = "📅 To check your bookings, go to 'My Requests' section. You can see all your active bookings, payment status, and download tickets there!"
            
        # Location/Nearby queries
        elif any(word in message_lower for word in ['near', 'nearby', 'close', 'around', 'location']):
            bot_response = "📍 Check the 'Nearby' section to find services close to your current location! We'll show you hotels, technicians, and more based on your GPS location."
            
        # Payment queries
        elif any(word in message_lower for word in ['payment', 'pay', 'cash', 'card', 'online']):
            bot_response = "💳 We accept multiple payment methods! After booking, you'll see payment options. For now, it's a simulated system to demonstrate the booking process."
            
        # Cancel queries
        elif any(word in message_lower for word in ['cancel', 'refund', 'return']):
            bot_response = "🔄 To cancel a booking, go to 'My Requests' and select your booking. Free cancellation is available up to 48 hours before check-in/service time!"
            
        # Help queries
        elif any(word in message_lower for word in ['help', 'how', 'what can you', 'feature', 'service']):
            bot_response = """🎯 I can help you with:
• Hotel bookings in major cities
• Flight bookings (domestic & international)
• Luxury cab services
• Home repair technicians
• Courier & delivery services
• Personalized AI recommendations
• Track your bookings

Just tell me what you need!"""
            
        # Thank you / Goodbye
        elif any(word in message_lower for word in ['thank', 'thanks', 'appreciate', 'bye', 'goodbye']):
            responses = [
                "You're welcome! 😊 Anything else I can help with?",
                "Happy to help! 🌟 Let me know if you need anything else.",
                "My pleasure! 👍 Feel free to ask if you have more questions.",
                "Glad I could help! 🎉 Have a great day!"
            ]
            bot_response = random.choice(responses)
            
        # Yes/No responses
        elif message_lower in ['yes', 'yeah', 'yep', 'sure', 'ok', 'okay']:
            bot_response = "Great! 😊 What would you like to do? I can help you book hotels, flights, cabs, technicians, or couriers. Just let me know!"
            
        elif message_lower in ['no', 'nope', 'not now']:
            bot_response = "No problem! 👍 I'm here whenever you need help. Just ask!"
            
        # Default - More flexible response
        else:
            # Try to extract keywords
            if len(user_message.split()) <= 3:
                # Short message - give general help
                bot_response = f"I see you mentioned '{user_message}'. 🤔 I can help you with:\n• Hotels & Accommodations\n• Flight Bookings\n• Cab Services\n• Technicians & Repairs\n• Courier Services\n\nWhich one interests you?"
            else:
                # Longer message - more conversational
                bot_response = "I understand you're asking about something, but I'm not quite sure what you need. 🤔 Could you rephrase that? I'm here to help with:\n\n✈️ Flights\n🏨 Hotels\n🚗 Cabs\n🔧 Technicians\n📦 Couriers\n\nWhat are you looking for?"
        
        return jsonify({
            'success': True,
            'response': bot_response,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Chatbot error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'response': "Sorry, I'm having trouble right now. Please try again or go directly to the Services section to make a booking!"
        }), 500


# ---------------------- Book Nearby Service ----------------------
@app.route('/api/book-nearby-service', methods=['POST'])
@login_required
def api_book_nearby_service():
    """Handle booking from nearby services with PRE-FILLED DATA"""
    try:
        data = request.json
        service_type = data.get('service_type')
        service_name = data.get('service_name')
        service_id = data.get('service_id')
        
        logger.info(f"Booking nearby service: {service_type} - {service_name} (ID: {service_id})")
        
        # Map service type to modal ID
        service_modal_map = {
            'Hotel Booking': 'hotel',
            'Car Booking': 'car',
            'Technician Booking': 'event',
            'Courier Booking': 'courier'
        }
        
        modal_id = service_modal_map.get(service_type, 'hotel')
        
        # Return pre-fill data along with modal ID
        return jsonify({
            'success': True,
            'modal_id': modal_id,
            'service_name': service_name,
            'service_id': service_id,
            'service_type': service_type,
            'prefill_data': {
                'service_name': service_name,
                'service_id': service_id,
            },
            'message': f'Opening {service_type} form with pre-filled details...'
        })
        
    except Exception as e:
        logger.error(f"Error booking nearby service: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Unable to open booking form. Please try again.'
        }), 500


# ---------------------- Run ----------------------
if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)