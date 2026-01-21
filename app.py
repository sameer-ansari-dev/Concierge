from flask import (
    Flask, render_template, request, redirect, url_for, flash, session,
    make_response, jsonify, send_from_directory, current_app
)
from werkzeug.utils import secure_filename
from flask_socketio import SocketIO, emit, join_room
import random
from db import save_user_profile_comprehensive, get_user_profile
from datetime import datetime, date, timedelta
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from db import get_db_connection
import psycopg2
import json
import os
import uuid
import math
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
import re

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'your_secret_key'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# File Upload Configuration
UPLOAD_FOLDER = os.path.join('static', 'uploads', 'profile_pictures')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Add these constants near other configurations
SUPPORT_UPLOAD_FOLDER = os.path.join('static', 'uploads', 'support_files')
os.makedirs(SUPPORT_UPLOAD_FOLDER, exist_ok=True)
ALLOWED_SUPPORT_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'pdf', 'doc', 'docx', 'txt'}

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ---------------------- SUPPORT CHAT FUNCTIONS ----------------------

def allowed_support_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_SUPPORT_EXTENSIONS

def save_support_file(file):
    if file and allowed_support_file(file.filename):
        filename = secure_filename(f"{uuid.uuid4().hex}_{file.filename}")
        filepath = os.path.join(SUPPORT_UPLOAD_FOLDER, filename)
        file.save(filepath)
        return f"/static/uploads/support_files/{filename}"
    return None

def get_user_unread_count(user_id):
    """Get count of unread support messages for user"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COUNT(*) 
            FROM support_messages 
            WHERE user_id = %s 
            AND sender_type = 'admin' 
            AND is_read = FALSE
        """, (user_id,))
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting unread count: {e}")
        return 0
    finally:
        cur.close()
        conn.close()

def mark_messages_read(user_id):
    """Mark all admin messages as read for user"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE support_messages 
            SET is_read = TRUE 
            WHERE user_id = %s 
            AND sender_type = 'admin' 
            AND is_read = FALSE
        """, (user_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error marking messages read: {e}")
    finally:
        cur.close()
        conn.close()

def get_support_chat_history(user_id):
    """Get chat history for support chat"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # First mark messages as read when fetching
        mark_messages_read(user_id)
        
        cur.execute("""
            SELECT 
                id,
                sender_type,
                message,
                message_type,
                file_path,
                created_at,
                is_read
            FROM support_messages 
            WHERE user_id = %s 
            ORDER BY created_at ASC
            LIMIT 100
        """, (user_id,))
        
        messages = []
        rows = cur.fetchall()
        for row in rows:
            messages.append({
                'id': row[0],
                'sender_type': row[1],
                'message': row[2],
                'message_type': row[3],
                'file_path': row[4],
                'timestamp': row[5].isoformat() if row[5] else '',
                'is_read': row[6],
                'is_me': row[1] == 'user'  # For user's perspective
            })
        
        return messages
    except Exception as e:
        logger.error(f"Error getting chat history: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def save_support_message(user_id, sender_type, message, message_type='text', file_path=None):
    """Save a support message to database"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO support_messages 
            (user_id, sender_type, message, message_type, file_path, is_read)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, created_at
        """, (
            user_id, 
            sender_type, 
            message, 
            message_type,
            file_path,
            True if sender_type == 'user' else False  # User messages are read immediately
        ))
        
        msg_id, timestamp = cur.fetchone()
        conn.commit()
        
        # Update user's unread count if admin sent message
        if sender_type == 'admin':
            cur.execute("""
                UPDATE users 
                SET unread_support_count = COALESCE(unread_support_count, 0) + 1 
                WHERE id = %s
            """, (user_id,))
            conn.commit()
        
        return msg_id, timestamp
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving support message: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

def save_support_file(file):
    """Save support file and return URL"""
    try:
        if file and allowed_support_file(file.filename):
            # Create unique filename
            timestamp = int(time.time())
            original_name = secure_filename(file.filename)
            filename = f"{timestamp}_{uuid.uuid4().hex}_{original_name}"
            
            # Ensure directory exists
            os.makedirs(SUPPORT_UPLOAD_FOLDER, exist_ok=True)
            
            # Save file
            file_path = os.path.join(SUPPORT_UPLOAD_FOLDER, filename)
            file.save(file_path)
            
            # Return relative URL
            return f"/static/uploads/support_files/{filename}"
    except Exception as e:
        logger.error(f"Error saving support file: {e}")
    
    return None

# ---------------------- SUPPORT CHAT ROUTES ----------------------

@app.route('/api/support/chat/history', methods=['GET'])
@login_required
def api_support_chat_history():
    """Get support chat history for user"""
    try:
        user_id = current_user.get_id()
        messages = get_support_chat_history(user_id)
        
        return jsonify({
            'success': True,
            'messages': messages,
            'unread_count': get_user_unread_count(user_id)
        })
    except Exception as e:
        logger.error(f"Error in chat history: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/support/chat/send', methods=['POST'])
@login_required
def api_support_chat_send():
    """Send a support message (User -> Admin)"""
    try:
        user_id = current_user.get_id()
        
        # Get message text
        message = request.form.get('message', '').strip()
        if not message and 'file' not in request.files:
            return jsonify({'success': False, 'error': 'Message is required'}), 400
        
        # Handle file upload
        file_url = None
        message_type = 'text'
        
        if 'file' in request.files:
            file = request.files['file']
            if file and file.filename:
                file_url = save_support_file(file)
                if file_url:
                    message_type = 'file'
                    if not message:
                        message = f"File: {file.filename}"
        
        # Save message
        msg_id, timestamp = save_support_message(
            user_id=user_id,
            sender_type='user',
            message=message,
            message_type=message_type,
            file_path=file_url
        )
        
        # Prepare response data
        response_data = {
            'id': msg_id,
            'user_id': user_id,
            'sender_type': 'user',
            'message': message,
            'message_type': message_type,
            'file_path': file_url,
            'timestamp': timestamp.isoformat(),
            'is_me': True
        }
        
        # Emit to admins only (prevents echo to user)
        socketio.emit('support_message_received', response_data, room='admin_support')
        
        return jsonify({
            'success': True,
            'message': 'Message sent',
            'data': response_data
        })
        
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/admin/support/users', methods=['GET'])
def api_admin_support_users():
    """Get list of users who contacted support - IMPROVED VERSION"""
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Get users with recent support messages - FIXED QUERY
        cur.execute("""
            SELECT 
                u.id,
                COALESCE(u.full_name, u.email) as name,
                u.email,
                u.profile_picture,
                COALESCE(u.unread_support_count, 0) as unread_count,
                sm.message as last_message,
                sm.created_at as last_message_at,
                sm.sender_type as last_sender,
                CASE 
                    WHEN EXISTS (
                        SELECT 1 FROM support_messages sm2 
                        WHERE sm2.user_id = u.id 
                        AND sm2.sender_type = 'user' 
                        AND sm2.created_at > NOW() - INTERVAL '5 minutes'
                    ) THEN 'online'
                    ELSE 'offline'
                END as status
            FROM users u
            LEFT JOIN support_messages sm ON u.id = sm.user_id
            WHERE u.id IN (
                SELECT DISTINCT user_id FROM support_messages
            )
            AND sm.id = (
                SELECT MAX(id) 
                FROM support_messages 
                WHERE user_id = u.id
            )
            ORDER BY sm.created_at DESC NULLS LAST
        """)
        
        rows = cur.fetchall()
        users = []
        
        for row in rows:
            users.append({
                'id': row[0],
                'name': row[1] or row[2].split('@')[0],
                'email': row[2],
                'profile_picture': row[3] or '',
                'unread_count': row[4],
                'last_message': (row[5] or '')[:50] + ('...' if len(row[5] or '') > 50 else ''),
                'last_message_at': row[6].isoformat() if row[6] else '',
                'last_sender': row[7] or 'user',
                'status': row[8] or 'offline'
            })
        
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        logger.error(f"Error getting support users: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/admin/support/messages/<int:user_id>', methods=['GET'])
def api_admin_support_messages(user_id):
    """Get support messages for a specific user - IMPROVED VERSION"""
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get user info
        cur.execute("""
            SELECT id, full_name, email, profile_picture 
            FROM users WHERE id = %s
        """, (user_id,))
        user_row = cur.fetchone()
        
        if not user_row:
            return jsonify({'success': False, 'error': 'User not found'}), 404
        
        user_info = {
            'id': user_row[0],
            'name': user_row[1] or user_row[2].split('@')[0],
            'email': user_row[2],
            'profile_picture': user_row[3] or '',
            'profile_picture_url': f"/static/uploads/profile_pictures/{user_row[3]}" if user_row[3] else ''
        }
        
        # Get messages with better ordering
        cur.execute("""
            SELECT 
                id,
                sender_type,
                message,
                message_type,
                file_path,
                created_at,
                is_read
            FROM support_messages 
            WHERE user_id = %s 
            ORDER BY created_at ASC
            LIMIT 200
        """, (user_id,))
        
        messages = []
        rows = cur.fetchall()
        for row in rows:
            file_path = row[4]
            # Ensure file path is a full URL if it exists
            if file_path and not file_path.startswith('http') and not file_path.startswith('/'):
                if 'support_files' in file_path:
                    file_path = f"/static/uploads/support_files/{file_path.split('/')[-1]}"
                elif 'profile_pictures' in file_path:
                    file_path = f"/static/uploads/profile_pictures/{file_path.split('/')[-1]}"
            
            messages.append({
                'id': row[0],
                'sender_type': row[1],
                'message': row[2],
                'message_type': row[3],
                'file_path': file_path,
                'timestamp': row[5].isoformat() if row[5] else '',
                'is_read': row[6],
                'is_admin': row[1] == 'admin'
            })
        
        # Mark messages as read (admin viewed them)
        cur.execute("""
            UPDATE support_messages 
            SET is_read = TRUE 
            WHERE user_id = %s 
            AND sender_type = 'user'
            AND is_read = FALSE
        """, (user_id,))
        
        # Reset user's unread count
        cur.execute("""
            UPDATE users 
            SET unread_support_count = 0 
            WHERE id = %s
        """, (user_id,))
        
        conn.commit()
        
        return jsonify({
            'success': True,
            'user': user_info,
            'messages': messages
        })
        
    except Exception as e:
        logger.error(f"Error getting admin messages: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@app.route('/api/admin/support/send', methods=['POST'])
def api_admin_support_send():
    """Admin sends message to user"""
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    try:
        data = request.json
        user_id = data.get('user_id')
        message = data.get('message', '').strip()
        message_type = data.get('message_type', 'text')
        file_path = data.get('file_path')
        
        if not user_id:
            return jsonify({'success': False, 'error': 'User ID is required'}), 400
        
        if not message and message_type == 'text':
            return jsonify({'success': False, 'error': 'Message is required'}), 400
        
        # Save message
        msg_id, timestamp = save_support_message(
            user_id=user_id,
            sender_type='admin',
            message=message,
            message_type=message_type,
            file_path=file_path
        )
        
        # Prepare response data
        response_data = {
            'id': msg_id,
            'user_id': user_id,
            'sender_type': 'admin',
            'message': message,
            'message_type': message_type,
            'file_path': file_path,
            'timestamp': timestamp.isoformat(),
            'is_admin': True  # For admin's perspective
        }
        
        # Emit socket event to user
        socketio.emit('support_message_received', response_data, room=f"user_{user_id}")
        
        return jsonify({
            'success': True,
            'message': 'Message sent to user',
            'data': response_data
        })
        
    except Exception as e:
        logger.error(f"Error admin sending message: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/admin/support/send-report', methods=['POST'])
def api_admin_support_send_report():
    """Admin sends booking report to user"""
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    try:
        data = request.json
        user_id = data.get('user_id')
        booking_id = data.get('booking_id')
        report_type = data.get('report_type', 'booking_summary')
        
        if not user_id or not booking_id:
            return jsonify({'success': False, 'error': 'User ID and Booking ID required'}), 400
        
        # Generate report file (PDF)
        report_filename = generate_booking_report(user_id, booking_id, report_type)
        if not report_filename:
            return jsonify({'success': False, 'error': 'Failed to generate report'}), 500
        
        report_url = f"/static/reports/{report_filename}"
        message = f"📋 Your {report_type.replace('_', ' ')} for booking {booking_id} is ready"
        
        # Save message with report
        msg_id, timestamp = save_support_message(
            user_id=user_id,
            sender_type='admin',
            message=message,
            message_type='report',
            file_path=report_url
        )
        
        response_data = {
            'id': msg_id,
            'user_id': user_id,
            'sender_type': 'admin',
            'message': message,
            'message_type': 'report',
            'file_path': report_url,
            'booking_id': booking_id,
            'timestamp': timestamp.isoformat()
        }
        
        # Emit socket event
        socketio.emit('support_message_received', response_data, room=f"user_{user_id}")
        
        return jsonify({
            'success': True,
            'message': 'Report sent to user',
            'data': response_data
        })
        
    except Exception as e:
        logger.error(f"Error sending report: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

def generate_booking_report(user_id, booking_id, report_type):
    """Generate booking report PDF"""
    try:
        # Get booking details
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT service_type, details, created_at 
            FROM requests 
            WHERE booking_id = %s AND user_id = %s
        """, (booking_id, user_id))
        
        booking = cur.fetchone()
        if not booking:
            return None
        
        service_type, details, created_at = booking
        
        # Generate PDF
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib import colors
        
        # Create reports directory if not exists
        reports_dir = os.path.join('static', 'reports')
        os.makedirs(reports_dir, exist_ok=True)
        
        filename = f"report_{booking_id}_{int(time.time())}.pdf"
        filepath = os.path.join(reports_dir, filename)
        
        # Create PDF
        c = canvas.Canvas(filepath, pagesize=A4)
        width, height = A4
        
        # Title
        c.setFont("Helvetica-Bold", 18)
        c.drawString(50, height - 50, f"Booking Report: {booking_id}")
        
        # Service Type
        c.setFont("Helvetica", 12)
        c.drawString(50, height - 80, f"Service: {service_type}")
        
        # Date
        c.drawString(50, height - 100, f"Date: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Details
        c.drawString(50, height - 130, "Details:")
        y = height - 150
        
        try:
            if isinstance(details, str):
                details_obj = json.loads(details)
            else:
                details_obj = details
                
            for key, value in details_obj.items():
                if y < 100:
                    c.showPage()
                    y = height - 50
                    c.setFont("Helvetica", 10)
                
                c.drawString(70, y, f"{key}: {value}")
                y -= 20
        except:
            c.drawString(70, y, str(details))
        
        # Footer
        c.setFont("Helvetica-Oblique", 8)
        c.drawString(50, 30, f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        c.drawString(50, 20, "Concierge Lifestyle Support")
        
        c.save()
        
        return filename
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        return None
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

# ========================
# ADMIN SUPPORT CHAT - UPLOAD & CLEAR CHAT
# ========================

@app.route('/api/admin/support/upload', methods=['POST'])
@login_required
def api_admin_support_upload():
    """Admin uploads file to send to user"""
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    try:
        user_id = request.form.get('user_id')
        file = request.files.get('file')
        
        if not user_id or not file:
            return jsonify({'success': False, 'error': 'User ID and file required'}), 400
        
        # Validate file
        if not allowed_support_file(file.filename):
            return jsonify({'success': False, 'error': 'File type not allowed'}), 400
        
        # Save file
        file_url = save_support_file(file)
        if not file_url:
            return jsonify({'success': False, 'error': 'Failed to save file'}), 500
        
        # Save message with file
        message = f"File: {file.filename}"
        msg_id, timestamp = save_support_message(
            user_id=user_id,
            sender_type='admin',
            message=message,
            message_type='file',
            file_path=file_url
        )
        
        # Prepare response
        response_data = {
            'id': msg_id,
            'user_id': user_id,
            'sender_type': 'admin',
            'message': message,
            'message_type': 'file',
            'file_path': file_url,
            'timestamp': timestamp.isoformat(),
            'is_admin': True
        }
        
        # Notify user via socket
        socketio.emit('support_message_received', response_data, room=f"user_{user_id}")
        
        return jsonify({
            'success': True,
            'message': 'File sent successfully',
            'data': response_data
        })
        
    except Exception as e:
        logger.error(f"Admin file upload error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Report Generation Endpoint - Fixed SQL query
@app.route('/api/admin/generate-user-report', methods=['POST'])
@login_required
def api_admin_generate_user_report():
    """Generate comprehensive user activity report and send to user"""
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403

    try:
        data = request.json
        user_id = data.get('user_id')
        send_via = data.get('send_via', ['dashboard'])
        report_type = data.get('report_type', 'full')
        period = data.get('period', 30)

        if not user_id:
            return jsonify({'success': False, 'error': 'User ID required'}), 400

        # Get user details for notifications
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT email, full_name FROM users WHERE id = %s", (user_id,))
        user_data = cur.fetchone()
        cur.close()
        conn.close()
        
        if not user_data:
            return jsonify({'success': False, 'error': 'User not found'}), 404
            
        user_email, user_name = user_data

        # Generate comprehensive report
        report_filename = generate_user_activity_report(user_id, report_type, period)
        if not report_filename:
            return jsonify({'success': False, 'error': 'Failed to generate report'}), 500

        report_url = f"/static/reports/{report_filename}"
        
        # Save report metadata to database
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO reports (user_id, report_type, file_path, generated_at, sent_via)
                VALUES (%s, %s, %s, NOW(), %s)
                RETURNING id
            """, (user_id, report_type, report_url, ','.join(send_via)))
            report_id = cur.fetchone()[0]
            conn.commit()
        except Exception as e:
            logger.error(f"Error saving report metadata: {e}")
            # Continue even if metadata save fails
        finally:
            cur.close()
            conn.close()

        # Send to dashboard if requested
        if 'dashboard' in send_via:
            message = f"📊 Your {report_type.replace('_', ' ')} activity report is ready for review"
            
            # Save notification to support chat
            try:
                msg_id, timestamp = save_support_message(
                    user_id=user_id,
                    sender_type='admin',
                    message=message,
                    message_type='report',
                    file_path=report_url
                )

                # Prepare response data
                response_data = {
                    'id': msg_id,
                    'user_id': user_id,
                    'sender_type': 'admin',
                    'message': message,
                    'message_type': 'report',
                    'file_path': report_url,
                    'timestamp': timestamp.isoformat(),
                    'is_admin': True
                }
                
                # Emit socket event to user
                socketio.emit('support_message_received', response_data, room=f"user_{user_id}")
                
                # Also emit to admin for confirmation
                socketio.emit('admin_support_message_sent', {
                    'user_id': user_id,
                    'report_id': report_id,
                    'filename': report_filename,
                    'message': f'Report sent to user {user_name}'
                }, room='admin_support')
                
                logger.info(f"Report sent to user {user_id} dashboard: {report_filename}")
                
            except Exception as e:
                logger.error(f"Error sending report to dashboard: {e}")

        # Send via email if requested
        if 'email' in send_via and user_email:
            try:
                # Note: Email sending would require SMTP setup
                # For now, log the intention and send notification
                logger.info(f"Email report to {user_email} - Feature requires SMTP configuration")
                
                # Send notification about email
                email_message = f"📧 Your activity report has been emailed to {user_email}"
                msg_id, timestamp = save_support_message(
                    user_id=user_id,
                    sender_type='admin',
                    message=email_message,
                    message_type='info'
                )
                
                socketio.emit('support_message_received', {
                    'id': msg_id,
                    'user_id': user_id,
                    'sender_type': 'admin',
                    'message': email_message,
                    'message_type': 'info',
                    'timestamp': timestamp.isoformat()
                }, room=f"user_{user_id}")
                
            except Exception as e:
                logger.error(f"Error handling email notification: {e}")

        return jsonify({
            'success': True,
            'message': 'Report generated and sent successfully',
            'report_url': report_url,
            'sent_via': send_via,
            'user_name': user_name,
            'report_id': report_id if 'report_id' in locals() else None
        })

    except Exception as e:
        logger.error(f"Error generating user report: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

def generate_user_activity_report(user_id, report_type='full', period=30):
    """Generate comprehensive user activity report PDF"""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib import colors
        from reportlab.platypus import Table, TableStyle
        from datetime import datetime, timedelta

        conn = get_db_connection()
        cur = conn.cursor()

        # Get user details
        cur.execute("""
            SELECT id, username, email, full_name, phone, created_at
            FROM users WHERE id = %s
        """, (user_id,))
        user = cur.fetchone()

        if not user:
            return None

        user_id_db, username, email, full_name, phone, created_at = user

        # Get user requests (period-based if not 'all')
        if period == 'all':
            cur.execute("""
                SELECT booking_id, service_type, details, created_at
                FROM requests WHERE user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))
        else:
            period_date = datetime.now() - timedelta(days=int(period))
            cur.execute("""
                SELECT booking_id, service_type, details, created_at
                FROM requests WHERE user_id = %s AND created_at >= %s
                ORDER BY created_at DESC
            """, (user_id, period_date))

        requests = cur.fetchall()

        # Get lifestyle profile if exists
        cur.execute("""
            SELECT monthly_budget, lifestyle_type, travel_frequency, preferred_services
            FROM lifestyle_profiles WHERE user_id = %s
        """, (user_id,))
        profile = cur.fetchone()

        cur.close()
        conn.close()

        # Create reports directory
        reports_dir = os.path.join('static', 'reports')
        os.makedirs(reports_dir, exist_ok=True)

        filename = f"user_report_{user_id}_{int(time.time())}.pdf"
        filepath = os.path.join(reports_dir, filename)

        # Create PDF
        c = canvas.Canvas(filepath, pagesize=A4)
        width, height = A4

        # Gold color theme
        gold = colors.HexColor('#d4af37')
        dark = colors.HexColor('#1e293b')

        # Header with gold background
        c.setFillColor(gold)
        c.rect(0, height - 80, width, 80, fill=True, stroke=False)

        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 24)
        c.drawCentredString(width / 2, height - 40, "CONCIERGE LIFESTYLE")
        c.setFont("Helvetica", 14)
        c.drawCentredString(width / 2, height - 60, "User Activity Report")

        # Reset color
        c.setFillColor(colors.black)

        # Report date
        y_pos = height - 100
        c.setFont("Helvetica", 10)
        c.setFillColor(colors.grey)
        c.drawRightString(width - 50, y_pos, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

        y_pos -= 30

        # User Information Section
        c.setFillColor(gold)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y_pos, "User Information")

        y_pos -= 5
        c.setStrokeColor(gold)
        c.setLineWidth(2)
        c.line(50, y_pos, width - 50, y_pos)

        y_pos -= 20
        c.setFillColor(colors.black)
        c.setFont("Helvetica", 11)

        info_data = [
            f"Name: {full_name or 'N/A'}",
            f"Username: {username}",
            f"Email: {email or 'N/A'}",
            f"Phone: {phone or 'Not provided'}",
            f"Member Since: {created_at.strftime('%Y-%m-%d') if created_at else 'N/A'}"
        ]

        for info in info_data:
            c.drawString(50, y_pos, info)
            y_pos -= 18

        y_pos -= 10

        # Lifestyle Profile Section (if exists)
        if profile:
            c.setFillColor(gold)
            c.setFont("Helvetica-Bold", 14)
            c.drawString(50, y_pos, "Lifestyle Profile")

            y_pos -= 5
            c.setStrokeColor(gold)
            c.line(50, y_pos, width - 50, y_pos)

            y_pos -= 20
            c.setFillColor(colors.black)
            c.setFont("Helvetica", 11)

            monthly_budget, lifestyle_type, travel_freq, preferred_services = profile
            profile_data = [
                f"Budget: {monthly_budget or 'Not set'}",
                f"Lifestyle: {lifestyle_type or 'Not set'}",
                f"Travel Frequency: {travel_freq or 'Not set'}",
                f"Preferred Services: {preferred_services or 'Not set'}"
            ]

            for info in profile_data:
                c.drawString(50, y_pos, info)
                y_pos -= 18

            y_pos -= 10

        # Activity Summary Section
        c.setFillColor(gold)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y_pos, f"Activity Summary (Last {period} days)" if period != 'all' else "Activity Summary (All Time)")

        y_pos -= 5
        c.setStrokeColor(gold)
        c.line(50, y_pos, width - 50, y_pos)

        y_pos -= 20
        c.setFillColor(colors.black)
        c.setFont("Helvetica", 11)

        # Count by service type
        service_counts = {}
        for req in requests:
            service_type = req[1]
            service_counts[service_type] = service_counts.get(service_type, 0) + 1

        c.drawString(50, y_pos, f"Total Requests: {len(requests)}")
        y_pos -= 18

        for service, count in service_counts.items():
            c.drawString(70, y_pos, f"• {service}: {count}")
            y_pos -= 18

        y_pos -= 15

        # Recent Requests Table (if space allows)
        if y_pos > 150 and requests:
            c.setFillColor(gold)
            c.setFont("Helvetica-Bold", 14)
            c.drawString(50, y_pos, "Recent Requests")

            y_pos -= 5
            c.setStrokeColor(gold)
            c.line(50, y_pos, width - 50, y_pos)

            y_pos -= 20
            c.setFillColor(colors.black)
            c.setFont("Helvetica", 9)

            # Show up to 5 recent requests
            for i, req in enumerate(requests[:5]):
                if y_pos < 100:
                    break
                booking_id, service_type, details, req_created = req
                c.drawString(50, y_pos, f"#{booking_id} - {service_type}")
                c.drawString(300, y_pos, 'Completed')
                c.drawString(400, y_pos, req_created.strftime('%Y-%m-%d') if req_created else 'N/A')
                y_pos -= 15

        # Footer
        c.setFillColor(colors.grey)
        c.setFont("Helvetica-Oblique", 9)
        c.drawCentredString(width / 2, 30, "Concierge Lifestyle - Premium Services")
        c.drawCentredString(width / 2, 20, "This is an automated report generated by the admin panel")

        c.save()

        logger.info(f"Generated user report: {filename}")
        return filename

    except Exception as e:
        logger.error(f"Error generating user activity report: {e}")
        import traceback
        traceback.print_exc()
        return None

@app.route('/api/admin/support/clear-chat/<int:user_id>', methods=['DELETE'])
@login_required
def api_admin_clear_chat(user_id):
    """Admin clears chat history with user"""
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Delete chat messages
        cur.execute("DELETE FROM support_messages WHERE user_id = %s", (user_id,))
        
        # Reset unread count
        cur.execute("UPDATE users SET unread_support_count = 0 WHERE id = %s", (user_id,))
        
        conn.commit()
        
        return jsonify({'success': True, 'message': 'Chat cleared successfully'})
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Clear chat error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

# ---------------------- SOCKET.IO HANDLERS FOR SUPPORT CHAT ----------------------

@socketio.on('support_message')
def handle_support_message(data):
    """Handle real-time support chat messages via WebSocket"""
    try:
        user_id = data.get('user_id')
        message = data.get('message', '').strip()
        sender_type = data.get('sender_type', 'user')
        
        if not user_id or not message:
            return
        
        # Validate sender
        if sender_type == 'admin' and not session.get('is_admin'):
            return
        
        # Save message
        msg_id, timestamp = save_support_message(
            user_id=user_id,
            sender_type=sender_type,
            message=message,
            message_type='text'
        )
        
        # Prepare response
        response_data = {
            'id': msg_id,
            'user_id': user_id,
            'sender_type': sender_type,
            'message': message,
            'timestamp': timestamp.isoformat(),
            'is_me': sender_type == 'user'  # For user's perspective
        }
        
        # Emit to appropriate recipients
        if sender_type == 'user':
            # User sent message: notify admin
            socketio.emit('support_message_received', response_data, room='admin_support')
            # Also send back to user for confirmation (if they use socket to send)
            socketio.emit('support_message_sent', response_data, room=f"user_{user_id}")
        else:
            # Admin sent message: notify user
            socketio.emit('support_message_received', response_data, room=f"user_{user_id}")
        
    except Exception as e:
        logger.error(f"Socket support message error: {e}")

@app.route('/api/user/reports', methods=['GET'])
@login_required
def api_user_reports():
    """Get all reports for the current user"""
    user_id = current_user.get_id()
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, report_type, file_path, generated_at, sent_via
            FROM reports 
            WHERE user_id = %s
            ORDER BY generated_at DESC
        """, (user_id,))
        
        reports = []
        rows = cur.fetchall()
        for row in rows:
            reports.append({
                'id': row[0],
                'report_type': row[1],
                'file_path': row[2],
                'generated_at': row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else 'N/A',
                'sent_via': row[4] or 'dashboard'
            })
        
        return jsonify({'success': True, 'reports': reports})
    except Exception as e:
        logger.error(f"Error fetching user reports: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/download-report/<int:report_id>')
@login_required
def download_report(report_id):
    """Download a specific report"""
    user_id = current_user.get_id()
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT file_path FROM reports 
            WHERE id = %s AND user_id = %s
        """, (report_id, user_id))
        
        result = cur.fetchone()
        if not result:
            flash("Report not found or unauthorized", "error")
            return redirect(url_for('dashboard'))
        
        file_path = result[0]
        # Remove /static/ prefix if present
        if file_path.startswith('/static/'):
            file_path = file_path[8:]  # Remove '/static/'
        
        # Full path to file
        reports_dir = os.path.join('static', 'reports')
        full_path = os.path.join(reports_dir, os.path.basename(file_path))
        
        if not os.path.exists(full_path):
            flash("Report file not found", "error")
            return redirect(url_for('dashboard'))
        
        return send_file(full_path, as_attachment=True)
        
    except Exception as e:
        logger.error(f"Error downloading report: {e}")
        flash("Error downloading report", "error")
        return redirect(url_for('dashboard'))
    finally:
        cur.close()
        conn.close()

@app.route('/api/admin/report-history')
@login_required
def api_admin_report_history():
    """Get report generation history for admin"""
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT r.id, r.user_id, u.full_name, r.report_type, 
                   r.file_path, r.generated_at, r.sent_via
            FROM reports r
            JOIN users u ON r.user_id = u.id
            ORDER BY r.generated_at DESC
            LIMIT 50
        """)
        
        reports = []
        rows = cur.fetchall()
        for row in rows:
            reports.append({
                'id': row[0],
                'user_id': row[1],
                'user_name': row[2] or f"User #{row[1]}",
                'report_type': row[3],
                'file_path': row[4],
                'generated_at': row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] else 'N/A',
                'sent_via': row[6] or 'dashboard'
            })
        
        # Get stats
        cur.execute("SELECT COUNT(*) FROM reports")
        total_reports = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM reports WHERE sent_via LIKE '%email%'")
        emailed_reports = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM reports WHERE sent_via LIKE '%dashboard%'")
        dashboard_reports = cur.fetchone()[0]
        
        cur.execute("SELECT MAX(generated_at) FROM reports")
        last_report_time = cur.fetchone()[0]
        
        return jsonify({
            'success': True,
            'reports': reports,
            'stats': {
                'total_reports': total_reports,
                'emailed_reports': emailed_reports,
                'dashboard_reports': dashboard_reports,
                'last_report_time': last_report_time.strftime('%Y-%m-%d %H:%M:%S') if last_report_time else None
            }
        })
    except Exception as e:
        logger.error(f"Error fetching report history: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@socketio.on('support_typing')
def handle_support_typing(data):
    """Handle typing indicators"""
    try:
        user_id = data.get('user_id')
        is_typing = data.get('is_typing', False)
        sender_type = data.get('sender_type', 'user')
        
        if sender_type == 'user':
            # User typing, notify admin
            socketio.emit('support_user_typing', {
                'user_id': user_id,
                'is_typing': is_typing
            }, room='admin_support')
        elif sender_type == 'admin':
            if not session.get('is_admin'):
                return
            # Admin typing, notify user
            socketio.emit('support_admin_typing', {
                'is_typing': is_typing
            }, room=f"user_{user_id}")
            
    except Exception as e:
        logger.error(f"Socket typing error: {e}")

@socketio.on('support_mark_read')
def handle_support_mark_read(data):
    """Mark messages as read"""
    try:
        user_id = data.get('user_id')
        if user_id:
            mark_messages_read(user_id)
            socketio.emit('support_messages_read', {
                'user_id': user_id,
                'timestamp': datetime.now().isoformat()
            }, room=f"user_{user_id}")
    except Exception as e:
        logger.error(f"Socket mark read error: {e}")

# ========================
# NEW SOCKET EVENTS FOR ADMIN SUPPORT CHAT
# ========================

@socketio.on('admin_support_message')
def handle_admin_support_message(data):
    """Handle admin support messages via WebSocket"""
    try:
        user_id = data.get('user_id')
        message = data.get('message', '').strip()
        
        if not user_id or not message:
            return
        
        # Save message
        msg_id, timestamp = save_support_message(
            user_id=user_id,
            sender_type='admin',
            message=message,
            message_type='text'
        )
        
        # Prepare response

        response_data = {
            'id': msg_id,
            'user_id': user_id,
            'sender_type': 'admin',
            'message': message,
            'timestamp': timestamp.isoformat(),
            'is_admin': True
        }
        
        # Emit to user
        socketio.emit('support_message_received', response_data, room=f"user_{user_id}")
        
        # Also send back to admin for confirmation
        emit('support_message_sent', response_data)
        
    except Exception as e:
        logger.error(f"Admin support message error: {e}")

@socketio.on('admin_support_file')
def handle_admin_support_file(data):
    """Handle admin file sharing via WebSocket"""
    try:
        user_id = data.get('user_id')
        file_url = data.get('file_url')
        file_name = data.get('file_name')
        
        if not user_id or not file_url:
            return
        
        message = f"File shared: {file_name}"
        
        # Save message with file
        msg_id, timestamp = save_support_message(
            user_id=user_id,
            sender_type='admin',
            message=message,
            message_type='file',
            file_path=file_url
        )
        
        # Prepare response
        response_data = {
            'id': msg_id,
            'user_id': user_id,
            'sender_type': 'admin',
            'message': message,
            'message_type': 'file',
            'file_path': file_url,
            'timestamp': timestamp.isoformat(),
            'is_admin': True
        }
        
        # Emit to user
        socketio.emit('support_message_received', response_data, room=f"user_{user_id}")
        
    except Exception as e:
        logger.error(f"Admin support file error: {e}")
        
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

# ---------------------- Helper Functions ----------------------
def get_tomorrow_date():
    tomorrow = datetime.now() + timedelta(days=1)
    return tomorrow.strftime('%Y-%m-%d')

def get_day_after_tomorrow():
    day_after = datetime.now() + timedelta(days=2)
    return day_after.strftime('%Y-%m-%d')

def get_in_7_days():
    """Return date string for 7 days from today (YYYY-MM-DD)"""
    today = datetime.now()
    future = today + timedelta(days=7)
    return future.strftime('%Y-%m-%d')

def get_default_service_time():
    # Default time: 2 PM tomorrow
    return "14:00"

def get_default_pickup_time():
    # Default pickup time: 10 AM tomorrow
    tomorrow = datetime.now() + timedelta(days=1)
    return tomorrow.replace(hour=10, minute=0, second=0, microsecond=0).strftime('%H:%M')

def get_tomorrow_time(hours_from_now=2):
    tomorrow = datetime.now() + timedelta(days=1)
    tomorrow = tomorrow.replace(hour=hours_from_now, minute=0, second=0, microsecond=0)
    return tomorrow.strftime('%H:%M')

# ---------------------- Notification Functions ----------------------
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

# ---------------------- Context Processor ----------------------
@app.context_processor
def inject_common_variables():
    """Inject common variables into all templates automatically"""
    if current_user.is_authenticated:
        user_id = current_user.get_id()
        
        support_unread_count = get_user_unread_count(user_id)

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
                SELECT full_name, email, phone, address, whatsapp, instagram, facebook, profile_picture
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
                    'facebook': contact_data[6] or '',
                    'profile_picture': contact_data[7] or ''
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
            'request_count': len(requests),
            'support_unread_count': support_unread_count
        }
    
    # Not authenticated
    return {
        'current_user_id': None,
        'user': 'Guest',
        'unread_count': 0,
        'notifications': [],
        'has_lifestyle_profile': False,
        'contact': {},
        'support_unread_count': 0,
        'requests': [],
        'request_count': 0
    }

# ---------------------- Lifestyle Profile Routes ----------------------
@app.route('/lifestyle_form')
@login_required
def lifestyle_form():
    """Display the lifestyle form with existing profile data"""
    user_id = current_user.get_id()
    
    # Get comprehensive profile
    profile = get_user_profile(user_id)
    
    # If profile exists, parse interests and preferred_services
    if profile:
        # Parse interests
        interests_raw = profile.get('interests', '')
        if interests_raw:
            profile['interests_list'] = interests_raw.split(',') if isinstance(interests_raw, str) else interests_raw
        
        # Parse preferred_services
        services_raw = profile.get('preferred_services', '')
        if services_raw:
            profile['preferred_services_list'] = services_raw.split(',') if isinstance(services_raw, str) else services_raw
    
    return render_template('lifestyle_form.html', profile=profile)

@app.route('/save_lifestyle', methods=['POST'])
@login_required
def save_lifestyle():
    """Save lifestyle data to database with all new fields"""
    try:
        user_id = current_user.get_id()
        
        # Collect all form data
        interests = request.form.getlist('interests')
        preferred_services = request.form.getlist('preferred_services')
        
        # Basic info
        age_group = request.form.get('age_group', '')
        profession = request.form.get('profession', '')
        monthly_budget = request.form.get('monthly_budget', 'medium')
        lifestyle_type = request.form.get('lifestyle_type', 'comfort')
        
        # Travel preferences
        travel_frequency = request.form.get('travel_frequency', 'monthly')
        travel_style = request.form.get('travel_style', 'comfort')
        typical_group_size = request.form.get('typical_group_size', 1, type=int)
        preferred_cab_type = request.form.get('preferred_cab_type', 'sedan')
        dietary_pref = request.form.get('dietary_pref', 'none')
        
        # Location
        city = request.form.get('city', '')
        area = request.form.get('area', '')
        latitude = request.form.get('latitude', '')
        longitude = request.form.get('longitude', '')
        
        # Home owner (convert string to boolean)
        home_owner_str = request.form.get('home_owner', 'no')
        home_owner = home_owner_str == 'yes'
        
        # Convert lists to strings
        interests_str = ','.join(interests) if interests else ''
        preferred_services_str = ','.join(preferred_services) if preferred_services else ''
        
        # Save to database using your existing function (update it to handle new fields)
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            # First check if profile exists
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
                    age_group, profession, monthly_budget, lifestyle_type,
                    travel_frequency, travel_style, typical_group_size, preferred_cab_type,
                    dietary_pref, city, area, latitude, longitude, home_owner,
                    interests_str, preferred_services_str, user_id
                ))
            else:
                # Insert new profile
                cur.execute("""
                    INSERT INTO lifestyle_profiles (
                        user_id, age_group, profession, monthly_budget, lifestyle_type,
                        travel_frequency, travel_style, typical_group_size, preferred_cab_type,
                        dietary_pref, city, area, latitude, longitude, home_owner,
                        interests, preferred_services, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    user_id, age_group, profession, monthly_budget, lifestyle_type,
                    travel_frequency, travel_style, typical_group_size, preferred_cab_type,
                    dietary_pref, city, area, latitude, longitude, home_owner,
                    interests_str, preferred_services_str
                ))
            
            # Clear existing AI recommendations to force regeneration
            cur.execute("DELETE FROM ai_recommendations WHERE user_id = %s", (user_id,))
            
            conn.commit()
            
            # Check if AJAX request
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.args.get('ajax') == '1':
                return jsonify({
                    'success': True,
                    'message': 'Lifestyle profile saved successfully!'
                })

            flash('✅ Your lifestyle profile has been saved! You will now get personalized AI suggestions.', 'success')
            
        except Exception as e:
            conn.rollback()
            print(f"Database error: {e}")
            flash('❌ Error saving profile. Please try again.', 'error')
        finally:
            cur.close()
            conn.close()
            
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
        
        # Add Passenger List if available
        if details.get('passengers_details'):
            story.append(Spacer(1, 20))
            story.append(Paragraph("Passenger List", header_style))
            story.append(Spacer(1, 10))
            
            pax_data = [["Name", "Age", "Gender"]]
            for pax in details['passengers_details']:
                pax_data.append([
                    pax.get('name', '-'),
                    str(pax.get('age', '-')),
                    pax.get('gender', '-')
                ])
                
            pax_table = Table(pax_data, colWidths=[250, 100, 150])
            pax_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498db')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
            ]))
            story.append(pax_table)
        
    elif service_type == 'Hotel Booking':
        hotel_data = [
            ["Hotel Name:", details.get('hotel_name', 'N/A')],
            ["Check-in Date:", details.get('checkin', 'N/A')],
            ["Check-out Date:", details.get('checkout', 'N/A')],
            ["Rooms:", str(details.get('rooms', 1))],
            ["Guests:", str(details.get('guests', 1))],
            ["Contact Email:", details.get('email', 'N/A')],
            ["Contact Mobile:", details.get('mobile', 'N/A')],
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
        
        # Add Guest List if available
        if details.get('guest_details'):
            story.append(Spacer(1, 20))
            story.append(Paragraph("Guest List", header_style))
            story.append(Spacer(1, 10))
            
            guest_list_data = [["Room", "Guest Name", "Type"]]
            for guest in details['guest_details']:
                guest_list_data.append([
                    f"Room {guest.get('room', '-')}",
                    f"{guest.get('title', '')} {guest.get('name', 'Guest')}",
                    guest.get('type', '-')
                ])
                
            guest_table = Table(guest_list_data, colWidths=[80, 320, 100])
            guest_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#7f8c8d')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
            ]))
            story.append(guest_table)
        
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
        if details.get('traveller_details'):
            story.append(Spacer(1, 20))
            story.append(Paragraph("Traveller List", header_style))
            story.append(Spacer(1, 10))
            
            pax_data = [["Title", "Full Name"]]
            for pax in details['traveller_details']:
                pax_data.append([
                    pax.get('title', '-'),
                    pax.get('full_name', '-')
                ])
                
            pax_table = Table(pax_data, colWidths=[100, 300])
            pax_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9b59b6')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
            ]))
            story.append(pax_table)
            
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

        # Add Customer Details if available
        if details.get('customer_name'):
            story.append(Spacer(1, 20))
            story.append(Paragraph("Customer Details", header_style))
            story.append(Spacer(1, 10))
            
            cust_data = [
                ["Customer Name:", details.get('customer_name', 'N/A')],
                ["Customer Address:", details.get('customer_address', 'N/A')],
                ["Primary Contact:", details.get('mobile', 'N/A')],
                ["Alternate Contact:", details.get('alternate_phone', 'N/A')],
                ["Email:", details.get('email', 'N/A')]
            ]
            
            cust_table = Table(cust_data, colWidths=[200, 300])
            cust_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#d35400')),
                ('TEXTCOLOR', (0, 0), (0, -1), colors.white),
                ('BACKGROUND', (1, 0), (1, -1), colors.HexColor('#fdebd0')),
                ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#2c3e50')),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7'))
            ]))
            story.append(cust_table)
        
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

        # Add Sender/Receiver Info if available
        if details.get('sender') and details.get('receiver'):
            story.append(Spacer(1, 20))
            story.append(Paragraph("Shipping Details", header_style))
            story.append(Spacer(1, 10))
            
            sender = details['sender']
            receiver = details['receiver']
            
            shipping_data = [
                ["Sender", "Receiver"],
                [f"Name: {sender.get('name', '-')}", f"Name: {receiver.get('name', '-')}"],
                [f"Phone: {sender.get('phone', '-')}", f"Phone: {receiver.get('phone', '-')}"],
                [f"Address: {sender.get('full_address', '-')}", f"Address: {receiver.get('full_address', '-')}"]
            ]
            
            ship_table = Table(shipping_data, colWidths=[250, 250])
            ship_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#16a085')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7')),
                ('VALIGN', (0, 0), (-1, -1), 'TOP')
            ]))
            story.append(ship_table)
    
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
# ---------------------- Static Mock Data ----------------------
# Central coordinates for mapping
CITY_COORDINATES = {
    "Mumbai": {"lat": 19.0760, "lng": 72.8777},
    "Pune": {"lat": 18.5204, "lng": 73.8567},
    "Nashik": {"lat": 19.9975, "lng": 73.7898},
    "Delhi": {"lat": 28.6139, "lng": 77.2090},
    "Bangalore": {"lat": 12.9716, "lng": 77.5946},
    "Hyderabad": {"lat": 17.3850, "lng": 78.4867},
    "Chennai": {"lat": 13.0827, "lng": 80.2707},
    "Kolkata": {"lat": 22.5726, "lng": 88.3639},
    "Jaipur": {"lat": 26.9124, "lng": 75.7873},
    "Goa": {"lat": 15.2993, "lng": 74.1240},
    "Ahmedabad": {"lat": 23.0225, "lng": 72.5714},
    "Chandigarh": {"lat": 30.7333, "lng": 76.7794},
    "Lucknow": {"lat": 26.8467, "lng": 80.9462},
    "Indore": {"lat": 22.7196, "lng": 75.8577},
    "Kerala": {"lat": 10.8505, "lng": 76.2711}
}

hotels_data = {
    "Mumbai": [
        {"name": "The Taj Mahal Palace", "address": "Apollo Bunder Road, Colaba, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.7, "price": 18425, "image": "images/mumbai/mumbai1.jpg", "lat": 18.9217, "lng": 72.8333},
        {"name": "The Oberoi, Mumbai", "address": "Nariman Point, Marine Drive, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.9, "price": 11904, "image": "images/mumbai/mumbai2.jpg", "lat": 18.9272, "lng": 72.8206},
        {"name": "Trident Nariman Point", "address": "Nariman Point, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": False, "rating": 4.6, "price": 9440, "image": "images/mumbai/mumbai3.jpg", "lat": 18.9286, "lng": 72.8213},
        {"name": "ITC Grand Central, Mumbai", "address": "Dr Babasaheb Ambedkar Road, Parel, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.5, "price": 8800, "image": "images/mumbai/mumbai4.jpg", "lat": 18.9996, "lng": 72.8402},
        {"name": "Sahara Star Hotel", "address": "Opposite Domestic Airport, Vile Parle (E), Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.4, "price": 10200, "image": "images/mumbai/mumbai5.jpg", "lat": 19.0948, "lng": 72.8541},
        {"name": "Hotel Marine Plaza", "address": "Marine Drive, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": False, "spa": False, "rating": 4.3, "price": 7900, "image": "images/mumbai/mumbai6.jpg", "lat": 18.9345, "lng": 72.8242},
        {"name": "Novotel Mumbai Juhu Beach", "address": "Juhu Beach, Balraj Sahani Marg, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.2, "price": 8400, "image": "images/mumbai/mumbai7.jpg", "lat": 19.1026, "lng": 72.8252},
        {"name": "Four Seasons Hotel Mumbai", "address": "Dr E Moses Road, Worli, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.5, "price": 12000, "image": "images/mumbai/mumbai8.jpg", "lat": 18.9964, "lng": 72.8202},
        {"name": "Hotel Sea Princess", "address": "Juhu Tara Road, Juhu Beach, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.1, "price": 6700, "image": "images/mumbai/mumbai9.jpg", "lat": 19.0967, "lng": 72.8267},
        {"name": "The St. Regis Mumbai", "address": "462 Senapati Bapat Marg, Lower Parel, Mumbai", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.8, "price": 14500, "image": "images/mumbai/mumbai10.jpg", "lat": 18.9932, "lng": 72.8239}
    ],
    "Pune": [
        {"name": "JW Marriott Hotel Pune", "address": "Senapati Bapat Road, Pune", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.6, "price": 9800, "image": "images/pune/pune1.jpg", "lat": 18.5309, "lng": 73.8334},
        {"name": "Conrad Pune", "address": "7 Mangaldas Road, Pune", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.7, "price": 8900, "image": "images/pune/pune2.jpg", "lat": 18.5367, "lng": 73.8812},
        {"name": "Hyatt Regency Pune", "address": "Weikfield IT Park, Pune Nagar Road, Pune", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.5, "price": 7500, "image": "images/pune/pune3.jpg", "lat": 18.5630, "lng": 73.9080},
        {"name": "The Westin Pune Koregaon Park", "address": "Koregaon Park, Pune", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.6, "price": 8200, "image": "images/pune/pune4.jpg", "lat": 18.5393, "lng": 73.8990},
        {"name": "Marriott Suites Pune", "address": "Koregaon Park, Pune", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.6, "price": 8500, "image": "images/pune/pune5.jpg", "lat": 18.5416, "lng": 73.9032},
        {"name": "Radisson Blu Hotel Pune Kharadi", "address": "Kharadi, Pune", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.4, "price": 6800, "image": "images/pune/pune6.jpg", "lat": 18.5529, "lng": 73.9357}
    ],
    "Nashik": [
        {"name": "Express Inn Nashik", "address": "Pathardi Phata, Mumbai-Agra Road, Nashik", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.4, "price": 5500, "image": "images/nashik/nashik1.jpg", "lat": 19.9679, "lng": 73.7686},
        {"name": "The Gateway Hotel Ambad", "address": "Ambad, Nashik", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.5, "price": 6200, "image": "images/nashik/nashik2.jpg", "lat": 19.9575, "lng": 73.7432},
        {"name": "Courtyard by Marriott Nashik", "address": "Mumbai-Agra Highway, Nashik", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": True, "spa": True, "rating": 4.6, "price": 6800, "image": "images/nashik/nashik3.jpg", "lat": 19.9723, "lng": 73.7750},
        {"name": "Grape County Eco Resort & Spa", "address": "Anjaneri, Nashik", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": False, "spa": True, "rating": 4.5, "price": 7500, "image": "images/nashik/nashik4.jpg", "lat": 19.9392, "lng": 73.6663},
        {"name": "Regenta Resort Soma Vine Village", "address": "Gangapur-Ganghavare Road, Nashik", "couple_friendly": True, "free_wifi": True, "pool": True, "gym": False, "spa": True, "rating": 4.3, "price": 7200, "image": "images/nashik/nashik5.jpg", "lat": 20.0152, "lng": 73.6931},
        {"name": "ibis Nashik", "address": "Nashik-Trimbakeshwar Road, Satpur, Nashik", "couple_friendly": True, "free_wifi": True, "pool": False, "gym": True, "spa": False, "rating": 4.1, "price": 3200, "image": "images/nashik/nashik6.jpg", "lat": 19.9922, "lng": 73.7455}
    ]
}

# City Localities for realistic addresses
CITY_LOCALITIES = {
    "Delhi": ["Connaught Place", "Karol Bagh", "South Extension", "Vasant Vihar", "Dwarka", "Rohini", "Saket", "Nehru Place"],
    "Bangalore": ["Indiranagar", "Koramangala", "Whitefield", "Jayanagar", "MG Road", "Electronic City", "HSR Layout"],
    "Hyderabad": ["Banjara Hills", "Jubilee Hills", "Gachibowli", "Hitech City", "Begumpet", "Secunderabad"],
    "Chennai": ["T Nagar", "Adyar", "Anna Nagar", "Mylapore", "Velachery", "Nungambakkam"],
    "Kolkata": ["Park Street", "Salt Lake", "New Town", "Ballygunge", "Alipore", "Howrah"],
    "Jaipur": ["Vaishali Nagar", "Malviya Nagar", "C Scheme", "Raja Park", "Mansarovar", "Amer Road"],
    "Goa": ["Calangute", "Candolim", "Panjim", "Anjuna", "Baga", "Margao", "Vasco"],
    "Ahmedabad": ["Satellite", "Vastrapur", "Navrangpura", "Maninagar", "Bopal", "SG Highway"],
    "Chandigarh": ["Sector 17", "Sector 35", "Sector 22", "Manimajra", "Industrial Area"],
    "Lucknow": ["Gomti Nagar", "Hazratganj", "Aliganj", "Indira Nagar", "Aminabad"],
    "Indore": ["Vijay Nagar", "Palasia", "Bhawarkua", "Rajwada", "Saket Nagar"]
}

def generate_dynamic_hotels(city):
    """Generate realistic hotels for new cities using 'online' photos"""
    city_center = CITY_COORDINATES.get(city, {"lat": 20.5937, "lng": 78.9629}) # Default India center
    localities = CITY_LOCALITIES.get(city, ["City Center", "Market Road", "Station Road", "Civil Lines", "Main Street"])
    
    prefixes = ["The Grand", "Royal", "Hotel", "Hyatt", "Radisson", "Marriott", "Taj", "Sheraton", "Hilton", "Lemon Tree", "ITC", "Novotel", "Holiday Inn"]
    suffixes = ["Palace", "Residency", "Suites", "Regency", "Plaza", "Inn", "Blu", "View", "Resort", "Towers"]
    
    # Online placeholder images from Unsplash (Hotels/Resorts)
    online_images = [
        "https://images.unsplash.com/photo-1566073771259-6a8506099945?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1582719508461-905c673771fd?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1542314831-068cd1dbfeeb?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1571896349842-68c8949120bb?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1455587734955-081b22074882?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1564501049412-61c2a3083791?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1520250497591-112f2f40a3f4?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1551882547-ff40c63fe5fa?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1618773928121-c32242e63f39?auto=format&fit=crop&w=800&q=80",
        "https://images.unsplash.com/photo-1596436889106-be35e843f974?auto=format&fit=crop&w=800&q=80"
    ]
    
    dynamic_hotels = []
    num_hotels = random.randint(8, 12)
    
    for i in range(num_hotels):
        name = f"{random.choice(prefixes)} {city} {random.choice(suffixes)}"
        if "The" not in name and "Hotel" not in name: 
            name = f"The {name}"
            
        rating = round(random.uniform(3.8, 5.0), 1)
        price = random.randint(2500, 15000)
        
        # Address Generation
        locality = random.choice(localities)
        street_no = random.randint(1, 99)
        address = f"{street_no}, {locality}, {city} - {random.randint(110001, 800000)}"
        
        # Random offset for map (approx 5-10km radius)
        lat_offset = random.uniform(-0.05, 0.05)
        lng_offset = random.uniform(-0.05, 0.05)
        
        dynamic_hotels.append({
            "name": name,
            "address": address,
            "couple_friendly": random.choice([True, False]),
            "free_wifi": True,
            "pool": random.choice([True, False]),
            "gym": random.choice([True, False]),
            "spa": random.choice([True, False]),
            "rating": rating,
            "price": price,
            "image": random.choice(online_images),
            "lat": city_center["lat"] + lat_offset,
            "lng": city_center["lng"] + lng_offset
        })
        
    # Sort by rating
    dynamic_hotels.sort(key=lambda x: x['rating'], reverse=True)
    return dynamic_hotels

cars_data = [
    {"model": "Toyota Etios", "seats": 4, "luggage": 2, "fuel_type": "CNG/Petrol/Diesel", "price": 936, "cab_class": "Standard", "pickup_time": "10:00", "dropoff_time": "12:00", "duration": "2h", "status": "Available"},
    {"model": "Honda City", "seats": 4, "luggage": 3, "fuel_type": "Petrol", "price": 1200, "cab_class": "Standard", "pickup_time": "09:00", "dropoff_time": "11:30", "duration": "2h 30m", "status": "Available"},
    {"model": "Toyota Fortuner", "seats": 6, "luggage": 4, "fuel_type": "Diesel", "price": 1500, "cab_class": "SUV", "pickup_time": "11:00", "dropoff_time": "13:00", "duration": "2h", "status": "Available"},
    {"model": "BMW 5 Series", "seats": 4, "luggage": 2, "fuel_type": "Petrol", "price": 2000, "cab_class": "Luxury", "pickup_time": "08:00", "dropoff_time": "10:00", "duration": "2h", "status": "Available"},
    {"model": "Maruti Eeco", "seats": 7, "luggage": 5, "fuel_type": "CNG", "price": 1800, "cab_class": "Standard", "pickup_time": "12:00", "dropoff_time": "14:30", "duration": "2h 30m", "status": "Available"},
    {"model": "Hyundai Creta", "seats": 5, "luggage": 3, "fuel_type": "Diesel", "price": 1400, "cab_class": "SUV", "pickup_time": "10:30", "dropoff_time": "12:30", "duration": "2h", "status": "Available"},
    {"model": "Mini Cooper", "seats": 4, "luggage": 2, "fuel_type": "Petrol", "price": 1100, "cab_class": "Luxury", "pickup_time": "09:30", "dropoff_time": "11:00", "duration": "1h 30m", "status": "Available"},
    {"model": "Mercedes E-Class", "seats": 4, "luggage": 3, "fuel_type": "Diesel", "price": 1600, "cab_class": "Luxury", "pickup_time": "13:00", "dropoff_time": "15:00", "duration": "2h", "status": "Available"},
    {"model": "Mahindra XUV700", "seats": 7, "luggage": 4, "fuel_type": "Petrol", "price": 1700, "cab_class": "SUV", "pickup_time": "14:00", "dropoff_time": "16:00", "duration": "2h", "status": "Available"},
    {"model": "Maruti Alto", "seats": 4, "luggage": 1, "fuel_type": "CNG", "price": 800, "cab_class": "Standard", "pickup_time": "07:00", "dropoff_time": "09:00", "duration": "2h", "status": "Available"},
    {"model": "Range Rover", "seats": 5, "luggage": 4, "fuel_type": "Diesel", "price": 2200, "cab_class": "Luxury", "pickup_time": "15:00", "dropoff_time": "17:30", "duration": "2h 30m", "status": "Available"},
    {"model": "Toyota Innova", "seats": 8, "luggage": 6, "fuel_type": "Diesel", "price": 1900, "cab_class": "SUV", "pickup_time": "16:00", "dropoff_time": "18:00", "duration": "2h", "status": "Available"},
    {"model": "Audi A4", "seats": 4, "luggage": 2, "fuel_type": "Petrol", "price": 1300, "cab_class": "Luxury", "pickup_time": "17:00", "dropoff_time": "19:00", "duration": "2h", "status": "Available"},
    {"model": "Toyota Prius", "seats": 4, "luggage": 2, "fuel_type": "Hybrid", "price": 950, "cab_class": "Standard", "pickup_time": "18:00", "dropoff_time": "20:00", "duration": "2h", "status": "Available"},
    {"model": "Mercedes V-Class", "seats": 6, "luggage": 5, "fuel_type": "CNG", "price": 2100, "cab_class": "Luxury", "pickup_time": "19:00", "dropoff_time": "21:00", "duration": "2h", "status": "Available"}
]

technicians_data = [
    {"id": "T001", "name": "Amit Sharma", "service_type": "ac_repair", "experience": 5, "rating": 4.8, "price": 800, "availability": "Available", "location": "Mumbai"},
    {"id": "T002", "name": "Rahul Patel", "service_type": "plumbing", "experience": 7, "rating": 4.6, "price": 600, "availability": "Available", "location": "Mumbai"},
    {"id": "T003", "name": "Sanjay Kumar", "service_type": "electrical", "experience": 10, "rating": 4.9, "price": 900, "availability": "Available", "location": "Mumbai"},
    {"id": "T004", "name": "Vikram Singh", "service_type": "carpentry", "experience": 4, "rating": 4.5, "price": 700, "availability": "Available", "location": "Mumbai"},
    {"id": "T005", "name": "Deepak Yadav", "service_type": "ac_repair", "experience": 6, "rating": 4.7, "price": 850, "availability": "Available", "location": "Pune"},
    {"id": "T006", "name": "Ravi Gupta", "service_type": "plumbing", "experience": 8, "rating": 4.8, "price": 650, "availability": "Available", "location": "Pune"},
    {"id": "T007", "name": "Anil Desai", "service_type": "electrical", "experience": 12, "rating": 4.9, "price": 950, "availability": "Available", "location": "Pune"},
    {"id": "T008", "name": "Manoj Joshi", "service_type": "carpentry", "experience": 5, "rating": 4.6, "price": 720, "availability": "Available", "location": "Pune"},
    {"id": "T009", "name": "Kiran Patil", "service_type": "ac_repair", "experience": 3, "rating": 4.4, "price": 750, "availability": "Available", "location": "Nashik"},
    {"id": "T010", "name": "Suresh Nair", "service_type": "plumbing", "experience": 9, "rating": 4.7, "price": 620, "availability": "Available", "location": "Nashik"},
    {"id": "T011", "name": "Ramesh Thakur", "service_type": "electrical", "experience": 6, "rating": 4.5, "price": 880, "availability": "Available", "location": "Nashik"},
    {"id": "T012", "name": "Prakash Shah", "service_type": "carpentry", "experience": 7, "rating": 4.6, "price": 700, "availability": "Available", "location": "Nashik"},
    {"id": "T013", "name": "Vijay Mehta", "service_type": "ac_repair", "experience": 4, "rating": 4.3, "price": 780, "availability": "Available", "location": "Mumbai"},
    {"id": "T014", "name": "Sunil Reddy", "service_type": "plumbing", "experience": 10, "rating": 4.8, "price": 670, "availability": "Available", "location": "Pune"},
    {"id": "T015", "name": "Arjun Kulkarni", "service_type": "electrical", "experience": 8, "rating": 4.7, "price": 920, "availability": "Available", "location": "Nashik"},
]

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

def get_analytics_data(days=7):
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
        
        # Use parameterized query for safety, although days is int
        cur.execute(f"""
            SELECT service_type, COUNT(*) 
            FROM requests 
            WHERE created_at >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY service_type
        """)
        service_data = cur.fetchall()
        
        timeline_labels = []
        timeline_data = []
        for i in range(days - 1, -1, -1):
            date_val = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            cur.execute("SELECT COUNT(*) FROM requests WHERE DATE(created_at) = %s", (date_val,))
            count = cur.fetchone()[0]
            timeline_labels.append(date_val)
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

                    # NEW: Real-time service price and availability fluctuations
                    price_fluctuations = {
                        'Hotel Booking': random.uniform(-0.05, 0.10),
                        'Car Booking': random.uniform(0.02, 0.15),
                        'Flight Booking': random.uniform(-0.10, 0.20),
                        'Technician Booking': random.choice([0, 0, 0.10])
                    }
                    availability_status = {
                        'Hotel Booking': 'High Demand' if price_fluctuations['Hotel Booking'] > 0.05 else 'Available',
                        'Car Booking': 'Few left' if price_fluctuations['Car Booking'] > 0.10 else 'Available',
                        'Technician Booking': 'Busy' if price_fluctuations['Technician Booking'] > 0 else 'Available'
                    }
                    socketio.emit('service_price_update', {
                        'fluctuations': price_fluctuations,
                        'availability': availability_status,
                        'timestamp': datetime.now().isoformat(),
                        'message': 'Dynamic pricing and availability updated'
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
                "INSERT INTO users (full_name, email, username, password, membership_tier) VALUES (%s, %s, %s, %s, 'Silver')",
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

    # Ensure admin also joins a socket room for admin-specific events
    session['socket_room'] = 'admin_support'

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
            socketio.server.emit('update_requests', {'requests': get_requests_json()}, namespace='/')
            analytics_data = get_analytics_data()
            emit('analytics_update', {'analytics': analytics_data})
            
            active_users = get_active_users()
            emit('user_activity', {
                'active_users': active_users,
                'active_count': len(active_users)
            })
            
            # ADD THIS: Join admin room for support chat notifications
            join_room('admin_support')
            
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

        socketio.server.emit('update_requests', {'requests': get_requests_json()}, namespace='/')
        
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
        socketio.server.emit('update_requests', {'requests': get_requests_json()}, namespace='/')
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

        socketio.server.emit('request_deleted', {
            'request_id': request_id,
            'booking_id': booking_id,
            'user_id': user_id,
            'message': f'Request #{request_id} deleted successfully'
        }, namespace='/')
        
        socketio.server.emit('update_requests', {'requests': get_requests_json()}, namespace='/')

    except Exception as e:
        conn.rollback()
        emit('delete_error', {'message': str(e)})
        logger.error(f"Error deleting request {request_id}: {e}")
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
        
        socketio.server.emit('ticket_sent_to_user', {
            'user_id': user_id,
            'booking_id': booking_id,
            'ticket_pdf_url': f"/static/tickets/{pdf_filename}"
        }, namespace='/')
        
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

@app.route('/admin/analytics')
def admin_analytics():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403

    # days param is accepted for UI compatibility; current analytics are computed for recent ranges.
    _days = request.args.get('days', default=7, type=int)
    return jsonify(get_analytics_data(days=_days))


@app.route('/admin/stats')
def admin_stats():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403

    analytics = get_analytics_data() or {}
    return jsonify({
        "total_users": analytics.get("total_users", 0),
        "total_requests": analytics.get("total_requests", 0),
        "new_users_today": analytics.get("new_users_today", 0),
        "active_users_today": analytics.get("active_users_today", 0),
        "pending_requests": analytics.get("pending_requests", 0),
        "confirmed_requests": analytics.get("confirmed_requests", 0),
        "active_requests_today": analytics.get("active_requests_today", 0)
    })


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
    guest_details = data.get('guest_details', [])

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
        "guest_details": guest_details,
        "simulated_payment": True,
        "simulated_payment_at": datetime.now().isoformat()
    }

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Create ticket immediately
        user_id = current_user.get_id()
        pdf_filename = generate_pdf_ticket(booking_id, 'Hotel Booking', details_obj, user_id)
        details_obj['ticket_pdf_url'] = f"/static/tickets/{pdf_filename}"
        details_obj['ticket_generated_at'] = datetime.now().isoformat()

        cur.execute("""
            INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            RETURNING id
        """, (
            user_id,
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
            socketio.emit('new_request', {'request': last_row})

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
        # Create ticket immediately
        user_id = current_user.get_id()
        pdf_filename = generate_pdf_ticket(booking_id, 'Car Booking', details_obj, user_id)
        details_obj['ticket_pdf_url'] = f"/static/tickets/{pdf_filename}"
        details_obj['ticket_generated_at'] = datetime.now().isoformat()

        cur.execute("""
            INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            RETURNING id
        """, (
            user_id,
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
            socketio.emit('new_request', {'request': last_row})

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
@app.route('/submit_car_booking', methods=['GET', 'POST'])
@login_required
def submit_car_booking():
    try:
        # Default values (used when called via GET without parameters)
        pickup = 'Mumbai'
        dropoff = 'Mumbai Airport'
        pickup_date = get_tomorrow_date()
        pickup_time = get_tomorrow_time(10)  # 10:00
        passengers = 2
        car_class = 'standard'
        special_requests = ''

        # ── Determine source of input ───────────────────────────────
        if request.method == 'GET':
            # Pre-filled from AI recommendations (URL parameters)
            location_text = session.get('user_location', {}).get('city', 'Mumbai')
            pickup = request.args.get('pickup', location_text).strip().title()
            dropoff = request.args.get('dropoff', f'{pickup} Airport').strip().title()
            pickup_date = request.args.get('pickup_date', get_tomorrow_date()).strip()
            pickup_time = request.args.get('pickup_time', get_tomorrow_time(10)).strip()
            try:
                passengers = max(1, min(9, int(request.args.get('passengers', 2))))
            except (ValueError, TypeError):
                passengers = 2
            car_class = request.args.get('cab_class', 'standard').strip().lower()
            special_requests = request.args.get('special_requests', '').strip()

        else:  # POST from modal form
            pickup = request.form.get('pickup', '').strip().title()
            dropoff = request.form.get('dropoff', '').strip().title()
            pickup_date = request.form.get('pickup_date', '').strip()
            pickup_time = request.form.get('pickup_time', '').strip()
            passengers_str = request.form.get('passengers', '1').strip()
            car_class = request.form.get('cab_class', 'standard').strip().lower()
            special_requests = request.form.get('special_requests', '').strip()

            try:
                passengers = int(passengers_str)
                if not 1 <= passengers <= 9:
                    raise ValueError
            except (ValueError, TypeError):
                flash("Passengers must be between 1 and 9.", "danger")
                return redirect(url_for('dashboard'))

        # ── Basic validation ────────────────────────────────────────
        if not all([pickup, dropoff, pickup_date, pickup_time]):
            flash("Please fill all required fields.", "danger")
            return redirect(url_for('dashboard'))

        # Parse pickup date + time
        if ':' in pickup_time:
            if len(pickup_time.split(':')) == 3:
                time_format = '%H:%M:%S'
            else:
                time_format = '%H:%M'
        else:
            flash("Invalid time format.", "danger")
            return redirect(url_for('dashboard'))

        try:
            pickup_dt = datetime.strptime(f"{pickup_date} {pickup_time}", f'%Y-%m-%d {time_format}')
            if pickup_dt < datetime.now():
                flash("Pickup time cannot be in the past.", "danger")
                return redirect(url_for('dashboard'))
        except ValueError as e:
            print("Date/time parse error:", e)
            flash("Invalid date or time format.", "danger")
            return redirect(url_for('dashboard'))

        # ── Map cab class ───────────────────────────────────────────
        class_map = {
            'economy': 'Standard',
            'standard': 'Standard',
            'comfort': 'Standard',
            'premium': 'SUV',
            'luxury': 'Luxury',
            'suv': 'SUV'
        }
        target_class = class_map.get(car_class, 'Standard')

        # Get coordinates
        pickup_coords = CITY_COORDINATES.get(pickup, CITY_COORDINATES.get('Mumbai'))
        if not pickup_coords: pickup_coords = {"lat": 19.0760, "lng": 72.8777}
        
        # Determine Dropoff Coordinates
        # 1. Check if dropoff is a known city
        dropoff_clean = dropoff.split(',')[0].strip().title() # Handle "Delhi, India" etc
        dropoff_coords = CITY_COORDINATES.get(dropoff_clean)
        
        # 2. If known city (Inter-city trip)
        if dropoff_coords:
            # Calculate Haversine distance for long trips
            lat1, lon1 = pickup_coords["lat"], pickup_coords["lng"]
            lat2, lon2 = dropoff_coords["lat"], dropoff_coords["lng"]
            R = 6371 # Earth radius in km
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            distance_km = R * c
            distance_km = round(distance_km, 1)
            
        else:
            # 3. Local trip (Random offset)
            dropoff_coords = {
                "lat": pickup_coords["lat"] + random.uniform(-0.05, 0.05),
                "lng": pickup_coords["lng"] + random.uniform(-0.05, 0.05)
            }
            # Euclidean approx for local
            dist_lat = (pickup_coords["lat"] - dropoff_coords["lat"]) * 111
            dist_lng = (pickup_coords["lng"] - dropoff_coords["lng"]) * 111
            distance_km = math.sqrt(dist_lat**2 + dist_lng**2)
            distance_km = round(max(5, distance_km), 1)

        # Get user profile for budget filtering
        user_id = current_user.get_id()
        profile = get_user_profile(user_id)
        
        # Generate Dynamic Cars
        enhanced_cars = []
        
        driver_names = ["Rajesh", "Suresh", "Amit", "Vikram", "Rahul", "Mohan", "Deepak", "Sanjay", "Vinod", "Arun"]
        
        # Base rates per km
        rates = {
            "Standard": 15,
            "SUV": 25,
            "Luxury": 50
        }
        
        filtered_cars = [c for c in cars_data if c.get('cab_class') == target_class or target_class == 'Standard']
        if not filtered_cars: filtered_cars = cars_data[:5]
        
        for idx, car in enumerate(filtered_cars):
            cab_class = car.get('cab_class', 'Standard')
            rate = rates.get(cab_class, 15)
            
            # Dynamic Price Calculation
            est_price = int(200 + (distance_km * rate)) # Base fare 200
            
            # Random Driver Location (near pickup)
            driver_lat = pickup_coords["lat"] + random.uniform(-0.02, 0.02)
            driver_lng = pickup_coords["lng"] + random.uniform(-0.02, 0.02)
            
            eta = random.randint(2, 15)
            
            enhanced_cars.append({
                'id': idx + 1,
                'model': car['model'],
                'cab_class': cab_class,
                'seats': car['seats'],
                'luggage': car['luggage'],
                'fuel_type': car['fuel_type'],
                'transmission': 'Automatic' if cab_class == 'Luxury' else 'Manual',
                'driver_name': random.choice(driver_names),
                'driver_rating': round(random.uniform(4.5, 5.0), 1),
                'eta': f"{eta} mins",
                'distance': f"{distance_km} km",
                'price': est_price,
                'pickup': pickup,
                'dropoff': dropoff,
                'pickup_date': pickup_date,
                'pickup_time': pickup_time,
                'lat': driver_lat,
                'lng': driver_lng
            })
            
        enhanced_cars.sort(key=lambda x: int(x['eta'].split()[0]))

        # ── Render results page ─────────────────────────────────────
        return render_template(
            'car_results.html',
            cars=enhanced_cars,
            pickup=pickup,
            dropoff=dropoff,
            pickup_coords=pickup_coords,
            dropoff_coords=dropoff_coords,
            pickup_date=pickup_date,
            pickup_time=pickup_time,
            passengers=passengers,
            car_class=target_class
        )

    except Exception as e:
        import traceback
        print("FATAL ERROR in submit_car_booking:")
        traceback.print_exc()
        flash("Something went wrong. Please try again.", "danger")
        return redirect(url_for('dashboard'))

# ---------------------- Technician Booking ----------------------
def generate_dynamic_technicians(service_type, city):
    """Generate realistic technicians for any city"""
    city_center = CITY_COORDINATES.get(city, {"lat": 19.0760, "lng": 72.8777})
    
    first_names = ["Ramesh", "Suresh", "Amit", "Vikram", "Rahul", "Mohan", "Deepak", "Sanjay", "Vinod", "Arun", "Rajesh", "Vijay", "Anil", "Sunil"]
    last_names = ["Kumar", "Sharma", "Singh", "Patel", "Yadav", "Gupta", "Verma", "Mishra", "Reddy", "Nair"]
    
    service_titles = {
        'ac_repair': ['AC Specialist', 'HVAC Expert', 'Cooling Tech'],
        'plumbing': ['Master Plumber', 'Pipe Fitter', 'Leakage Expert'],
        'electrical': ['Senior Electrician', 'Wiring Expert', 'Certified Electrician'],
        'carpentry': ['Master Carpenter', 'Woodwork Artist', 'Furniture Expert'],
        'cleaning': ['Deep Cleaning Pro', 'Hygiene Expert', 'Sanitization Spec.'],
        'pest_control': ['Pest Control Expert', 'Fumigation Tech', 'Vector Control']
    }
    
    titles = service_titles.get(service_type, ['Service Expert', 'Technician', 'Specialist'])
    
    technicians = []
    num_techs = random.randint(8, 12)
    
    for i in range(num_techs):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        experience = random.randint(2, 15)
        rating = round(random.uniform(4.2, 5.0), 1)
        jobs_done = random.randint(50, 500)
        
        # Location offset (within 5-10km)
        lat_offset = random.uniform(-0.05, 0.05)
        lng_offset = random.uniform(-0.05, 0.05)
        tech_lat = city_center["lat"] + lat_offset
        tech_lng = city_center["lng"] + lng_offset
        
        # Calculate ETA based on distance (approx)
        dist_km = math.sqrt((lat_offset*111)**2 + (lng_offset*111)**2)
        eta_mins = int(15 + (dist_km * 3)) # 3 mins per km + base
        
        # Price calculation
        base_prices = {
            'ac_repair': 500, 'plumbing': 300, 'electrical': 300,
            'carpentry': 400, 'cleaning': 800, 'pest_control': 1000
        }
        base_price = base_prices.get(service_type, 400)
        price = base_price + (experience * 20) # More exp = higher price
        price = int(round(price, -1)) # Round to nearest 10
        
        # Generate Phone Number
        phone = f"+91 {random.randint(70000, 99999)} {random.randint(10000, 99999)}"
        
        technicians.append({
            "id": f"T{random.randint(1000, 9999)}",
            "name": name,
            "title": random.choice(titles),
            "service_type": service_type.replace('_', ' ').title(),
            "experience": experience,
            "rating": rating,
            "jobs_completed": jobs_done,
            "price": price,
            "phone": phone, # Added Phone
            "availability": "Available Now" if random.random() > 0.3 else f"Available at {random.randint(9, 18)}:00",
            "location": city,
            "lat": tech_lat,
            "lng": tech_lng,
            "eta": f"{eta_mins} mins",
            "distance": f"{dist_km:.1f} km",
            "verified": random.choice([True, True, False]),
            "vaccinated": random.choice([True, True, False])
        })
        
    technicians.sort(key=lambda x: x['rating'], reverse=True)
    return technicians

@app.route('/submit-technician-booking', methods=['GET', 'POST'])
@login_required
def submit_technician_booking():
    try:
        # Default values
        service_type = 'ac_repair'
        location = 'Mumbai'
        service_date = get_tomorrow_date()
        service_time = get_default_service_time()
        urgency = 'normal'
        description = ''

        if request.method == 'GET':
            location_text = session.get('user_location', {}).get('city', 'Mumbai')
            location = request.args.get('location', location_text).strip().title()
            service_date = request.args.get('service_date', get_tomorrow_date()).strip()
            service_time = request.args.get('service_time', get_default_service_time()).strip()
            service_type = request.args.get('service_type', 'ac_repair').strip().lower()
            urgency = request.args.get('urgency', 'normal').strip().lower()
            description = request.args.get('description', '').strip()

        else:  # POST
            service_type = (request.form.get('service_type') or '').strip().lower()
            location = (request.form.get('location') or '').strip().title()
            service_date = request.form.get('service_date', '').strip()
            service_time = request.form.get('service_time', '').strip()
            urgency = request.form.get('urgency', 'normal').strip().lower()
            description = request.form.get('description', '').strip()

        # Validation
        if not all([service_type, location, service_date, service_time]):
            flash("Please provide all required technician details.", "danger")
            return redirect(url_for('dashboard'))

        # Get coordinates for map center
        normalized_location = location.split(',')[0].strip()
        city_coords = CITY_COORDINATES.get(normalized_location, CITY_COORDINATES.get('Mumbai'))
        if location not in CITY_COORDINATES:
             # Try to find a partial match or default to Mumbai but keep the label
             for key in CITY_COORDINATES:
                 if key in location:
                     city_coords = CITY_COORDINATES[key]
                     break

        # Generate Dynamic Technicians
        technicians = generate_dynamic_technicians(service_type, normalized_location)
        
        # Filter if needed (though generator handles logic)
        # Apply urgency pricing surge if needed
        if urgency == 'urgent':
            for tech in technicians:
                tech['price'] = int(tech['price'] * 1.5)
        elif urgency == 'emergency':
            for tech in technicians:
                tech['price'] = int(tech['price'] * 2.0)

        # Select display count
        selected_technicians = technicians # Show all generated

        if request.method == 'GET':
            flash(f"Found {len(selected_technicians)} experts for {service_type.replace('_', ' ').title()} near {location}", "info")

        return render_template('technician_results.html',
                               technicians=selected_technicians,
                               service_type=service_type,
                               location=location,
                               city_coords=city_coords,
                               service_date=service_date,
                               service_time=service_time,
                               urgency=urgency,
                               description=description)

    except Exception as e:
        import traceback
        print("ERROR in submit_technician_booking:")
        traceback.print_exc()
        flash("Something went wrong. Please try again.", "danger")
        return redirect(url_for('dashboard'))

@app.route('/technician/confirm', methods=['POST'])
@login_required
def confirm_technician():
    data = request.get_json(silent=True) or {}

    booking_id = f"TECH-{random.randint(1000, 9999)}"
    payload = json.dumps({
        "technician_id": data.get("technician_id"),
        "name": data.get("name"),
        "phone": data.get("technician_phone"), # Save technician phone
        "service_type": data.get("service_type"),
        "location": data.get("location"),
        "service_date": data.get("service_date"),
        "service_time": data.get("service_time"),
        "description": data.get("description"),
        "total_price": data.get("total_price"),
        "email": data.get("email"),
        "mobile": data.get("mobile"),
        "customer_name": data.get("customer_name"),
        "customer_address": data.get("customer_address"),
        "alternate_phone": data.get("alternate_phone")
    })

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Create ticket immediately
        user_id = current_user.get_id()
        pdf_filename = generate_pdf_ticket(booking_id, 'Technician Booking', json.loads(payload), user_id)
        
        # Update payload with ticket url
        details_obj = json.loads(payload)
        details_obj['ticket_pdf_url'] = f"/static/tickets/{pdf_filename}"
        details_obj['ticket_generated_at'] = datetime.now().isoformat()
        payload = json.dumps(details_obj)

        # Auto-Approve Logic (Phase 3)
        total_price = float(data.get("total_price", 0))
        urgency = data.get("urgency", "normal").lower()
        admin_status = 'Pending'
        
        if urgency != 'emergency' and total_price < 2000:
            admin_status = 'Confirmed'
            # Send notification for auto-approval
            save_notification(
                user_id=user_id,
                title="Technician Booking Approved",
                message=f"Your request {booking_id} has been automatically approved.",
                icon="check_circle",
                type="success"
            )

        cur.execute("""
            INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation, created_at)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s)
        """, (
            user_id,
            booking_id,
            'Technician Booking',
            payload,
            'Confirmed',
            admin_status,
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
                    "booking_id": booking_id,
                    "service_type": "Technician Booking",
                    "ticket_url": f"/static/tickets/{pdf_filename}"
                })
            except Exception as e:
                app.logger.exception("socketio.emit payment_confirmed failed: %s", e)

        return jsonify({"success": True, "booking_id": booking_id, "ticket_url": f"/static/tickets/{pdf_filename}"})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# ---------------------- Courier Booking ----------------------
@app.route('/submit_courier_booking', methods=['GET', 'POST'])
@login_required
def submit_courier_booking():
    try:
        # Default values
        pickup = 'Mumbai'
        dropoff = 'Mumbai Downtown'
        pickup_date = get_tomorrow_date()
        pickup_time = get_default_pickup_time()
        package_weight = 2.0
        courier_type = 'standard'
        special_requests = ''

        if request.method == 'GET':
            # Pre-filled from AI Recommendations (URL params)
            location_text = session.get('user_location', {}).get('city', 'Mumbai')
            pickup = request.args.get('pickup', location_text).strip().title()
            dropoff = request.args.get('dropoff', f'{pickup} Downtown').strip().title()
            pickup_date = request.args.get('pickup_date', get_tomorrow_date()).strip()
            pickup_time = request.args.get('pickup_time', get_default_pickup_time()).strip()
            try:
                package_weight = float(request.args.get('package_weight', 2.0))
                package_weight = max(0.1, package_weight)  # Minimum 0.1 kg
            except (ValueError, TypeError):
                package_weight = 2.0
            courier_type = request.args.get('courier_type', 'standard').strip().lower()
            special_requests = request.args.get('special_requests', '').strip()

        else:  # POST from modal form
            pickup = request.form.get('pickup', '').strip().title()
            dropoff = request.form.get('dropoff', '').strip().title()
            pickup_date = request.form.get('pickup_date', '').strip()
            pickup_time = request.form.get('pickup_time', '').strip()
            try:
                package_weight = float(request.form.get('package_weight', '2.0'))
                if package_weight < 0.1:
                    raise ValueError
            except ValueError:
                flash("Package weight must be at least 0.1 kg.", "danger")
                return redirect(url_for('dashboard'))
            courier_type = request.form.get('courier_type', 'standard').strip().lower()
            special_requests = request.form.get('special_requests', '').strip()

        # ── Basic validation ───────────────────────────────────────
        if not all([pickup, dropoff, pickup_date, pickup_time]):
            flash("Please fill all required fields.", "danger")
            return redirect(url_for('dashboard'))

        # Validate pickup date/time
        try:
            pickup_dt = datetime.strptime(f"{pickup_date} {pickup_time}", '%Y-%m-%d %H:%M')
            if pickup_dt < datetime.now():
                flash("Pickup time cannot be in the past.", "danger")
                return redirect(url_for('dashboard'))
        except ValueError:
            flash("Invalid date or time format.", "danger")
            return redirect(url_for('dashboard'))

        # ── Distance Calculation ───────────────────────────────────
        pickup_coords = CITY_COORDINATES.get(pickup.split(',')[0].strip().title())
        if not pickup_coords:
            # Try partial match or default
            for key, val in CITY_COORDINATES.items():
                if key in pickup:
                    pickup_coords = val
                    break
            if not pickup_coords: pickup_coords = CITY_COORDINATES.get('Mumbai')

        dropoff_coords = CITY_COORDINATES.get(dropoff.split(',')[0].strip().title())
        if not dropoff_coords:
            for key, val in CITY_COORDINATES.items():
                if key in dropoff:
                    dropoff_coords = val
                    break
            if not dropoff_coords: 
                # Random offset for local delivery if unknown
                dropoff_coords = {
                    "lat": pickup_coords["lat"] + random.uniform(-0.1, 0.1),
                    "lng": pickup_coords["lng"] + random.uniform(-0.1, 0.1)
                }

        # Calculate Distance (Haversine)
        R = 6371
        lat1, lon1 = math.radians(pickup_coords["lat"]), math.radians(pickup_coords["lng"])
        lat2, lon2 = math.radians(dropoff_coords["lat"]), math.radians(dropoff_coords["lng"])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance_km = round(R * c, 1)
        if distance_km < 2: distance_km = 5.0 # Min distance

        # ── Pricing logic ──────────────────────────────────────────
        services = {
            "standard":   {"rate_per_km": 5, "base": 50, "speed": "40 km/h"},
            "express":    {"rate_per_km": 12, "base": 150, "speed": "60 km/h"},
            "overnight":  {"rate_per_km": 20, "base": 300, "speed": "N/A"}
        }
        svc = services.get(courier_type, services["standard"])
        
        # ── Generate mock couriers ─────────────────────────────────
        all_couriers = []
        base_names = [
            "SwiftFly", "NinjaPost", "TurboShip", "SpeedyWing", "FlashCargo",
            "ZoomX", "RocketMail", "BlitzSend", "JetPack", "HyperCourier",
            "LightningDrop", "VortexShip", "CometCarry", "MeteorMove", "AstroPost"
        ]

        for _ in range(20):
            name = random.choice(base_names)
            
            # Duration logic
            avg_speed = int(svc["speed"].split()[0]) if svc["speed"] != "N/A" else 50
            hours_travel = distance_km / avg_speed
            hours_total = hours_travel + random.randint(2, 24) # Processing time
            
            if hours_total < 24:
                duration_str = f"{int(hours_total)} hours"
            else:
                duration_str = f"{int(hours_total/24)} days"
                
            if courier_type == 'express' and distance_km < 50:
                duration_str = f"{random.randint(60, 180)} mins"

            # Price logic: Base + (Dist * Rate) + (Weight * 10)
            final_price = svc["base"] + (distance_km * svc["rate_per_km"] * 0.5) + (package_weight * 15)
            final_price = int(round(final_price, -1))

            all_couriers.append({
                "id": f"{'EXP' if courier_type=='express' else 'COU'}-{random.randint(100, 9999)}",
                "name": name,
                "pickup": pickup,
                "dropoff": dropoff,
                "pickup_time": pickup_time,
                "dropoff_time": "By End of Day",
                "courier_type": courier_type.capitalize(),
                "max_weight": random.choice([20, 25, 30, 40, 50]),
                "rating": round(random.uniform(4.0, 5.0), 1),
                "availability": "Available",
                "duration": duration_str,
                "price": final_price,
                "distance": f"{distance_km} km",
                "vehicle": "Bike" if package_weight < 10 else "Van"
            })

        display_count = random.choice([5, 6])
        couriers = random.sample(all_couriers, display_count)
        couriers.sort(key=lambda x: x['price'])

        # ── Flash message ──────────────────────────────────────────
        if request.method == 'GET':
            flash(f"Found {len(couriers)} couriers for {distance_km}km trip", "info")
        else:
            flash("Search updated.", "info")

        return render_template(
            'courier_results.html',
            pickup=pickup,
            dropoff=dropoff,
            pickup_coords=pickup_coords,
            dropoff_coords=dropoff_coords,
            pickup_date=pickup_date,
            pickup_time=pickup_time,
            package_weight=package_weight,
            courier_type=courier_type.capitalize(),
            special_requests=special_requests,
            couriers=couriers,
            current_user_id=current_user.get_id()
        )

    except Exception as e:
        import traceback
        print("ERROR in submit_courier_booking:")
        traceback.print_exc()
        flash("Something went wrong. Please try again.", "danger")
        return redirect(url_for('dashboard'))

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
        # Create ticket immediately
        user_id = current_user.get_id()
        pdf_filename = generate_pdf_ticket(booking_id, 'Courier Booking', payload, user_id)
        payload['ticket_pdf_url'] = f"/static/tickets/{pdf_filename}"
        payload['ticket_generated_at'] = datetime.now().isoformat()

        # Auto-Approve Logic (Phase 3)
        admin_status = 'Pending'
        if package_weight < 5.0 and data['courier_type'] == 'standard':
            admin_status = 'Confirmed'
            save_notification(
                user_id=user_id,
                title="Courier Booking Approved",
                message=f"Your courier request {booking_id} has been automatically approved.",
                icon="check_circle",
                type="success"
            )

        cur.execute("""
            INSERT INTO requests
            (user_id, booking_id, service_type, details, payment_status, admin_confirmation)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
        """, (
            user_id,
            booking_id,
            'Courier Booking',
            json.dumps(payload),
            'Confirmed',
            admin_status
        ))
        conn.commit()

        row = get_last_request_json()
        if row:
            socketio.emit('new_request', {'request': row}, to=None)
            socketio.emit('payment_confirmed', {
                "request_id": row[0],
                "booking_id": booking_id,
                "service_type": 'Courier Booking',
                "ticket_url": f"/static/tickets/{pdf_filename}"
            }, to=None)

        return jsonify({
            "success": True,
            "booking_id": booking_id,
            "message": "Booking confirmed!",
            "ticket_url": f"/static/tickets/{pdf_filename}"
        })

    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Booking DB error: {e}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()

# ---------------------- Chat System ----------------------
# ---------------------- Chat System (Refactored) ----------------------

@app.route('/api/admin/chat/users', methods=['GET'])
@login_required
def get_admin_chat_users():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Get list of users who have support messages
        # DISTINCT ON user_id to get unique users, ordered by latest message
        query = """
            SELECT DISTINCT ON (u.id) 
                u.id, u.full_name, u.profile_picture,
                m.message, m.created_at, m.is_read, m.sender_type
            FROM users u
            JOIN support_messages m ON u.id = m.user_id
            ORDER BY u.id, m.created_at DESC
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        users = []
        for r in rows:
            users.append({
                'id': r[0],
                'name': r[1],
                'profile_picture': r[2] or '',
                'last_message': r[3],
                'timestamp': r[4].isoformat() if r[4] else '',
                'is_read': r[5],
                'last_sender': r[6] # 'user' or 'admin'
            })
            
        # Sort users by most recent message descending
        users.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        logger.error(f"Admin chat users error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/admin/chat/history/<int:user_id>', methods=['GET'])
@login_required
def get_admin_chat_history(user_id):
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Mark messages from this user as read
        cur.execute("UPDATE support_messages SET is_read = TRUE WHERE user_id = %s AND sender_type = 'user'", (user_id,))
        conn.commit()
        
        # Fetch history from support_messages
        query = """
            SELECT id, user_id, sender_type, message, created_at, is_read, attachment_url
            FROM support_messages 
            WHERE user_id = %s
            ORDER BY created_at ASC
        """
        cur.execute(query, (user_id,))
        rows = cur.fetchall()
        
        messages = []
        for r in rows:
            messages.append({
                'id': r[0],
                'user_id': r[1],
                'sender_type': r[2], # 'user' or 'admin'
                'message': r[3],
                'timestamp': r[4].isoformat() if r[4] else '',
                'is_read': r[5],
                'attachment_url': r[6],
                'is_admin': r[2] == 'admin'
            })
            
        return jsonify({'success': True, 'messages': messages})
    except Exception as e:
        logger.error(f"Admin chat history error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/chat/history', methods=['GET'])
@login_required
def get_chat_history():
    """User fetching their own support chat history"""
    user_id = current_user.get_id()
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        query = """
            SELECT id, sender_type, message, created_at, is_read, attachment_url
            FROM support_messages 
            WHERE user_id = %s
            ORDER BY created_at ASC
        """
        cur.execute(query, (user_id,))
        rows = cur.fetchall()
        
        messages = []
        for r in rows:
            sender_type = r[1]
            # For the user: 'user' means "Me", 'admin' means "Support"
            is_me = (sender_type == 'user')
            
            messages.append({
                'id': r[0],
                'sender_type': sender_type,
                'message': r[2],
                'timestamp': r[3].isoformat() if r[3] else '',
                'is_read': r[4],
                'attachment_url': r[5],
                'is_me': is_me
            })
            
        return jsonify({'success': True, 'messages': messages})
    except Exception as e:
        logger.error(f"User chat history error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@socketio.on('send_chat_message')
def handle_chat_message(data):
    """Handle real-time SUPPORT chat messages"""
    sender_id = data.get('sender_id') # The actual user ID sending (or admin's dummy ID)
    message = data.get('message')
    role = data.get('role', 'user') # 'user' or 'admin'
    target_user_id = data.get('target_user_id') # Required if admin is sending
    
    if not message:
        return

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # In support_messages table:
        # user_id is ALWAYS the End User.
        # sender_type is 'user' or 'admin'.
        
        db_user_id = None
        sender_type = ''
        
        if role == 'user':
            db_user_id = sender_id
            sender_type = 'user'
            room_to_emit = f"user_{sender_id}" # User listens here
            # Admin listens to global events or we broadcast to admin
        else:
            # Admin sending
            db_user_id = target_user_id
            sender_type = 'admin'
            room_to_emit = f"user_{target_user_id}"

        if not db_user_id:
            return

        cur.execute("""
            INSERT INTO support_messages (user_id, sender_type, message)
            VALUES (%s, %s, %s)
            RETURNING id, created_at
        """, (db_user_id, sender_type, message))
        
        msg_id, timestamp = cur.fetchone()
        conn.commit()
        
        msg_data = {
            'id': msg_id,
            'user_id': db_user_id,
            'sender_type': sender_type,
            'message': message,
            'timestamp': timestamp.isoformat(),
            'is_me': (role == 'user') # For the sender's UI confirmation
        }
        
        # 1. Emit to the specific user's room (Both User and Admin rely on this for live updates in chat window)
        emit('new_support_message', msg_data, room=room_to_emit)
        
        # 2. If User sent it, also Broadcast to Admins so their list updates
        if role == 'user':
            socketio.server.emit('admin_new_message_alert', msg_data, namespace='/')
            
    except Exception as e:
        logger.error(f"Chat socket error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

@app.route('/api/admin/chat/send-report', methods=['POST'])
@login_required
def admin_send_report():
    if not session.get('is_admin'):
        return jsonify({"error": "Unauthorized"}), 403
    
    data = request.json
    user_id = data.get('user_id')
    report_type = data.get('report_type', 'booking_history')
    
    if not user_id:
        return jsonify({"error": "User ID required"}), 400
        
    try:
        # Generate Report
        if report_type == 'booking_history':
            file_url = generate_booking_history_pdf(user_id)
            message = "Here is your requested booking history report."
        else:
            return jsonify({"error": "Unknown report type"}), 400
            
        # Save to DB
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Insert Message into support_messages
        cur.execute("""
            INSERT INTO support_messages (user_id, sender_type, message, attachment_url)
            VALUES (%s, 'admin', %s, %s)
            RETURNING id, created_at
        """, (user_id, message, file_url))
        msg_id, timestamp = cur.fetchone()
        
        # Insert Report Metadata
        cur.execute("""
            INSERT INTO reports (user_id, report_type, file_path)
            VALUES (%s, %s, %s)
        """, (user_id, report_type, file_url))
        
        conn.commit()
        cur.close()
        conn.close()
        
        # Emit via Socket
        msg_data = {
            'id': msg_id,
            'user_id': user_id,
            'sender_type': 'admin',
            'message': message,
            'timestamp': timestamp.isoformat(),
            'attachment_url': file_url
        }
        
        socketio.emit('new_support_message', msg_data, room=f"user_{user_id}")
        
        return jsonify({'success': True, 'message': 'Report sent successfully'})
        
    except Exception as e:
        logger.error(f"Send report error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ---------------------- User Details ----------------------
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
        
        optional_fields = ['address', 'phone', 'whatsapp', 'instagram', 'facebook', 'profile_picture']
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
                'facebook': 'facebook',
                'profile_picture': 'profile_picture'
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

@app.route('/upload_profile_picture', methods=['POST'])
@login_required
def upload_profile_picture():
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest' or 'application/json' in request.accept_mimetypes

    if 'profile_picture' not in request.files:
        msg = 'No file part'
        if is_ajax: return jsonify({'success': False, 'message': msg}), 400
        flash(msg, 'error')
        return redirect(url_for('dashboard'))
    
    file = request.files['profile_picture']
    
    if file.filename == '':
        msg = 'No selected file'
        if is_ajax: return jsonify({'success': False, 'message': msg}), 400
        flash(msg, 'error')
        return redirect(url_for('dashboard'))
        
    if file and allowed_file(file.filename):
        try:
            # Generate unique filename
            ext = file.filename.rsplit('.', 1)[1].lower()
            filename = secure_filename(f"user_{current_user.get_id()}_{int(time.time())}.{ext}")
            
            # Save file
            save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(save_path)
            
            # Save to database
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute("UPDATE users SET profile_picture = %s WHERE id = %s", (filename, current_user.get_id()))
                conn.commit()
                
                # Success
                msg = 'Profile picture updated successfully!'
                new_url = url_for('static', filename='uploads/profile_pictures/' + filename)
                
                if is_ajax:
                    return jsonify({
                        'success': True, 
                        'message': msg, 
                        'image_url': new_url
                    })
                
                flash(msg, 'success')
            except Exception as e:
                conn.rollback()
                logger.error(f"DB Error updating profile pic: {e}")
                msg = 'Error updating database.'
                if is_ajax: return jsonify({'success': False, 'message': msg}), 500
                flash(msg, 'error')
            finally:
                cur.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"File Save Error: {e}")
            msg = 'Error saving file.'
            if is_ajax: return jsonify({'success': False, 'message': msg}), 500
            flash(msg, 'error')
            
    else:
        msg = 'Allowed file types are png, jpg, jpeg, gif'
        if is_ajax: return jsonify({'success': False, 'message': msg}), 400
        flash(msg, 'error')
        
    return redirect(url_for('dashboard'))

# ---------------------- Hotel / Travel ----------------------
@app.route('/hotel')
@login_required
def hotel_booking():
    return render_template("hotel.html", user=session['username'])

@app.route('/submit-hotel-booking', methods=['GET', 'POST'])
@login_required
def submit_hotel_booking():
    # Default values
    destination = 'Mumbai'
    checkin = get_tomorrow_date()
    checkout = get_day_after_tomorrow()
    rooms = 1
    guests = 2
    min_price = 0
    max_price = 50000 # Default max

    if request.method == 'GET':
        # Pre-filled from AI Recommendations (via URL params)
        destination = request.args.get('destination', 'Mumbai').strip().capitalize()
        checkin = request.args.get('checkin', get_tomorrow_date())
        checkout = request.args.get('checkout', get_day_after_tomorrow())
        try:
            rooms = max(1, int(request.args.get('rooms', 1)))
            guests = max(1, int(request.args.get('guests', 2)))
            min_price = int(request.args.get('min_price', 0))
            max_price = int(request.args.get('max_price', 50000))
        except (ValueError, TypeError):
            rooms = 1
            guests = 2
            min_price = 0
            max_price = 50000

    else:  # POST from modal form
        destination = request.form.get('destination', '').strip().capitalize()
        checkin = request.form.get('checkin', '').strip()
        checkout = request.form.get('checkout', '').strip()
        try:
            rooms = max(1, int(request.form.get('rooms', 1)))
            guests = max(1, int(request.form.get('guests', 2)))
            min_price = int(request.form.get('min_price', 0))
            max_price = int(request.form.get('max_price', 50000))
        except (ValueError, TypeError):
            rooms = 1
            guests = 2
            min_price = 0
            max_price = 50000

    # Validation
    if not destination:
        flash("Please enter a destination city.", "danger")
        return redirect(url_for('dashboard'))

    # Guest validation (Max 3 guests per room)
    if guests > (rooms * 3):
        flash(f"⚠️ Limit Exceeded: Maximum 3 guests allowed per room. For {guests} guests, please book at least {math.ceil(guests/3)} rooms.", "warning")
        # Adjust for the user
        rooms = math.ceil(guests/3)

    if not checkin or not checkout:
        flash("Please select check-in and check-out dates.", "danger")
        return redirect(url_for('dashboard'))

    try:
        check_in_date = datetime.strptime(checkin, '%Y-%m-%d').date()
        check_out_date = datetime.strptime(checkout, '%Y-%m-%d').date()
        if check_out_date <= check_in_date:
            flash("Check-out date must be after check-in date.", "danger")
            return redirect(url_for('dashboard'))
        if check_in_date < date.today():
            flash("Check-in date cannot be in the past.", "danger")
            return redirect(url_for('dashboard'))
    except ValueError:
        flash("Invalid date format. Please use the date picker.", "danger")
        return redirect(url_for('dashboard'))

    # Check if city is supported or generate dynamic data
    if destination in hotels_data:
        all_hotels = hotels_data[destination]
    else:
        # Dynamic generation for all other cities
        all_hotels = generate_dynamic_hotels(destination)
        
    # Get city coordinates for map
    city_coords = CITY_COORDINATES.get(destination)
    if not city_coords and all_hotels:
        # Fallback: use first hotel's coords
        city_coords = {"lat": all_hotels[0]['lat'], "lng": all_hotels[0]['lng']}
    elif not city_coords:
        # Default fallback
        city_coords = {"lat": 20.5937, "lng": 78.9629}

    # Get user profile to filter by budget
    user_id = current_user.get_id()
    profile = get_user_profile(user_id)

    # Filter hotels based on budget
    filtered_hotels = all_hotels
    if profile:
        monthly_budget = profile.get('monthly_budget', 'medium')
        lifestyle_type = profile.get('lifestyle_type', 'comfort')

        # Budget price ranges per night (aligned with lifestyle/engine.py)
        # low: Under Rs 25,000/month → max Rs 3,000/night
        # medium: Rs 25,000-75,000/month → max Rs 6,000/night
        # high: Rs 75,000-1,50,000/month → max Rs 15,000/night
        # premium: Above Rs 1,50,000/month → unlimited

        if monthly_budget == 'low':
            budget_limit = 3000
            # For low budget, strictly exclude luxury
            if lifestyle_type == 'luxury':
                budget_limit = 0  # Show no results - budget mismatch
        elif monthly_budget == 'medium':
            budget_limit = 6000
            # Medium budget with luxury preference gets slightly higher range
            if lifestyle_type == 'luxury':
                budget_limit = 7500
        elif monthly_budget == 'high':
            budget_limit = 15000
        else:  # premium
            budget_limit = float('inf')  # No limit

        # Filter hotels by price
        if budget_limit > 0:
            filtered_hotels = [h for h in all_hotels if h.get('price', 0) <= budget_limit]
        else:
            filtered_hotels = []

    # ── User Defined Price Range Filter (Overrides budget if specific range provided) ──
    # If user manually sets filters in "Modify Search", we respect that over profile budget
    if 'min_price' in request.form or 'min_price' in request.args:
        filtered_hotels = [
            h for h in all_hotels 
            if min_price <= h.get('price', 0) <= max_price
        ]

    random.shuffle(filtered_hotels)
    # selected_hotels = filtered_hotels[:min(len(filtered_hotels), 6)]  # Show up to 6
    selected_hotels = filtered_hotels # Show all matching for better filtering experience

    # Render results page
    return render_template('hotel_results.html',
                           hotels=selected_hotels,
                           destination=destination,
                           checkin=checkin,
                           checkout=checkout,
                           rooms=rooms,
                           guests=guests,
                           city_coords=city_coords,
                           user_budget=profile.get('monthly_budget', 'medium') if profile else 'medium',
                           min_price=min_price,
                           max_price=max_price)

@app.route('/submit-travel-booking', methods=['GET', 'POST'])
@login_required
def submit_travel_booking():
    import logging
    logger = logging.getLogger(__name__)

    # Default values
    origin = 'Mumbai'
    destination = 'Delhi'  # Popular default
    departure_date = get_in_7_days()
    return_date = None
    adults = 1
    children = 0
    infants = 0
    travel_class = 'economy'

    if request.method == 'GET':
        # Pre-filled from AI Recommendations (via URL params)
        location_text = session.get('user_location', {}).get('city', 'Mumbai')
        origin = request.args.get('origin', location_text).strip().title()
        destination = request.args.get('destination', 'Delhi').strip().title()
        departure_date = request.args.get('departure_date', get_in_7_days())
        return_date = request.args.get('return_date')  # Optional
        try:
            adults = max(1, int(request.args.get('adults', 1)))
            children = max(0, int(request.args.get('children', 0)))
            infants = max(0, int(request.args.get('infants', 0)))
        except (ValueError, TypeError):
            adults = 1
            children = 0
            infants = 0
        travel_class = request.args.get('class', 'economy').lower()

    else:  # POST from modal form
        origin = request.form.get('origin', '').strip().title()
        destination = request.form.get('destination', '').strip().title()
        departure_date = request.form.get('departure_date', '').strip()
        return_date = request.form.get('return_date', '').strip() or None
        try:
            adults = max(1, int(request.form.get('adults', 1)))
            children = max(0, int(request.form.get('children', 0)))
            infants = max(0, int(request.form.get('infants', 0)))
        except (ValueError, TypeError):
            adults = 1
            children = 0
            infants = 0
        travel_class = request.form.get('class', 'economy').lower()

    logger.debug(f"Travel search: origin={origin}, destination={destination}, departure={departure_date}, "
                 f"return={return_date}, adults={adults}, children={children}, infants={infants}, class={travel_class}")

    # Validation
    if not origin or not destination or not departure_date:
        flash("Please provide origin, destination, and departure date.", "danger")
        return redirect(url_for('dashboard'))

    if origin.lower() == destination.lower():
        flash("Origin and destination cannot be the same.", "danger")
        return redirect(url_for('dashboard'))

    try:
        departure = datetime.strptime(departure_date, '%Y-%m-%d')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if departure < today:
            flash("Departure date cannot be in the past.", "danger")
            return redirect(url_for('dashboard'))
        if return_date:
            return_date_obj = datetime.strptime(return_date, '%Y-%m-%d')
            if return_date_obj <= departure:
                flash("Return date must be after departure date.", "danger")
                return redirect(url_for('dashboard'))
    except ValueError:
        flash("Invalid date format. Please use the date picker.", "danger")
        return redirect(url_for('dashboard'))

    # Generate flights
    flights = generate_flight_data(origin, destination, travel_class)

    display_count = random.choice([5, 6])
    displayed_flights = random.sample(flights, min(display_count, len(flights))) if len(flights) > display_count else flights

    arrival_date = departure_date  # Or calculate if needed

    flash("Showing best flight options for your trip!", "info")

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
    
    # Expanded Airlines List
    airlines = [
        {'name': 'IndiGo', 'code': '6E', 'hub': 'DEL'},
        {'name': 'Air India', 'code': 'AI', 'hub': 'DEL'},
        {'name': 'Vistara', 'code': 'UK', 'hub': 'DEL'},
        {'name': 'SpiceJet', 'code': 'SG', 'hub': 'DEL'},
        {'name': 'Air India Express', 'code': 'IX', 'hub': 'CCJ'},
        {'name': 'Akasa Air', 'code': 'QP', 'hub': 'BOM'},
        {'name': 'Alliance Air', 'code': '9I', 'hub': 'DEL'},
        {'name': 'Star Air', 'code': 'S5', 'hub': 'BLR'}
    ]
    
    # Expanded Airport Codes (Major + Tier 2)
    airport_codes = {
        'Mumbai': 'BOM', 'Delhi': 'DEL', 'Bangalore': 'BLR', 'Chennai': 'MAA',
        'Hyderabad': 'HYD', 'Kolkata': 'CCU', 'Pune': 'PNQ', 'Goa': 'GOI',
        'Jaipur': 'JAI', 'Ahmedabad': 'AMD', 'Lucknow': 'LKO', 'Cochin': 'COK',
        'Patna': 'PAT', 'Indore': 'IDR', 'Chandigarh': 'IXC', 'Nagpur': 'NAG',
        'Bhubaneswar': 'BBI', 'Coimbatore': 'CJB', 'Thiruvananthapuram': 'TRV',
        'Visakhapatnam': 'VTZ', 'Surat': 'STV', 'Varanasi': 'VNS', 'Guwahati': 'GAU',
        'Amritsar': 'ATQ', 'Ranchi': 'IXR', 'Raipur': 'RPR', 'Bhopal': 'BHO'
    }
    
    # Dynamic Code Generation for unknown cities
    origin_code = airport_codes.get(origin, origin[:3].upper())
    destination_code = airport_codes.get(destination, destination[:3].upper())
    
    # Calculate Approximate Distance & Duration (Mock logic)
    # Using lat/long distance would be better, but mock logic suffices for realism
    # Base duration 1h + random factor
    base_duration_mins = random.randint(60, 180) 
    
    num_flights = random.randint(12, 18)
    
    for i in range(num_flights):
        airline = random.choice(airlines)
        flight_number = f"{airline['code']}{random.randint(100, 999)}"
        flight_name = f"{airline['name']} {flight_number}"
        
        # Smart Departure Times (Morning, Afternoon, Evening, Night)
        time_slot = random.choice(['morning', 'morning', 'afternoon', 'evening', 'night'])
        if time_slot == 'morning': departure_hour = random.randint(5, 11)
        elif time_slot == 'afternoon': departure_hour = random.randint(12, 16)
        elif time_slot == 'evening': departure_hour = random.randint(17, 21)
        else: departure_hour = random.randint(22, 23) # or early morning 0-4
        
        departure_minute = random.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
        
        # Duration variation
        duration_minutes = base_duration_mins + random.randint(-15, 15)
        duration_hours = duration_minutes // 60
        duration_remaining_minutes = duration_minutes % 60
        
        # Arrival Time Calculation
        departure_total_mins = departure_hour * 60 + departure_minute
        arrival_total_mins = (departure_total_mins + duration_minutes) % (24 * 60)
        arrival_hour = arrival_total_mins // 60
        arrival_minute = arrival_total_mins % 60
        
        # Stops Logic
        stops = random.choices([0, 1, 2], weights=[70, 25, 5], k=1)[0]
        if stops > 0:
            duration_hours += stops * 2 # Add layover time
        
        # Price Logic
        base_price_map = {
            'economy': random.randint(3000, 6000),
            'premium_economy': random.randint(5500, 9000),
            'business': random.randint(12000, 25000),
            'first': random.randint(25000, 45000)
        }
        price = base_price_map.get(travel_class, 4000)
        
        # Adjust price based on stops (cheaper) and time (expensive morning/evening)
        if stops > 0: price *= 0.85
        if 8 <= departure_hour <= 10 or 17 <= departure_hour <= 19: price *= 1.10
        
        price = int(price / 100) * 100 + 99 # Make it look like 4999
        
        baggage_allowance = {
            'economy': "15kg (1 Pc)",
            'premium_economy': "25kg (2 Pcs)",
            'business': "35kg (2 Pcs)",
            'first': "40kg (3 Pcs)"
        }.get(travel_class, "15kg")
        
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
            'duration': f'{duration_hours}h {duration_remaining_minutes:02d}m',
            'travel_class': travel_class,
            'seats_available': random.randint(2, 25),
            'price': int(price),
            'status': 'On Time',
            'baggage_allowance': baggage_allowance,
            'meal_included': travel_class != 'economy' or random.choice([True, False]),
            'wifi_available': travel_class != 'economy',
            'stops': stops,
            'refundable': random.choice([True, False]),
            'deal': random.choice(['', '', '', 'Fastest', 'Cheapest', 'Best Value']),
            'lat_origin': CITY_COORDINATES.get(origin, {}).get('lat', 20.59), # Pass coords for map
            'lng_origin': CITY_COORDINATES.get(origin, {}).get('lng', 78.96),
            'lat_dest': CITY_COORDINATES.get(destination, {}).get('lat', 28.61),
            'lng_dest': CITY_COORDINATES.get(destination, {}).get('lng', 77.20)
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
        # Create ticket immediately
        user_id = current_user.get_id()
        pdf_filename = generate_pdf_ticket(booking_id, 'Flight Booking', details_obj, user_id)
        details_obj['ticket_pdf_url'] = f"/static/tickets/{pdf_filename}"
        details_obj['ticket_generated_at'] = datetime.now().isoformat()

        cur.execute("""
            INSERT INTO requests (user_id, booking_id, service_type, details, payment_status, admin_confirmation)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            RETURNING id
        """, (
            user_id,
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
            "message": "Flight booking confirmed successfully!",
            "ticket_url": f"/static/tickets/{pdf_filename}"
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
    """Secure password reset - sends reset link instead of exposing password"""
    if request.method == 'POST':
        username = request.form.get('username')

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id, email FROM users WHERE username = %s", (username,))
            result = cur.fetchone()
            
            if result:
                user_id, email = result
                # Generate secure reset token
                reset_token = str(uuid.uuid4())
                token_expiry = datetime.now() + timedelta(hours=1)
                
                # Store token in database (create table if needed)
                try:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS password_reset_tokens (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER REFERENCES users(id),
                            token VARCHAR(255) UNIQUE NOT NULL,
                            expires_at TIMESTAMP NOT NULL,
                            used BOOLEAN DEFAULT FALSE,
                            created_at TIMESTAMP DEFAULT NOW()
                        )
                    """)
                    # Invalidate any existing tokens for this user
                    cur.execute("UPDATE password_reset_tokens SET used = TRUE WHERE user_id = %s", (user_id,))
                    # Insert new token
                    cur.execute("""
                        INSERT INTO password_reset_tokens (user_id, token, expires_at)
                        VALUES (%s, %s, %s)
                    """, (user_id, reset_token, token_expiry))
                    conn.commit()
                    
                    # In production, send email with reset link
                    # For now, show a success message (don't reveal if user exists)
                    try:
                        import smtplib
                        from email.mime.text import MIMEText
                        from email.mime.multipart import MIMEMultipart

                        # Email configuration
                        smtp_server = "smtp.gmail.com"
                        smtp_port = 587
                        sender_email = "formovieenjoy.1@gmail.com"  # Replace with environment variable
                        sender_password = "vfli ztom yhln iyfc".replace(" ", "")  # Remove spaces from app password

                        msg = MIMEMultipart()
                        msg['From'] = sender_email
                        msg['To'] = email
                        msg['Subject'] = "Password Reset Request"

                        reset_link = url_for('reset_password', token=reset_token, _external=True)

                        body = f"""
                        Hello,

                        You have requested to reset your password. Please click the link below to reset it:

                        {reset_link}

                        If you did not request this change, please ignore this email.

                        Link expires in 1 hour.

                        Best regards,
                        Concierge Life Team
                        """

                        msg.attach(MIMEText(body, 'plain'))

                        server = smtplib.SMTP(smtp_server, smtp_port)
                        server.starttls()
                        server.login(sender_email, sender_password)
                        server.send_message(msg)
                        server.quit()

                        logger.info(f"Password reset email sent to {email}")
                    except Exception as email_error:
                        logger.error(f"Failed to send reset email: {email_error}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error creating reset token: {e}")
            
            # Always show same message to prevent user enumeration
            flash("If an account exists with that username, a password reset link has been sent to the registered email.", "info")
        except Exception as e:
            logger.error(f"Forgot password error: {e}")
            flash("An error occurred. Please try again.", "error")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for('forgot_password'))
    return render_template('forgot_password.html')


@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    """Handle password reset with token"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Verify token is valid and not expired
        cur.execute("""
            SELECT user_id FROM password_reset_tokens 
            WHERE token = %s AND used = FALSE AND expires_at > NOW()
        """, (token,))
        result = cur.fetchone()
        
        if not result:
            flash("Invalid or expired reset link. Please request a new one.", "error")
            return redirect(url_for('forgot_password'))
        
        user_id = result[0]
        
        if request.method == 'POST':
            new_password = request.form.get('password')
            confirm_password = request.form.get('confirm_password')
            
            if not new_password or len(new_password) < 6:
                flash("Password must be at least 6 characters.", "error")
                return render_template('reset_password.html', token=token)
            
            if new_password != confirm_password:
                flash("Passwords do not match.", "error")
                return render_template('reset_password.html', token=token)
            
            # Update password and mark token as used
            cur.execute("UPDATE users SET password = %s WHERE id = %s", (new_password, user_id))
            cur.execute("UPDATE password_reset_tokens SET used = TRUE WHERE token = %s", (token,))
            conn.commit()
            
            flash("Password reset successfully! Please login with your new password.", "success")
            return redirect(url_for('login'))
        
        return render_template('reset_password.html', token=token)
        
    except Exception as e:
        logger.error(f"Reset password error: {e}")
        flash("An error occurred. Please try again.", "error")
        return redirect(url_for('forgot_password'))
    finally:
        cur.close()
        conn.close()

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


# ---------------------- New Real-time Features (Phase 2) ----------------------

from lifestyle.engine import _dynamic_price_info

@app.route('/api/estimate-price', methods=['POST'])
@login_required
def api_estimate_price():
    """Get real-time price estimate based on current demand/time"""
    try:
        data = request.json
        service_type = data.get('service_type')
        
        # Base prices (can be moved to DB/Config later)
        base_prices = {
            'Hotel Booking': (3000, 8000),
            'Flight Booking': (4000, 10000),
            'Car Booking': (800, 2000),
            'Luxury Cabs': (2500, 5000),
            'Technician Booking': (500, 1500),
            'Courier Booking': (100, 500),
            'Courier & Delivery': (100, 500)
        }
        
        if service_type not in base_prices:
            # Try to map generic types
            if 'hotel' in service_type.lower(): service_type = 'Hotel Booking'
            elif 'flight' in service_type.lower(): service_type = 'Flight Booking'
            elif 'car' in service_type.lower() or 'cab' in service_type.lower(): service_type = 'Car Booking'
            elif 'tech' in service_type.lower(): service_type = 'Technician Booking'
            elif 'courier' in service_type.lower(): service_type = 'Courier Booking'
            
        min_p, max_p = base_prices.get(service_type, (1000, 3000))
        
        # Adjust base based on specific params if provided
        if service_type == 'Car Booking' and data.get('cab_class') == 'luxury':
            min_p, max_p = 3000, 7000
        elif service_type == 'Flight Booking' and data.get('class') == 'business':
            min_p, max_p = 15000, 35000
            
        price_str, reason = _dynamic_price_info(service_type, min_p, max_p, datetime.now())
        
        return jsonify({
            'success': True,
            'estimate': price_str,
            'reason': reason,
            'service_type': service_type
        })
        
    except Exception as e:
        logger.error(f"Price estimate error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

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

@app.route('/api/user-profile')
@login_required
def api_user_profile():
    """Get user lifestyle profile data for pre-filling booking forms"""
    try:
        user_id = current_user.get_id()
        profile = get_user_profile(user_id)

        if profile:
            return jsonify({
                'success': True,
                'has_profile': True,
                'profile': {
                    'typical_group_size': profile.get('typical_group_size', 2),
                    'monthly_budget': profile.get('monthly_budget', 'medium'),
                    'lifestyle_type': profile.get('lifestyle_type', 'comfort'),
                    'travel_style': profile.get('travel_style', 'comfort'),
                    'preferred_cab_type': profile.get('preferred_cab_type', 'sedan'),
                    'city': profile.get('city', 'Mumbai'),
                    'preferred_services': profile.get('preferred_services', '').split(',') if profile.get('preferred_services') else []
                }
            })
        else:
            return jsonify({
                'success': True,
                'has_profile': False,
                'profile': None
            })
    except Exception as e:
        logger.error(f"Error fetching user profile: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Unable to fetch profile'
        }), 500

@app.route('/api/lifestyle-recommendations')
@login_required
def api_lifestyle_recommendations():
    """Get AI-powered recommendations based on comprehensive lifestyle profile.

    This endpoint now uses the modular lifestyle/service.py for recommendation generation,
    eliminating code duplication and ensuring consistent behavior.
    """
    try:
        user_id = current_user.get_id()

        # Use the modular recommendation service
        from lifestyle.service import recompute_recommendations

        result = recompute_recommendations(user_id, force=False, algorithm_version="v2")

        return jsonify({
            'success': True,
            'has_profile': result.get('has_profile', False),
            'recommendations': result.get('recommendations', []),
            'source': result.get('source', 'generated'),
            'profile_updated_at': result.get('profile_updated_at'),
            'algorithm_version': result.get('algorithm_version', 'v2')
        })

    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'message': 'Unable to load recommendations. Please try again.',
            'recommendations': [],
            'has_profile': True
        }), 500


@app.route('/api/lifestyle-recommendations-legacy')
@login_required
def api_lifestyle_recommendations_legacy():
    """Legacy endpoint - kept for backward compatibility. Use /api/lifestyle-recommendations instead."""
    try:
        user_id = current_user.get_id()
        
        # Legacy: Check database directly
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT service_type, title, description, reason, match_score, metadata 
                FROM ai_recommendations 
                WHERE user_id = %s AND is_dismissed = FALSE
                ORDER BY match_score DESC
            """, (user_id,))
            rows = cur.fetchall()
            
            if rows:
                recommendations = []
                for r in rows:
                    recommendations.append({
                        'service_type': r[0],
                        'title': r[1],
                        'description': r[2],
                        'reason': r[3],
                        'match_score': r[4],
                        'metadata': r[5] if isinstance(r[5], dict) else json.loads(r[5] or '{}')
                    })
                return jsonify({
                    'success': True,
                    'has_profile': True,
                    'recommendations': recommendations,
                    'source': 'database'
                })
        except Exception as e:
            logger.error(f"Error fetching from ai_recommendations: {e}")
        finally:
            cur.close()
            conn.close()

        # 2. If no recommendations found, GENERATE NEW ONES
        logger.info(f"Generating NEW recommendations for user {user_id}")
        
        profile = get_user_profile(user_id)
        
        if not profile:
            return jsonify({
                'success': True,
                'has_profile': False,
                'recommendations': [],
                'message': 'Complete your lifestyle profile to get personalized recommendations'
            }), 200

        # ... (rest of the generation logic) ...
        # Handle interests whether it's string or list
        interests_raw = profile.get('interests', '')
        if isinstance(interests_raw, list):
            interests = [str(i).strip().lower() for i in interests_raw if str(i).strip()]
        elif isinstance(interests_raw, str):
            interests = [i.strip().lower() for i in interests_raw.split(',') if i.strip()]
        else:
            interests = []

        # Handle preferred services
        preferred_services_raw = profile.get('preferred_services', '')
        if isinstance(preferred_services_raw, list):
            preferred_services = [str(s).strip().lower() for s in preferred_services_raw if str(s).strip()]
        elif isinstance(preferred_services_raw, str):
            preferred_services = [s.strip().lower() for s in preferred_services_raw.split(',') if s.strip()]
        else:
            preferred_services = []

        # Get other profile data
        travel_frequency = profile.get('travel_frequency', 'monthly')
        travel_style = profile.get('travel_style', 'comfort')
        lifestyle_type = profile.get('lifestyle_type', 'comfort')
        monthly_budget = profile.get('monthly_budget', 'medium')
        typical_group_size = int(profile.get('typical_group_size', 1))
        preferred_cab_type = profile.get('preferred_cab_type', 'sedan')
        home_owner = bool(profile.get('home_owner', False))
        city = profile.get('city', '')
        profession = profile.get('profession', '')

        # Get past bookings to boost scores based on frequency
        conn = get_db_connection()
        cur = conn.cursor()
        past_services_counts = {}
        try:
            cur.execute("""
                SELECT service_type, COUNT(*) 
                FROM requests 
                WHERE user_id = %s 
                GROUP BY service_type
            """, (user_id,))
            rows = cur.fetchall()
            for r in rows:
                past_services_counts[r[0]] = r[1]
        except Exception as e:
            logger.error(f"Error fetching past services: {e}")
        finally:
            cur.close()
            conn.close()

        # Dynamic Pricing Helper
        def get_dynamic_price_info(service_type, base_price_min, base_price_max):
            now = datetime.now()
            hour = now.hour
            weekday = now.weekday() # 0=Mon, 6=Sun
            
            multiplier = 1.0
            reasons = []
            
            # Time-based Logic
            if service_type in ['Car Booking', 'Luxury Cabs']:
                if hour in [8, 9, 10, 17, 18, 19]:
                    multiplier += 0.4
                    reasons.append("Peak Traffic")
                elif hour >= 22 or hour <= 5:
                    multiplier += 0.2
                    reasons.append("Night Fare")
                    
            elif service_type == 'Hotel Booking':
                if weekday in [4, 5, 6]: # Fri-Sun
                    multiplier += 0.3
                    reasons.append("Weekend Demand")
                elif hour >= 20: # Late night booking
                    multiplier -= 0.1 # Last minute deal potential
                    reasons.append("Late Night Deal")

            elif service_type == 'Flight Booking':
                if weekday in [4, 5, 6]:
                    multiplier += 0.2
                    reasons.append("Weekend Travel")
                if hour <= 6:
                    multiplier -= 0.1
                    reasons.append("Early Bird")

            elif service_type == 'Technician Booking':
                if weekday == 6:
                    multiplier += 0.5
                    reasons.append("Sunday Service")
                elif hour >= 18:
                    multiplier += 0.25
                    reasons.append("After Hours")

            # Calculate final prices
            final_min = int(base_price_min * multiplier)
            final_max = int(base_price_max * multiplier)
            
            price_str = f"₹{final_min:,}-{final_max:,}"
            if "night" in service_type.lower() or service_type == 'Hotel Booking':
                price_str += "/night"
            elif "car" in service_type.lower() or "cab" in service_type.lower():
                price_str += "/trip"
                
            reason_str = f"{', '.join(reasons)} (+{int((multiplier-1)*100)}%)" if reasons and multiplier > 1.0 else ""
            if multiplier < 1.0 and reasons:
                 reason_str = f"{', '.join(reasons)} ({int((multiplier-1)*100)}%)"
            
            return price_str, reason_str

        recommendations = []

        # SCORING LOGIC & GENERATION
        
        # 1. HOTEL RECOMMENDATIONS
        hotel_score = 0
        hotel_reasons = []
        
        if travel_frequency in ['monthly', 'weekly', 'frequent']:
            hotel_score += 25
            hotel_reasons.append("frequent traveler")
        
        if lifestyle_type == 'luxury':
            hotel_score += 30
            hotel_reasons.append("luxury lifestyle")
        elif lifestyle_type == 'comfort':
            hotel_score += 20
        
        # Interest check
        matched_interests = [i for i in ['fine_dining', 'spa', 'shopping', 'fitness'] if i in interests]
        if matched_interests:
            hotel_score += len(matched_interests) * 10
            hotel_reasons.append(f"interests: {', '.join(matched_interests)}")
        
        if 'hotel' in preferred_services:
            hotel_score += 25
            hotel_reasons.append("preferred service")
            
        # Boost based on history frequency
        hist_count = past_services_counts.get('Hotel Booking', 0)
        if hist_count > 0:
            hotel_score += min(30, 10 + (hist_count * 5))
            hotel_reasons.append(f"booked {hist_count} times")
        
        # Check budget
        hotel_budget_ok = True
        base_min, base_max = 4000, 8000 # Default medium
        
        if monthly_budget == 'low':
            if lifestyle_type == 'luxury': hotel_score -= 30; hotel_budget_ok = False
            base_min, base_max = 2000, 5000
            hotel_type = "Budget Hotels"
        elif monthly_budget == 'medium':
            if lifestyle_type == 'luxury' and 'fine_dining' not in interests: hotel_score -= 10
            base_min, base_max = 4000, 8000
            hotel_type = "Comfort Hotels"
        elif monthly_budget == 'high':
            if hotel_score > 0: hotel_score += 15; hotel_reasons.append("premium budget")
            base_min, base_max = 8000, 15000
            hotel_type = "Premium Hotels"
        else: # premium
            if hotel_score > 0: hotel_score += 15; hotel_reasons.append("premium budget")
            base_min, base_max = 15000, 40000
            hotel_type = "Luxury Resorts"
        
        if hotel_score >= 50 and hotel_budget_ok:
            price_str, price_reason = get_dynamic_price_info('Hotel Booking', base_min, base_max)
            recommendations.append({
                'id': 1,
                'service_type': 'Hotel Booking',
                'reason': f'Perfect for {", ".join(hotel_reasons[:2])}.',
                'match_score': min(95, hotel_score),
                'metadata': {
                    'price': price_str,
                    'price_reason': price_reason,
                    'hotel_type': hotel_type,
                    'location': city or 'Major Cities',
                    'amenities': 'Matched to your preferences'
                }
            })

        # 2. FLIGHT RECOMMENDATIONS
        flight_score = 0
        flight_reasons = []
        
        if travel_frequency in ['weekly', 'frequent']:
            flight_score += 40; flight_reasons.append("frequent flyer")
        elif travel_frequency == 'monthly':
            flight_score += 25; flight_reasons.append("monthly traveler")
        
        if 'flight' in preferred_services:
            flight_score += 30; flight_reasons.append("preferred service")
            
        hist_count = past_services_counts.get('Flight Booking', 0)
        if hist_count > 0:
            flight_score += min(30, 10 + (hist_count * 5))
            flight_reasons.append(f"booked {hist_count} times")
        
        if travel_style == 'business':
            flight_score += 20; flight_reasons.append("business travel")
        
        flight_budget_ok = True
        base_min, base_max = 3000, 8000
        
        if monthly_budget == 'low' and travel_style in ['business', 'luxury']:
             flight_score -= 20; flight_budget_ok = False

        if flight_score >= 40 and flight_budget_ok:
            if monthly_budget in ['high', 'premium'] and (travel_style == 'business' or lifestyle_type == 'luxury'):
                travel_class = "Business Class"
                base_min, base_max = 15000, 40000
            elif monthly_budget == 'high' or travel_style == 'comfort':
                travel_class = "Premium Economy"
                base_min, base_max = 8000, 15000
            else:
                travel_class = "Economy"
                base_min, base_max = 3000, 8000
            
            price_str, price_reason = get_dynamic_price_info('Flight Booking', base_min, base_max)
            recommendations.append({
                'id': 2,
                'service_type': 'Flight Booking',
                'reason': f'Ideal for {", ".join(flight_reasons)}.',
                'match_score': min(90, flight_score),
                'metadata': {
                    'price': price_str,
                    'price_reason': price_reason,
                    'class': travel_class,
                    'routes': 'Domestic & International'
                }
            })

        # 3. CAR RECOMMENDATIONS
        car_score = 0
        car_reasons = []
        
        if typical_group_size > 3:
            car_score += 25; car_reasons.append(f"group of {typical_group_size}")
        
        if preferred_cab_type == 'luxury' or lifestyle_type == 'luxury':
            car_score += 30; car_reasons.append("luxury preference")
        elif preferred_cab_type in ['suv', 'sedan']:
            car_score += 20; car_reasons.append(f"{preferred_cab_type} preference")
        
        if 'cab' in preferred_services:
            car_score += 25; car_reasons.append("preferred service")
            
        hist_count = past_services_counts.get('Car Booking', 0)
        if hist_count > 0:
            car_score += min(30, 10 + (hist_count * 5))
            car_reasons.append(f"booked {hist_count} times")
        
        car_budget_ok = True
        if monthly_budget == 'low' and preferred_cab_type == 'luxury':
            car_score -= 25; car_budget_ok = False
        
        if car_score >= 40 and car_budget_ok:
            if monthly_budget in ['high', 'premium'] and (preferred_cab_type == 'luxury' or lifestyle_type == 'luxury'):
                cab_type = "Luxury Cabs (BMW/Merc)"
                base_min, base_max = 2500, 5000
            elif monthly_budget != 'low' and (preferred_cab_type == 'suv' or typical_group_size > 3):
                cab_type = "Premium SUV"
                base_min, base_max = 1500, 3000
            elif monthly_budget == 'low':
                cab_type = "Budget Sedan"
                base_min, base_max = 500, 1200
            else:
                cab_type = "Comfort Sedan"
                base_min, base_max = 800, 1500
            
            price_str, price_reason = get_dynamic_price_info('Car Booking', base_min, base_max)
            recommendations.append({
                'id': 3,
                'service_type': 'Car Booking',
                'reason': f'Best for {", ".join(car_reasons)}.',
                'match_score': min(85, car_score),
                'metadata': {
                    'price': price_str,
                    'price_reason': price_reason,
                    'vehicle': cab_type,
                    'capacity': f'Up to {max(4, typical_group_size)} passengers'
                }
            })

        # 4. TECHNICIAN RECOMMENDATIONS
        if home_owner:
            tech_score = 60
            tech_reasons = ["home owner"]
            
            if any(i in interests for i in ['tech', 'fitness', 'music', 'art']):
                tech_score += 15; tech_reasons.append("home maintenance needs")
            
            if 'technician' in preferred_services:
                tech_score += 20; tech_reasons.append("preferred service")
            
            hist_count = past_services_counts.get('Technician Booking', 0)
            if hist_count > 0:
                tech_score += min(30, 10 + (hist_count * 5))
                tech_reasons.append(f"booked {hist_count} times")
            
            price_str, price_reason = get_dynamic_price_info('Technician Booking', 500, 2000)
            recommendations.append({
                'id': 4,
                'service_type': 'Technician Booking',
                'reason': f'Essential for {", ".join(tech_reasons)}.',
                'match_score': min(90, tech_score),
                'metadata': {
                    'price': price_str,
                    'price_reason': price_reason,
                    'availability': 'Same-day & Emergency',
                    'services': 'AC, Plumbing, Electrical, Carpentry'
                }
            })

        # 5. COURIER RECOMMENDATIONS
        courier_score = 0
        courier_reasons = []
        
        if 'courier' in preferred_services:
            courier_score += 40; courier_reasons.append("preferred service")
        
        if travel_style == 'business' or profile.get('profession', '') in ['business', 'working', 'freelancer']:
            courier_score += 25; courier_reasons.append(f"{profession} needs")
        
        delivery_type = "Standard Delivery"
        base_min, base_max = 100, 300
        
        if monthly_budget in ['high', 'premium']:
            courier_score += 15; courier_reasons.append("express delivery budget")
            delivery_type = "Express Delivery"
            base_min, base_max = 300, 800
        elif monthly_budget == 'medium':
             delivery_type = "Standard/Express"
             base_min, base_max = 150, 500
            
        hist_count = past_services_counts.get('Courier Booking', 0)
        if hist_count > 0:
            courier_score += min(30, 10 + (hist_count * 5))
            courier_reasons.append(f"booked {hist_count} times")
        
        if courier_score >= 40:
            price_str, price_reason = get_dynamic_price_info('Courier Booking', base_min, base_max)
            recommendations.append({
                'id': 5,
                'service_type': 'Courier Booking',
                'reason': f'Useful for {", ".join(courier_reasons)}.',
                'match_score': min(80, courier_score),
                'metadata': {
                    'price': price_str,
                    'price_reason': price_reason,
                    'delivery': delivery_type,
                    'tracking': 'Real-time GPS Tracking'
                }
            })

        # Save recommendations to database
        if recommendations:
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                
                # Clear old recommendations first
                cur.execute("DELETE FROM ai_recommendations WHERE user_id = %s", (user_id,))
                
                # Insert new ones
                for rec in recommendations:
                    cur.execute("""
                        INSERT INTO ai_recommendations (
                            user_id, service_type, title, description, reason, match_score, metadata, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        user_id, 
                        rec['service_type'],
                        rec['service_type'], # Title
                        rec['reason'], # Description
                        rec['reason'], 
                        rec['match_score'],
                        json.dumps(rec.get('metadata', {}))
                    ))
                conn.commit()
            except Exception as e:
                logger.error(f"Error saving recommendations: {e}")
                if conn: conn.rollback()
            finally:
                if cur: cur.close()
                if conn: conn.close()

        # If no specific recommendations, show generic ones
        if not recommendations:
            # Generic recommendations based on common needs
            generic_recs = [
                {
                    'id': 6,
                    'service_type': 'Hotel Booking',
                    'reason': 'Great for weekend getaways and business trips',
                    'match_score': 75,
                    'metadata': {
                        'price': '₹3,000-15,000/night',
                        'location': 'Popular Destinations',
                        'amenities': 'Basic to Premium'
                    }
                },
                {
                    'id': 7,
                    'service_type': 'Car Booking',
                    'reason': 'Convenient for local travel and airport transfers',
                    'match_score': 70,
                    'metadata': {
                        'price': '₹1,000-3,000/trip',
                        'vehicle': 'Standard to Luxury',
                        'capacity': 'Up to 4 passengers'
                    }
                }
            ]
            recommendations = generic_recs

        # Sort by match score (highest first) and limit to 5
        recommendations.sort(key=lambda x: x['match_score'], reverse=True)
        recommendations = recommendations[:5]

        return jsonify({
            'success': True,
            'has_profile': True,
            'recommendations': recommendations,
            'new_recommendations': len(recommendations),
            'profile_summary': {
                'travel_style': travel_style,
                'lifestyle_type': lifestyle_type,
                'interests_count': len(interests),
                'preferred_services': preferred_services
            }
        })

    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return proper JSON error response
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'message': 'Unable to load recommendations. Please try again.',
            'recommendations': [],
            'has_profile': True
        }), 500

@app.route('/api/nearby-services', methods=['POST'])
@login_required
def api_nearby_services():
    """Get services near user location with PROPER booking data and BUDGET filtering"""
    try:
        data = request.json
        lat = data.get('lat')
        lng = data.get('lng')
        radius = data.get('radius', 10)  # Default 10km radius
        user_id = current_user.get_id()
        
        using_fallback = False
        if not lat or not lng:
            # Fallback to Mumbai coordinates
            lat = 19.0760
            lng = 72.8777
            using_fallback = True
            
        # Get user profile for budget filtering
        profile = get_user_profile(user_id)
        budget = profile.get('monthly_budget', 'medium') if profile else 'medium'
        
        logger.info(f"Finding services near: {lat}, {lng} (radius: {radius}km) for budget: {budget}")
        
        # Get location name
        city = 'Mumbai'
        if not using_fallback:
            try:
                import requests
                # Use a timeout to prevent hanging
                response = requests.get(
                    f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lng}&zoom=10",
                    headers={'User-Agent': 'ConciergeLifestyle/1.0'},
                    timeout=2
                )
                if response.ok:
                    location_data = response.json()
                    city = location_data.get('address', {}).get('city') or location_data.get('address', {}).get('town') or 'Unknown Location'
            except:
                pass
        
        services = []
        
        # City boundary boxes - ACCURATE land boundaries to prevent services on water/forest
        # Format: (min_lat, max_lat, min_lng, max_lng) - strict land-only boundaries
        CITY_BOUNDARIES = {
            # Mumbai - Avoid Arabian Sea (west), avoid Thane Creek (east)
            'Mumbai': (18.90, 19.27, 72.82, 72.96),

            # Pune - Avoid hills and forest areas
            'Pune': (18.42, 18.63, 73.75, 73.95),

            # Nashik - City center, avoid Sahyadri hills
            'Nashik': (19.95, 20.05, 73.75, 73.85),

            # Delhi - NCR boundaries
            'Delhi': (28.50, 28.75, 77.05, 77.30),

            # Bangalore - City limits, avoid outskirts
            'Bangalore': (12.90, 13.10, 77.50, 77.70),

            # Default tight boundary for unknown cities
            'default': None
        }

        # Advanced land validation - city-specific checks
        def is_on_land(lat, lng, city_name):
            """
            Validates if coordinates are on habitable land.
            Returns False for water bodies, forests, restricted areas.
            """
            if city_name == 'Mumbai':
                # Mumbai's unique geography - peninsula with Arabian Sea on west
                # South Mumbai (lat < 18.95): Very narrow, avoid west coast
                if lat < 18.95:
                    # South Mumbai: Only lng > 72.825 (Nariman Point eastward)
                    return lng > 72.825

                # Central Mumbai (18.95 - 19.05): Wider
                elif lat < 19.05:
                    return 72.82 <= lng <= 72.89

                # North Mumbai/Suburbs (19.05 - 19.20): Widest part
                elif lat < 19.20:
                    return 72.82 <= lng <= 72.95

                # Far North (>19.20): Narrower again
                else:
                    return 72.84 <= lng <= 72.92

            elif city_name == 'Pune':
                # Pune: Avoid Western Ghats hills
                # Hills mostly to the west and north
                if lat > 18.58:  # North Pune
                    return lng > 73.80  # Avoid Lonavala direction
                return True

            elif city_name == 'Delhi':
                # Delhi: Yamuna River on east, avoid it
                if lng > 77.28:  # East of Yamuna
                    return False
                return True

            elif city_name == 'Bangalore':
                # Bangalore: Generally landlocked, safe
                return True

            # Unknown cities: Be conservative
            return True

        def calculate_distance(lat1, lng1, lat2, lng2):
            """Calculate accurate distance in km using Haversine formula"""
            import math
            R = 6371  # Earth's radius in km

            lat1_rad = math.radians(lat1)
            lat2_rad = math.radians(lat2)
            dlat = math.radians(lat2 - lat1)
            dlng = math.radians(lng2 - lng1)

            a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlng/2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

            return R * c

        def get_nearby_coords(center_lat, center_lng, max_dist_km):
            """
            Generate realistic coordinates within radius.
            - Validates against city boundaries
            - Checks for water bodies / forests
            - Respects actual distance (not approximate)
            - More attempts for stricter validation
            """
            import math

            # Get city boundary if available
            boundary = CITY_BOUNDARIES.get(city, CITY_BOUNDARIES.get('default'))

            max_attempts = 50  # Increased from 20 to 50 for better coverage
            successful_attempts = 0

            for attempt in range(max_attempts):
                # Generate random angle (0 to 360 degrees)
                angle = random.uniform(0, 2 * math.pi)

                # Distance: Use sqrt for uniform circular distribution
                # Ensure minimum distance of 0.3km to avoid too close
                distance = math.sqrt(random.uniform(0.3**2, max_dist_km**2))

                # Convert distance to lat/lng offsets
                # 1 degree latitude ≈ 111km
                # 1 degree longitude ≈ 111km * cos(latitude)
                lat_offset = (distance / 111.0) * math.cos(angle)
                lng_offset = (distance / (111.0 * math.cos(math.radians(center_lat)))) * math.sin(angle)

                new_lat = center_lat + lat_offset
                new_lng = center_lng + lng_offset

                # Calculate actual distance for verification
                actual_distance = calculate_distance(center_lat, center_lng, new_lat, new_lng)

                # Skip if distance miscalculation (should not happen, but safety check)
                if actual_distance > max_dist_km * 1.1:  # 10% tolerance
                    continue

                # Validate against city boundary
                if boundary:
                    min_lat, max_lat, min_lng, max_lng = boundary
                    if not (min_lat <= new_lat <= max_lat and min_lng <= new_lng <= max_lng):
                        continue

                # Validate land check
                if not is_on_land(new_lat, new_lng, city):
                    continue

                # Valid location found!
                return new_lat, new_lng, round(actual_distance, 1)

            # Fallback: If all attempts failed, place near center of valid area
            logger.warning(f"Could not find valid location after {max_attempts} attempts, using fallback")

            if boundary:
                min_lat, max_lat, min_lng, max_lng = boundary
                # Try center of boundary with small random offset
                for _ in range(10):
                    safe_lat = (min_lat + max_lat) / 2 + random.uniform(-0.02, 0.02)
                    safe_lng = (min_lng + max_lng) / 2 + random.uniform(-0.02, 0.02)

                    if is_on_land(safe_lat, safe_lng, city):
                        dist = calculate_distance(center_lat, center_lng, safe_lat, safe_lng)
                        return safe_lat, safe_lng, round(min(dist, max_dist_km), 1)

            # Ultimate fallback: Use center location
            return center_lat, center_lng, 0.5

        # Hotels - Filtered by budget
        all_hotels = [
            {
                'id': 1,
                'name': 'The Taj Majestic',
                'type': 'Hotel Booking',
                'description': 'Luxury 5-star hotel with world-class amenities and spa',
                'rating': 4.8,
                'price_val': 12000,
                'price': '₹12,000/night',
                'budget_cat': 'high',
                'address': f'{city} Downtown',
                'booking_data': { 'destination': city, 'hotel_name': 'The Taj Majestic', 'price_per_night': 12000, 'available_rooms': 15 }
            },
            {
                'id': 2,
                'name': 'Grand Plaza Hotel',
                'type': 'Hotel Booking',
                'description': 'Modern business hotel with premium facilities',
                'rating': 4.4,
                'price_val': 6500,
                'price': '₹6,500/night',
                'budget_cat': 'medium',
                'address': f'{city} City Center',
                'booking_data': { 'destination': city, 'hotel_name': 'Grand Plaza Hotel', 'price_per_night': 6500, 'available_rooms': 20 }
            },
            {
                'id': 10,
                'name': 'City Stay Inn',
                'type': 'Hotel Booking',
                'description': 'Clean and comfortable budget stay',
                'rating': 4.1,
                'price_val': 2500,
                'price': '₹2,500/night',
                'budget_cat': 'low',
                'address': f'{city} Hub',
                'booking_data': { 'destination': city, 'hotel_name': 'City Stay Inn', 'price_per_night': 2500, 'available_rooms': 10 }
            },
            {
                'id': 11,
                'name': 'Royal Palace & Spa',
                'type': 'Hotel Booking',
                'description': 'Exclusive ultra-luxury palace experience',
                'rating': 4.9,
                'price_val': 25000,
                'price': '₹25,000/night',
                'budget_cat': 'premium',
                'address': f'{city} Royal District',
                'booking_data': { 'destination': city, 'hotel_name': 'Royal Palace', 'price_per_night': 25000, 'available_rooms': 5 }
            }
        ]
        
        # Filter hotels by budget
        target_hotels = []
        if budget == 'low':
            target_hotels = [h for h in all_hotels if h['budget_cat'] == 'low']
        elif budget == 'medium':
            target_hotels = [h for h in all_hotels if h['budget_cat'] in ['low', 'medium']]
        elif budget == 'high':
            target_hotels = [h for h in all_hotels if h['budget_cat'] in ['medium', 'high']]
        else: # premium
            target_hotels = [h for h in all_hotels if h['budget_cat'] in ['high', 'premium']]
            
        if not target_hotels: target_hotels = all_hotels[:2] # Fallback

        # Generate nearby instances for hotels
        for h in target_hotels:
            h_lat, h_lng, dist = get_nearby_coords(lat, lng, radius)
            h_copy = h.copy()
            h_copy.update({'lat': h_lat, 'lng': h_lng, 'distance': dist})
            services.append(h_copy)

        # Technicians
        tech_lat, tech_lng, tech_dist = get_nearby_coords(lat, lng, radius)
        services.append({
            'id': 3,
            'name': 'QuickFix Home Services',
            'type': 'Technician Booking',
            'description': 'AC repair, plumbing, electrical - Available 24/7',
            'distance': tech_dist,
            'lat': tech_lat,
            'lng': tech_lng,
            'rating': 4.6,
            'price': '₹500-1,200',
            'address': f'{city} Residential Area',
            'booking_data': { 'technician_id': 'TECH-001', 'service_types': ['AC Repair', 'Plumbing'], 'location': f'{city} Area', 'hourly_rate': 800 }
        })

        # Cars
        all_cars = [
            { 'id': 5, 'name': 'Premium Cab Services', 'type': 'Car Booking', 'description': 'Luxury cabs', 'rating': 4.7, 'price': '₹2,500+', 'budget_cat': 'high', 'model': 'BMW 5 Series', 'class': 'luxury' },
            { 'id': 15, 'name': 'Reliable City Cabs', 'type': 'Car Booking', 'description': 'Comfortable sedans', 'rating': 4.3, 'price': '₹800+', 'budget_cat': 'medium', 'model': 'Toyota Etios', 'class': 'standard' }
        ]
        
        target_cars = [c for c in all_cars if (budget in ['low', 'medium'] and c['budget_cat'] == 'medium') or (budget in ['high', 'premium'] and c['budget_cat'] == 'high')]
        if not target_cars: target_cars = all_cars # Fallback

        for c in target_cars:
            c_lat, c_lng, c_dist = get_nearby_coords(lat, lng, radius)
            services.append({
                'id': c['id'],
                'name': c['name'],
                'type': c['type'],
                'description': c['description'],
                'distance': c_dist,
                'lat': c_lat,
                'lng': c_lng,
                'rating': c['rating'],
                'price': c['price'],
                'address': f'{city} Road',
                'booking_data': { 'cab_class': c['class'], 'vehicle_model': c['model'], 'pickup_location': city, 'base_fare': 1000 }
            })

        # Couriers
        cour_lat, cour_lng, cour_dist = get_nearby_coords(lat, lng, radius)
        services.append({
            'id': 6,
            'name': 'Express Courier Hub',
            'type': 'Courier Booking',
            'description': 'Same-day delivery across the city',
            'distance': cour_dist,
            'lat': cour_lat,
            'lng': cour_lng,
            'rating': 4.5,
            'price': '₹100-500',
            'address': f'{city} Commercial District',
            'booking_data': { 'courier_type': 'express', 'max_weight': 20, 'pickup_location': city, 'price_per_kg': 50 }
        })
        
        # Sort by distance
        services.sort(key=lambda x: x['distance'])
        
        return jsonify({
            'success': True,
            'services': services[:10],
            'user_location': {'lat': lat, 'lng': lng, 'city': city},
            'using_fallback': using_fallback
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

@app.route('/api/chatbot', methods=['POST'])
@login_required
def api_chatbot():
    """Interactive Concierge Chatbot with City Options & Quick Buttons"""
    try:
        data = request.json
        if not data or 'message' not in data:
            return jsonify({'success': False, 'error': 'Message required'}), 400

        user_message = data.get('message', '').strip()
        if not user_message:
            return jsonify({'success': False, 'error': 'Empty message'}), 400

        user_id = current_user.get_id()
        logger.info(f"Chatbot message from user {user_id}: {user_message}")

        # Safe profile loading (handles string/list interests)
        profile = None
        first_name = "there"
        interests = []

        try:
            profile = get_user_profile(user_id)
            if profile:
                raw_name = profile.get('first_name')
                if raw_name and str(raw_name).strip() not in ['None', 'null', '']:
                    first_name = str(raw_name).strip().capitalize()

                raw_interests = profile.get('interests')
                if raw_interests:
                    if isinstance(raw_interests, str):
                        interests = [i.strip().capitalize() for i in raw_interests.split(',') if i.strip()]
                    elif isinstance(raw_interests, (list, tuple)):
                        interests = [str(i).strip().capitalize() for i in raw_interests if str(i).strip()]
        except Exception as e:
            logger.warning(f"Profile error for {user_id}: {e}")

        message_lower = user_message.lower()
        words = set(message_lower.split())

        # === INTERACTIVE RESPONSES WITH OPTIONS ===
        if any(k in message_lower for k in ['hotel', 'stay', 'accommodation', 'room', 'resort']):
            bot_response = (
                f"🏨 Wonderful choice, {first_name}! We specialize in luxury stays in three exclusive destinations:\n\n"
                "Which city would you like to explore?"
            )
            quick_replies = [
                {"title": "🌆 Mumbai", "payload": "hotel_mumbai"},
                {"title": "🏙️ Pune", "payload": "hotel_pune"},
                {"title": "🍷 Nashik", "payload": "hotel_nashik"}
            ]

        elif any(k in message_lower for k in ['flight', 'fly', 'plane', 'ticket']):
            bot_response = (
                "✈️ Ready for takeoff in style?\n\n"
                "We handle domestic & international flights with premium seating.\n"
                "Where would you like to fly from and to?"
            )
            quick_replies = [
                {"title": "Search Flights", "payload": "search_flights"},
                {"title": "Business Class Deals", "payload": "business_class"}
            ]

        elif any(k in message_lower for k in ['cab', 'taxi', 'car', 'ride', 'chauffeur']):
            bot_response = (
                "🚗 Your luxury ride awaits!\n\n"
                "Choose from Mercedes, BMW, Audi, or Limousine with professional chauffeurs."
            )
            quick_replies = [
                {"title": "Airport Transfer", "payload": "airport_cab"},
                {"title": "City Tour", "payload": "city_cab"},
                {"title": "Outstation Trip", "payload": "outstation_cab"}
            ]

        elif any(k in message_lower for k in ['technician', 'repair', 'fix', 'ac', 'plumber', 'electrician']):
            bot_response = (
                "🔧 Expert home care incoming!\n\n"
                "Our verified technicians are available for same-day service."
            )
            quick_replies = [
                {"title": "AC Repair", "payload": "ac_repair"},
                {"title": "Plumbing", "payload": "plumbing"},
                {"title": "Electrical", "payload": "electrical"},
                {"title": "Other Issue", "payload": "other_repair"}
            ]

        elif any(k in message_lower for k in ['courier', 'delivery', 'send', 'package']):
            bot_response = (
                "📦 Lightning-fast delivery!\n\n"
                "Same-day & express options available across cities."
            )
            quick_replies = [
                {"title": "Same-Day Delivery", "payload": "same_day_courier"},
                {"title": "Document Courier", "payload": "document_courier"},
                {"title": "Fragile/Heavy Parcel", "payload": "heavy_courier"}
            ]

        else:
            # General greeting or fallback with main options
            bot_response = (
                f"Hello {first_name}! 👋\n\n"
                "How may I assist you today?"
            )
            quick_replies = [
                {"title": "🏨 Hotels", "payload": "hotels"},
                {"title": "✈️ Flights", "payload": "flights"},
                {"title": "🚗 Luxury Cabs", "payload": "cabs"},
                {"title": "🔧 Home Repairs", "payload": "technician"},
                {"title": "📦 Courier", "payload": "courier"}
            ]

        return jsonify({
            'success': True,
            'response': bot_response,
            'quick_replies': quick_replies,  # Your frontend can render these as buttons!
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Chatbot critical error: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'response': "I'm having a brief technical moment. Please try again or visit Services directly."
        }), 500

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