import pdfplumber
import pandas as pd
import warnings
from datetime import datetime
import sys
import re
import smtplib
import imaplib
import email
import os
import shutil
import mysql.connector
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Suppress warnings
warnings.filterwarnings("ignore", message="CropBox missing from /Page")

# Configuration
CONFIG = {
    "imap_server": "mail.myfleetview.info",
    "imap_port": 993,
    "smtp_server": "mail.myfleetview.info",
    "smtp_port": 587,
    "sender_email": "jd@mobile-eye.in",
    "sender_password": "transworld@123",
    "receiver_email": "jd@mobile-eye.in,p_khedkar@twtech.in,swintern@twtech.in,javadev1@twtech.in,mgrsoft@twtech.in",
    "target_subject": "GRN",
    "download_folder": "/home/twtech/BAGIC_ITC/GRN/GRN_downloaded_pdfs",
    "backup_folder": "/home/twtech/BAGIC_ITC/GRN/GRN_processed_backups",
    "default_pdf": "Auto Mail Generation.PDF",
    "mysql_host": "94.136.185.184",
    "mysql_user": "fleetview",
    "mysql_password": "1@flv",
    "mysql_database": "db_ITC",
    "mysql_table": "t_GRN_Data",
    "trash_folder": "Trash"
}

def create_folders():
    """Ensure required folders exist"""
    for folder in [CONFIG['download_folder'], CONFIG['backup_folder']]:
        if not os.path.exists(folder):
            os.makedirs(folder)

# def get_mysql_connection():
#     """Create and return MySQL connection"""
#     try:
#         conn = mysql.connector.connect(
#             host=CONFIG['mysql_host'],
#             user=CONFIG['mysql_user'],
#             password=CONFIG['mysql_password'],
#             database=CONFIG['mysql_database']
#         )
#         return conn
#     except mysql.connector.Error as err:
#         print(f"MySQL connection error: {err}")
#         return None

import time  # Required for retry delay

def get_mysql_connection(retries=3, delay=5):
    """
    Establish a connection to the MySQL database with retry logic.

    Parameters:
        retries (int): Number of times to retry the connection.
        delay (int): Seconds to wait between retries.

    Returns:
        mysql.connector.connection.MySQLConnection or None: Connection object if successful, None if all retries fail.
    """
    for attempt in range(retries):
        try:
            # Attempt to create a new MySQL connection using configuration
            conn = mysql.connector.connect(
                host=CONFIG['mysql_host'],
                user=CONFIG['mysql_user'],
                password=CONFIG['mysql_password'],
                database=CONFIG['mysql_database'],
                connection_timeout=10  # Prevents indefinite hanging if server is unresponsive
            )
            print("MySQL connection successful")
            return conn  # Connection established successfully
        except mysql.connector.Error as err:
            # Print the error and retry if attempts remain
            print(f"MySQL connection attempt {attempt + 1} failed: {err}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Wait before retrying
            else:
                # All retries exhausted, return None
                print("All MySQL connection attempts failed.")
                return None


def create_table_if_not_exists(conn):
    """Create shipments table if it doesn't exist"""
    try:
        cursor = conn.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {CONFIG['mysql_table']} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sl_no VARCHAR(20),
            grn_date DATE,
            grn_time TIME,
            source VARCHAR(100),
            destination VARCHAR(100),
            shipment_number VARCHAR(50),
            transporter VARCHAR(100),
            truck_number VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

def insert_data_to_mysql(conn, data):
    """Insert data into MySQL table"""
    try:
        cursor = conn.cursor()
        insert_query = f"""
        INSERT INTO {CONFIG['mysql_table']} (
            sl_no, grn_date, grn_time, source, 
            destination, shipment_number, transporter, truck_number
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        records = []
        for _, row in data.iterrows():
            try:
                record = (
                    str(row['S1. No.']),
                    row['grn_date'],
                    row['grn_time'],
                    str(row['Source']),
                    str(row['Destination']),
                    str(row['Shipment Number']),
                    str(row['Transporter']),
                    str(row['Truck Number']) if pd.notna(row['Truck Number']) else None
                )
                records.append(record)
            except Exception as e:
                print(f"Error processing row {row}: {str(e)}")
                continue
        
        if records:
            cursor.executemany(insert_query, records)
            conn.commit()
            print(f"Inserted {len(records)} records into MySQL")
        cursor.close()
        return True
    except mysql.connector.Error as err:
        print(f"Error inserting data: {err}")
        conn.rollback()
        return False

def parse_pdf_data(pdf_path):
    """Extract GRN data from PDF with table structure"""
    data = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Extract table data
            table = page.extract_table()
            
            if not table:
                print("No table found on page")
                continue
                
            # Skip header row if it exists
            start_row = 1 if table[0][0] in ["S1. No.", "Sl. No."] else 0
            
            for row in table[start_row:]:
                # Skip empty rows or rows with missing data
                if not row or len(row) < 8:
                    continue
                    
                # Skip rows that might be headers or footers
                if row[0] in ["S1. No.", "Sl. No.", "GRN Date"] or not row[0].strip().isdigit():
                    continue
                    
                try:
                    parsed = {
                        'S1. No.': row[0].strip() if row[0] else None,
                        'grn_date': row[1].strip() if row[1] else None,
                        'grn_time': row[2].strip() if row[2] else None,
                        'Source': row[3].strip() if row[3] else None,
                        'Destination': row[4].strip() if row[4] else None,
                        'Shipment Number': row[5].strip() if row[5] else None,
                        'Transporter': row[6].strip() if row[6] else None,
                        'Truck Number': row[7].strip() if row[7] else None
                    }
                    data.append(parsed)
                except Exception as e:
                    print(f"Error parsing row: {row}\nError: {str(e)}")
                    continue
    
    print(f"\nPDF parsing complete. {len(data)} records found.")
    
    if not data:
        print("Warning: No data was extracted from the PDF")
        return pd.DataFrame()
    
    # Create DataFrame and convert date format
    df = pd.DataFrame(data)
    try:
        # Convert date format and handle errors
        df['grn_date'] = pd.to_datetime(df['grn_date'], format='%d.%m.%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        # Drop rows where date conversion failed
        df = df.dropna(subset=['grn_date'])
        # Convert time format
        df['grn_time'] = pd.to_datetime(df['grn_time'], format='%H:%M:%S', errors='coerce').dt.strftime('%H:%M:%S')
        df = df.dropna(subset=['grn_time'])
    except Exception as e:
        print(f"Date/Time conversion error: {str(e)}")
    
    return df

def send_email(subject, body, attachment_path=None):
    """Send email with attachment"""
    try:
        msg = MIMEMultipart()
        msg['From'] = CONFIG['sender_email']
        msg['To'] = CONFIG['receiver_email']
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        if attachment_path:
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="{os.path.basename(attachment_path)}"'
            )
            msg.attach(part)
        
        with smtplib.SMTP(CONFIG['smtp_server'], CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(CONFIG['sender_email'], CONFIG['sender_password'])
            server.send_message(msg)
        print(f"Email sent successfully with subject: {subject}")
        return True
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        return False

def move_email_to_trash(mail, email_id):
    """Move email to trash folder"""
    try:
        # Copy the email to Trash folder
        result = mail.copy(email_id, CONFIG['trash_folder'])
        if result[0] == 'OK':
            # Mark the original for deletion
            mail.store(email_id, '+FLAGS', '\\Deleted')
            mail.expunge()
            return True
        else:
            print(f"Failed to copy email to trash: {result}")
            return False
    except Exception as e:
        print(f"Error moving email to trash: {str(e)}")
        return False

def download_attachments():
    """Download PDF attachments from emails with 'GRN' in subject"""
    try:
        mail = imaplib.IMAP4_SSL(CONFIG['imap_server'], CONFIG['imap_port'])
        mail.login(CONFIG['sender_email'], CONFIG['sender_password'])
        mail.select('inbox')
        
        # Search for emails with subject containing "GRN"
        status, messages = mail.search(None, f'(SUBJECT "{CONFIG["target_subject"]}")')
        if status != "OK":
            print("Error searching emails")
            return []
        
        email_ids = messages[0].split()
        downloaded_files = []
        
        print(f"Found {len(email_ids)} emails with matching subject")
        
        for email_id in email_ids:
            try:
                # Convert email_id to string if it's bytes
                email_id_str = email_id.decode('utf-8') if isinstance(email_id, bytes) else str(email_id)
                
                # Skip invalid email IDs
                if not email_id_str.isdigit():
                    print(f"Skipping invalid email ID: {email_id}")
                    continue
                
                status, msg_data = mail.fetch(email_id_str, '(RFC822)')
                if status != "OK":
                    print(f"Error fetching email {email_id_str}")
                    continue
                
                if not msg_data or not isinstance(msg_data[0], tuple):
                    print(f"Invalid message data for email {email_id_str}")
                    continue
                
                raw_email = msg_data[0][1]
                if not raw_email:
                    print(f"Empty message content for email {email_id_str}")
                    continue
                
                msg = email.message_from_bytes(raw_email)
                subject = msg['subject']
                print(f"Processing email with subject: {subject}")
                
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get('Content-Disposition') is None:
                        continue
                    
                    filename = part.get_filename()
                    if filename and filename.lower().endswith('.pdf'):
                        filepath = os.path.join(CONFIG['download_folder'], filename)
                        with open(filepath, 'wb') as f:
                            f.write(part.get_payload(decode=True))
                        downloaded_files.append(filepath)
                        print(f"Downloaded: {filename}")
                
                # Move to trash instead of just deleting
                if move_email_to_trash(mail, email_id_str):
                    print(f"Moved email to trash: {subject}")
                else:
                    print(f"Failed to move email to trash: {subject}")
                    # Fallback to regular deletion
                    mail.store(email_id_str, '+FLAGS', '\\Deleted')
                    mail.expunge()
                    print("Email marked for deletion instead")
                
            except Exception as e:
                print(f"Error processing email {email_id}: {str(e)}")
                continue
        
        mail.close()
        mail.logout()
        
        return downloaded_files
    
    except Exception as e:
        print(f"Error processing emails: {str(e)}")
        return []

def move_to_backup(*files):
    """Move processed files to backup folder"""
    for filepath in files:
        if os.path.exists(filepath):
            try:
                dest = os.path.join(CONFIG['backup_folder'], os.path.basename(filepath))
                shutil.move(filepath, dest)
                print(f"Moved to backup: {os.path.basename(filepath)}")
            except Exception as e:
                print(f"Error moving file {filepath}: {str(e)}")

def process_pdf_file(pdf_path):
    """Process a single PDF file"""
    print(f"\nProcessing PDF: {pdf_path}")
    
    try:
        df = parse_pdf_data(pdf_path)
        
        if df.empty:
            print("No data was extracted from the PDF.")
            return False
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        
        # Generate output filenames
        csv_file = f"{base_name}_{timestamp}.csv"
        excel_file = f"{base_name}_{timestamp}.xlsx"
        
        # Save outputs
        df.to_csv(csv_file, index=False)
        print(f"Saved CSV: {csv_file}")
        
        excel_success = False
        try:
            import openpyxl
            df.to_excel(excel_file, index=False, engine='openpyxl')
            print(f"Saved Excel: {excel_file}")
            excel_success = True
        except ImportError:
            print("Note: openpyxl not installed - skipping Excel export")
        except Exception as e:
            print(f"Excel export error: {str(e)}")
        
        # Insert data into MySQL
        mysql_success = False
        conn = get_mysql_connection()
        if conn:
            create_table_if_not_exists(conn)
            mysql_success = insert_data_to_mysql(conn, df)
            conn.close()
        
        # Send email and move to backup if successful
        current_date = datetime.now().strftime('%d.%m.%y')
        subject = f"GRN Data Processing Report - {current_date}"
        body = f"""GRN data processing complete:
- PDF processed: {os.path.basename(pdf_path)}
- Records extracted: {len(df)}
- CSV file: {csv_file}
- Excel file: {excel_file if excel_success else 'Not generated'}
- MySQL records inserted: {'Success' if mysql_success else 'Failed'}
"""
        
        email_sent = send_email(subject, body, csv_file)
        if excel_success:
            send_email(subject, body, excel_file)
        
        if email_sent and mysql_success:
            files_to_backup = [pdf_path, csv_file]
            if excel_success:
                files_to_backup.append(excel_file)
            move_to_backup(*files_to_backup)
        
        return True
        
    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        create_folders()
        
        print("Checking for emails with GRN data...")
        pdf_files = download_attachments()
        
        if not pdf_files:
            print("No PDF attachments found with GRN in subject.")
            if os.path.exists(CONFIG['default_pdf']):
                print(f"Using default PDF file: {CONFIG['default_pdf']}")
                pdf_files = [CONFIG['default_pdf']]
            else:
                print(f"Default PDF file not found: {CONFIG['default_pdf']}")
                sys.exit(1)
        
        for pdf_path in pdf_files:
            if not os.path.exists(pdf_path):
                print(f"PDF file not found: {pdf_path}")
                continue
                
            if not process_pdf_file(pdf_path):
                print(f"Failed to process PDF: {pdf_path}")
        
        print("\nProcessing complete!")
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
    finally:
        sys.exit(0)
