import os
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
RECIPIENT = os.getenv("RECIPIENT")

def send_email_alert(message: str):
    msg = EmailMessage()
    msg['Subject'] = "🚨 HRIS Pipeline Alert"
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = RECIPIENT
    msg.set_content(message)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
        print("✅ Alert email sent to", RECIPIENT)
    except Exception as e:
        print(f"⚠️ Failed to send alert email: {e}")

if __name__ == "__main__":
    send_email_alert("🚨 Test Alert: Pipeline Failure Simulation.")