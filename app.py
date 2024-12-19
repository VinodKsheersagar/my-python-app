from flask import Flask, request, jsonify
from datetime import datetime, timedelta, timezone
import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import smtplib
from email.mime.text import MIMEText
import logging

app = Flask(__name__)

# Suppress Werkzeug (default HTTP request logger) logs
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# PostgreSQL connection settings
# DATABASE_URL = 'postgresql://postgres:kamareddy@localhost/demo'

DATABASE_URL = 'postgres://ucqg3es2agqojb:p42ec69de2aedfde62abcb9d4532fa9b9548cd78cb7ebdb96dc7d0b8d2d0a322b@c2ihhcf1divl18.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/dpg84b6k55g28'
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set.")

pool = SimpleConnectionPool(1, int(os.environ.get("DB_POOL_MAX", 18)), DATABASE_URL)

# Email settings
EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", 587))
EMAIL_USER = os.environ.get("EMAIL_USER", "sparevikum14@gmail.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "KAmar")
EMAIL_TO = os.environ.get("EMAIL_TO", "ksheersagar12vinod@gmail.com")

if not all([EMAIL_HOST, EMAIL_USER, EMAIL_PASS, EMAIL_TO]):
    raise ValueError("Email configuration environment variables are not set correctly.")

# Create database table
def create_table():
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS priyanka_india_alerts (
                alert_id SERIAL PRIMARY KEY,
                ticker VARCHAR(255) NOT NULL,
                alert_time TIMESTAMP WITH TIME ZONE NOT NULL,
                open_price VARCHAR(255),
                high_price VARCHAR(255),
                low_price VARCHAR(255),
                close_price VARCHAR(255),
                price_change_pct VARCHAR(255),
                volume_change_pct VARCHAR(255)      
            );
        """)
        conn.commit()
        cursor.close()
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise
    finally:
        pool.putconn(conn)

create_table()

# Save alert to the database
def save_alert(ticker, alert_time, open_price, high_price,low_price, close_price, price_change_pct, volume_change_pct):
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        alert_time = datetime.fromtimestamp(alert_time / 1000.0, timezone.utc)  # Convert milliseconds to datetime
        cursor.execute("""
            INSERT INTO priyanka_india_alerts (ticker, alert_time, open_price, high_price,low_price, close_price, price_change_pct, volume_change_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (ticker, alert_time, open_price, high_price,low_price, close_price, price_change_pct, volume_change_pct))
        conn.commit()
        # logging.info(f"Alert saved for {ticker}")
    except Exception as error:
        logging.error(f"Error saving alert: {error}")
        raise
    finally:
        if cursor:
            cursor.close()
        pool.putconn(conn)

# Retrieve recent alerts for a ticker
def get_recent_alerts(ticker, since_time):
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        since_time = datetime.fromtimestamp(since_time / 1000.0, timezone.utc)  # Convert milliseconds to datetime
        cursor.execute("""
            SELECT * FROM priyanka_india_alerts
            WHERE ticker = %s AND alert_time >= %s;
        """, (ticker, since_time))
        results = cursor.fetchall()
        return results
    except Exception as error:
        logging.error(f"Error retrieving alerts: {error}")
        raise
    finally:
        if cursor:
            cursor.close()
        pool.putconn(conn)

# Retrieve all alerts for a specific stock name
def get_all_alerts(ticker):
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM priyanka_india_alerts
            WHERE ticker = %s
            ORDER BY date DESC;
        """, (ticker,))
        results = cursor.fetchall()
        return results
    except Exception as error:
        logging.error(f"Error retrieving all alerts for ticker {ticker}: {error}")
        raise
    finally:
        if cursor:
            cursor.close()
        pool.putconn(conn)

# Send email notification
def send_email(ticker):
    try:
        subject = f"High Activity Alert for {ticker}"
        body = f"More than 3 alerts received for {ticker} within the last 15 minutes."
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(EMAIL_TO)

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, EMAIL_TO, msg.as_string())
        logging.info(f"Email sent for {ticker}")

    except Exception as e:
        logging.error(f"Error sending email: {e}")
        raise

# Flask route to receive webhook alerts
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json()
        # logging.info(data)
        if not data or not all(k in data for k in ["ticker", "alert_time","open_price","high_price","low_price","close_price","price_change_pct","volume_change_pct"]):
            raise ValueError("Invalid payload")

        ticker = data.get("ticker")
        alert_time = int(data.get("alert_time"))  # Retain milliseconds as is
        open_price = data.get("open_price")
        high_price = data.get("high_price")
        low_price = data.get("low_price")
        close_price = data.get("close_price")
        price_change_pct = data.get("price_change_pct")
        volume_change_pct = data.get("volume_change_pct")
       


        if float(close_price) >20 and float(volume_change_pct) > 350 and float(price_change_pct) > 0.35:
            logging.info(ticker +" :Volume and Price both are high, Volume increased by : " +volume_change_pct + " Price increased by : "+ price_change_pct)

        # if float(close_price) >50 and float(volume_change_pct) > 650 and float(price_change_pct) < 0.4:
        #    logging.info(ticker +" :Volume is high,but Price increase is low and Volume increased by : " +volume_change_pct + " Price increased by : "+ price_change_pct)

        #if float(close_price) >50 and float(volume_change_pct) < 450 and float(price_change_pct) > 0.4:
        #    logging.info(ticker +" :Volume is oky,but Price increase is High and Volume increased by : " +volume_change_pct + " Price increased by : "+ price_change_pct)

        # Save alert to the database
        save_alert(ticker, alert_time, open_price, high_price,low_price, close_price, price_change_pct, volume_change_pct)

        # Check for high activity alerts
        since_time = alert_time - 6 * 60 * 1000  # Subtract 15 minutes in milliseconds
        recent_alerts = get_recent_alerts(ticker, since_time)

        if len(recent_alerts) > 2:
            logging.info("High activity alert triggered for : "+ticker +" : stock")
            # send_email(ticker)
            return jsonify({"message": "High activity alert triggered and email sent!"}), 200

        return jsonify({"message": "Alert logged."}), 200
    except ValueError as ve:
        logging.error(f"Validation error: {ve}")
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return jsonify({"error": "Internal server error"}), 500

# Flask route to get all alerts for a specific stock name
@app.route("/alerts/<ticker>", methods=["GET"])
def get_alerts_for_ticker(ticker):
    try:
        alerts = get_all_alerts(ticker)
        return jsonify({"alerts": alerts}), 200
    except Exception as e:
        logging.error(f"Error retrieving alerts for ticker {ticker}: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5500)))
