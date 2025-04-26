from flask import Blueprint, jsonify
import psycopg2
import json
import boto3

# Route blueprint definition
notification_bp = Blueprint('notification_bp', __name__)

# Load DB credentials from Secrets Manager
client = boto3.client('secretsmanager', region_name='ap-southeast-1')
secret = client.get_secret_value(SecretId='rs/admin/credentials')
creds = json.loads(secret['SecretString'])

# Endpoint to generate sales alert notifications
# Polling method used in notification generation
# The front-end will call this endpoint in regular interals(every 5 seconds), if a notification is sent then will be displayed in the UI
@notification_bp.route('/sales/alert', methods=['GET'])
def target_alert():
    try:
        conn = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-tdb'
        )
        cursor = conn.cursor()

        # Check for products that met or exceeded their sales target
        cursor.execute("""
            SELECT p.product_name, SUM(s.amount) AS total_sales, p.target
            FROM tdb.sale s
            INNER JOIN tdb.insurance_product p ON s.product_id = p.product_id
            GROUP BY p.product_name, p.target
            HAVING SUM(s.amount) >= p.target
            ORDER BY p.product_name;
        """)
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        # Generate notifications
        if results:
            notifications = [
                f"Sales target achieved for product '{row[0]}': "
                f"Target = {row[2]}, Total Sales = {row[1]}"
                for row in results
            ]
            return jsonify({"notifications": notifications}), 200
        else:
            return jsonify({"notifications": None}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500