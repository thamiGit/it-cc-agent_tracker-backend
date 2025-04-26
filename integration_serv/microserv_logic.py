from flask import Blueprint, request, jsonify
import psycopg2
import json
import boto3

# Route blueprint definition
integration_bp = Blueprint('integration_bp', __name__)

# Load credentials from Secrets Manager
client = boto3.client('secretsmanager', region_name='ap-southeast-1')
secret = client.get_secret_value(SecretId='rs/admin/credentials')
creds = json.loads(secret['SecretString'])

# Endpoint to insert/update sales data
@integration_bp.route('/sales/upsert', methods=['POST'])
def insert_or_update_sale():
    try:
        data = request.get_json()
        sale_id = data['sale_id']
        sale_date = data['sale_date']
        id = data['id']
        product_id = data['product_id']
        amount = data['amount']

        conn = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-tdb'
        )
        cursor = conn.cursor()

        # Check if the agent is permitted to sell the product
        cursor.execute("""
            SELECT 1 FROM tdb.agent_product
            WHERE id = %s AND product_id = %s
        """, (id, product_id))
        if cursor.fetchone() is None:
            return jsonify({
                "message": f"Agent '{id}' is not permitted to sell product '{product_id}'."
            }), 403

        # Update if sale_id exists, else insert
        cursor.execute("""
            UPDATE tdb.sale
            SET sale_date = %s,
                id = %s,
                product_id = %s,
                amount = %s
            WHERE sale_id = %s
        """, (sale_date, id, product_id, amount, sale_id))

        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO tdb.sale (sale_id, sale_date, id, product_id, amount)
                VALUES (%s, %s, %s, %s, %s)
            """, (sale_id, sale_date, id, product_id, amount))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Sales record inserted/updated successfully."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500