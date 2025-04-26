from flask import Blueprint, request, jsonify
import psycopg2
import json
import boto3

# Route blueprint definition
agent_bp = Blueprint('agent_bp', __name__)

# Load DB credentials from Secrets Manager
client = boto3.client('secretsmanager', region_name='ap-southeast-1')
secret = client.get_secret_value(SecretId='rs/admin/credentials')
creds = json.loads(secret['SecretString'])

# Endpoint to insert/update agent data and permitted products
@agent_bp.route('/agentserv/upsert', methods=['POST'])
def insert_or_update_agent():
    try:
        data = request.get_json()
        agent_info = data['agent_information']
        products = data['products']

        conn = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-tdb'
        )
        cursor = conn.cursor()

        # Update the existing agent
        cursor.execute("""
            UPDATE tdb.agent
            SET agent_code = %s,
                first_name = %s,
                last_name = %s,
                email = %s,
                phone = %s,
                team_id = %s
            WHERE id = %s
            """, (
            agent_info['agent_code'],
            agent_info['first_name'],
            agent_info['last_name'],
            agent_info['email'],
            agent_info['phone'],
            agent_info['team_id'],
            agent_info['id']
            )
        )

        # If no agent rows were updated, insert a new one
        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO tdb.agent (id, agent_code, first_name, last_name, email, phone, team_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    agent_info['id'],
                    agent_info['agent_code'],
                    agent_info['first_name'],
                    agent_info['last_name'],
                    agent_info['email'],
                    agent_info['phone'],
                    agent_info['team_id']
                )
            )

        # Insert agent-product mappings only if not already present
        for product_id in products:
            cursor.execute("""
                SELECT 1 FROM tdb.agent_product WHERE id = %s AND product_id = %s
            """, (agent_info['id'], product_id))
            exists = cursor.fetchone()
            if not exists:
                cursor.execute("""
                    INSERT INTO tdb.agent_product (id, product_id)
                    VALUES (%s, %s)
                """, (agent_info['id'], product_id))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Agent data inserted/updated successfully."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500