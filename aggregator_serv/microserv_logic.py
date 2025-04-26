import psycopg2
import boto3
import json
from datetime import datetime

# Load credentials from Secrets Manager
client = boto3.client('secretsmanager', region_name='ap-southeast-1')
secret = client.get_secret_value(SecretId='rs/admin/credentials')
creds = json.loads(secret['SecretString'])

# logics for dim_product warehouse table
def load_dim_product():
    try:
        # Connect to transactional db
        conn_trx = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-tdb'
        )
        cursor_trx = conn_trx.cursor()

        # Connect to warehouse db
        conn_dwh = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-dwh'
        )
        cursor_dwh = conn_dwh.cursor()

        # Truncate dim table
        cursor_dwh.execute("TRUNCATE TABLE dwh.dim_product")

        # Fetch product data from transactional table
        cursor_trx.execute("""
            SELECT product_id, product_name, product_type, target
            FROM tdb.insurance_product
        """)
        rows = cursor_trx.fetchall()
        
        # Insert into dim_product with auto-incrementing product_sk
        for sk, row in enumerate(rows, start=1):
            cursor_dwh.execute("""
                INSERT INTO dwh.dim_product (product_sk, product_id, product_name, product_type, target)
                VALUES (%s, %s, %s, %s, %s)
            """, (sk, row[0], row[1], row[2], row[3]))

        conn_dwh.commit()
        conn_trx.close()
        conn_dwh.close()

        print("load_dim_product completed successfully.")

    except Exception as e:
        print(f"Error in load_dim_product: {e}")

# logics for dim_agent warehouse table
def load_dim_agent():
    try:
        # Connect to transactional db
        conn_trx = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-tdb'
        )
        cursor_trx = conn_trx.cursor()

        # Connect to data warehouse db
        conn_dwh = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-dwh'
        )
        cursor_dwh = conn_dwh.cursor()

        # Truncate dim table
        cursor_dwh.execute("TRUNCATE TABLE dwh.dim_agent")

        # Fetch joined agent + team + branch data from transactional tables
        cursor_trx.execute("""
            SELECT 
                a.id,
                a.first_name || ' ' || a.last_name AS agent_name,
                t.team_name,
                b.branch_name
            FROM tdb.agent a
            INNER JOIN tdb.team t ON a.team_id = t.team_id
            INNER JOIN tdb.branch b ON t.branch_id = b.branch_id
        """)
        rows = cursor_trx.fetchall()

        # Insert into dim_agent with auto-incrementing agent_sk
        for sk, row in enumerate(rows, start=1):
            cursor_dwh.execute("""
                INSERT INTO dwh.dim_agent (agent_sk, id, agent_name, team_name, branch_name)
                VALUES (%s, %s, %s, %s, %s)
            """, (sk, row[0], row[1], row[2], row[3]))

        conn_dwh.commit()
        conn_trx.close()
        conn_dwh.close()

        print("load_dim_agent completed successfully.")

    except Exception as e:
        print(f"Error in load_dim_agent: {e}")

# logics for fact_sales warehouse table
def load_fact_sales():
    try:
        # Connect to transactional db
        conn_trx = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-tdb'
        )
        cursor_trx = conn_trx.cursor()

        # Connect to data warehouse db
        conn_dwh = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-dwh'
        )
        cursor_dwh = conn_dwh.cursor()

        # Truncate fact table
        cursor_dwh.execute("TRUNCATE TABLE dwh.fact_sales")

        # Fetch sales data from transactional table
        cursor_trx.execute("""
            SELECT sale_id, sale_date, id, product_id, amount
            FROM tdb.sale
        """)
        sales = cursor_trx.fetchall()

        # Build in-memory key maps for surrogate lookups
        cursor_dwh.execute("SELECT date, date_sk FROM dwh.dim_date")
        date_map = {row[0]: row[1] for row in cursor_dwh.fetchall()}

        cursor_dwh.execute("SELECT product_id, product_sk FROM dwh.dim_product")
        product_map = {row[0]: row[1] for row in cursor_dwh.fetchall()}

        cursor_dwh.execute("SELECT id, agent_sk FROM dwh.dim_agent")
        agent_map = {row[0]: row[1] for row in cursor_dwh.fetchall()}

        # Insert into fact_sales using surrogate keys
        for sale in sales:
            sale_id, sale_date, id, product_id, amount = sale
            date_sk = date_map.get(sale_date)
            product_sk = product_map.get(product_id)
            agent_sk = agent_map.get(id)

            if date_sk and product_sk and agent_sk:
                cursor_dwh.execute("""
                    INSERT INTO dwh.fact_sales (sale_id, date_sk, product_sk, agent_sk, amount)
                    VALUES (%s, %s, %s, %s, %s)
                """, (sale_id, date_sk, product_sk, agent_sk, amount))

        conn_dwh.commit()
        conn_trx.close()
        conn_dwh.close()

        print("load_fact_sales completed successfully.")

    except Exception as e:
        print(f"Error in load_fact_sales: {e}")

# Load sales_team_aggr aggregated sales table
def load_sales_team_aggr():
    try:
        # Connect to data warehouse
        conn_dwh = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-dwh'
        )
        cursor = conn_dwh.cursor()

        # Truncate aggregation table
        cursor.execute("TRUNCATE TABLE dwh.sales_team_aggr")

        # Aggregate sales by team_name
        cursor.execute("""
            SELECT da.team_name, SUM(fs.amount) AS total_sale_amount
            FROM dwh.fact_sales fs
            INNER JOIN dwh.dim_agent da ON fs.agent_sk = da.agent_sk
            GROUP BY da.team_name
            ORDER BY total_sale_amount DESC
        """)

        team_sales = cursor.fetchall()

        # Insert aggregated data into sales_team_aggr
        for row in team_sales:
            cursor.execute("""
                INSERT INTO dwh.sales_team_aggr (team_name, total_sale_amount)
                VALUES (%s, %s)
            """, (row[0], row[1]))

        conn_dwh.commit()
        conn_dwh.close()

        print("load_sales_team_aggr completed successfully.")

    except Exception as e:
        print(f"Error in load_sales_team_aggr: {e}")

# Load dwh.sales_branch_aggr aggregated sales table
def load_sales_branch_aggr():
    try:
        # Connect to data warehouse
        conn_dwh = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-dwh'
        )
        cursor = conn_dwh.cursor()

        # Truncate aggregation table
        cursor.execute("TRUNCATE TABLE dwh.sales_branch_aggr")

        # Aggregate sales by branch_name
        cursor.execute("""
            SELECT da.branch_name, SUM(fs.amount) AS total_sale_amount
            FROM dwh.fact_sales fs
            INNER JOIN dwh.dim_agent da ON fs.agent_sk = da.agent_sk
            GROUP BY da.branch_name
            ORDER BY total_sale_amount DESC
        """)

        branch_sales = cursor.fetchall()

        # Insert aggregated data into sales_branch_aggr
        for row in branch_sales:
            cursor.execute("""
                INSERT INTO dwh.sales_branch_aggr (branch_name, total_sale_amount)
                VALUES (%s, %s)
            """, (row[0], row[1]))

        conn_dwh.commit()
        conn_dwh.close()

        print("load_sales_branch_aggr completed successfully.")

    except Exception as e:
        print(f"Error in load_sales_branch_aggr: {e}")

# Load dwh.sales_product_aggr agg sales table
def load_sales_product_aggr():
    try:
        # Connect to data warehouse
        conn_dwh = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['username'],
            password=creds['password'],
            dbname='mat-dwh'
        )
        cursor = conn_dwh.cursor()

        # Truncate agg table
        cursor.execute("TRUNCATE TABLE dwh.sales_product_aggr")

        # Aggregate sales with target from product dimension
        cursor.execute("""
            SELECT dp.product_name, SUM(fs.amount) AS total_sale_amount, dp.target
            FROM dwh.fact_sales fs
            INNER JOIN dwh.dim_product dp ON fs.product_sk = dp.product_sk
            GROUP BY dp.product_name, dp.target
            ORDER BY total_sale_amount DESC
        """)

        product_sales = cursor.fetchall()

        # Insert aggregated data into dwh.sales_product_aggr
        for row in product_sales:
            product_name, total_sale_amount, target = row
            status = 'YES' if total_sale_amount >= target else 'NO'

            cursor.execute("""
                INSERT INTO dwh.sales_product_aggr (
                    product_name, total_sale_amount, target, sales_target_achieved
                ) VALUES (%s, %s, %s, %s)
            """, (product_name, total_sale_amount, target, status))

        conn_dwh.commit()
        conn_dwh.close()

        print("load_sales_product_aggr completed successfully.")

    except Exception as e:
        print(f"Error in load_sales_product_aggr: {e}")

#Load agg sales tables in data warehouse
def run_aggregator_service():
    print(f"Aggregation Job Started at {datetime.now()}")

    # ETL process for DWH
    load_dim_product()
    load_dim_agent()
    load_fact_sales()

    # Load aggregated sales tables in DWH
    load_sales_team_aggr()
    load_sales_branch_aggr()
    load_sales_product_aggr()

    print(f"Aggregation Job Completed at {datetime.now()}")