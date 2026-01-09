"""
ETL Pipeline: PostgreSQL to MySQL Data Warehouse
Author: Data Engineering Team
Description: Extracts data from PostgreSQL, transforms it, and loads to MySQL every 6 hours
"""

from datetime import datetime, timedelta, date
import logging
import re
from typing import List, Dict, Any

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException

# Setup logging
logger = logging.getLogger(__name__)

# Default arguments for DAG
default_args = {
    'owner': 'data-engineering-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
}

# Define DAG
dag = DAG(
    'postgres_to_mysql_etl',
    default_args=default_args,
    description='ETL pipeline from PostgreSQL to MySQL',
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'postgresql', 'mysql', 'data-pipeline'],
    max_active_runs=1,
)


# ==================== EXTRACTION FUNCTIONS ====================

def extract_customers_from_postgres(**context) -> None:
    """
    Extract customer data from PostgreSQL
    
    Requirements:
    - Use PostgresHook for connection
    - SELECT all fields from raw_data.customers
    - Filter: WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
    - Convert to list of dictionaries
    - Push to XCom with key 'customers_data'
    """
    try:
        logger.info("Starting extraction of customers from PostgreSQL")
        
        # Initialize PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_raw_data')
        
        # SQL query with incremental load (last 1 day)
        sql_query = """
            SELECT 
                customer_id,
                first_name,
                last_name,
                email,
                phone,
                address,
                city,
                state,
                zip_code,
                created_at,
                updated_at
            FROM raw_data.customers
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
            ORDER BY customer_id
        """
        
        # Execute query and fetch data
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Convert to list of dictionaries with datetime serialization
        customers_data = []
        for row in cursor.fetchall():
            customer_dict = dict(zip(columns, row))
            
            # Convert datetime objects to ISO format string for XCom serialization
            for key, value in customer_dict.items():
                if isinstance(value, datetime):
                    customer_dict[key] = value.isoformat()
            
            customers_data.append(customer_dict)
        
        # Close connections
        cursor.close()
        connection.close()
        
        logger.info(f"Extracted {len(customers_data)} customers from PostgreSQL")
        
        # Push to XCom with key 'customers_data'
        context['task_instance'].xcom_push(key='customers_data', value=customers_data)
        
        logger.info("Successfully pushed customers data to XCom")
        
    except Exception as e:
        logger.error(f"Error extracting customers from PostgreSQL: {str(e)}")
        raise AirflowException(f"Failed to extract customers: {str(e)}")


def extract_products_from_postgres(**context) -> None:
    """
    Extract product data from PostgreSQL
    
    Requirements:
    - Use PostgresHook for connection
    - SELECT fields from raw_data.products
    - JOIN with raw_data.suppliers to get supplier_name
    - Filter: WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
    - Convert to list of dictionaries
    - Push to XCom with key 'products_data'
    """
    try:
        logger.info("Starting extraction of products from PostgreSQL")
        
        # Initialize PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_raw_data')
        
        # SQL query with JOIN and incremental load
        sql_query = """
            SELECT 
                p.product_id,
                p.product_name,
                p.supplier_id,
                s.supplier_name,
                p.category,
                p.price,
                p.cost,
                p.stock_quantity,
                p.created_at,
                p.updated_at
            FROM raw_data.products p
            LEFT JOIN raw_data.suppliers s ON p.supplier_id = s.supplier_id
            WHERE p.updated_at >= CURRENT_DATE - INTERVAL '1 day'
            ORDER BY p.product_id
        """
        
        # Execute query and fetch data
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Convert to list of dictionaries with datetime serialization
        products_data = []
        for row in cursor.fetchall():
            product_dict = dict(zip(columns, row))
            
            # Convert datetime objects to ISO format string
            for key, value in product_dict.items():
                if isinstance(value, datetime):
                    product_dict[key] = value.isoformat()
            
            products_data.append(product_dict)
        
        # Close connections
        cursor.close()
        connection.close()
        
        logger.info(f"Extracted {len(products_data)} products from PostgreSQL")
        
        # Push to XCom with key 'products_data'
        context['task_instance'].xcom_push(key='products_data', value=products_data)
        
        logger.info("Successfully pushed products data to XCom")
        
    except Exception as e:
        logger.error(f"Error extracting products from PostgreSQL: {str(e)}")
        raise AirflowException(f"Failed to extract products: {str(e)}")


def extract_orders_from_postgres(**context) -> None:
    """
    Extract order data from PostgreSQL
    
    Requirements:
    - Use PostgresHook for connection
    - SELECT all fields from raw_data.orders
    - Filter: WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
    - Convert to list of dictionaries
    - Push to XCom with key 'orders_data'
    """
    try:
        logger.info("Starting extraction of orders from PostgreSQL")
        
        # Initialize PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_raw_data')
        
        # SQL query with incremental load
        sql_query = """
            SELECT 
                order_id,
                customer_id,
                product_id,
                quantity,
                unit_price,
                total_amount,
                order_date,
                status,
                created_at,
                updated_at
            FROM raw_data.orders
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
            ORDER BY order_id
        """
        
        # Execute query and fetch data
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Convert to list of dictionaries with datetime serialization
        orders_data = []
        for row in cursor.fetchall():
            order_dict = dict(zip(columns, row))
            
            # Convert datetime/date objects to ISO format string
            for key, value in order_dict.items():
                if isinstance(value, (datetime, date)):
                    order_dict[key] = value.isoformat()
            
            orders_data.append(order_dict)
        
        # Close connections
        cursor.close()
        connection.close()
        
        logger.info(f"Extracted {len(orders_data)} orders from PostgreSQL")
        
        # Push to XCom with key 'orders_data'
        context['task_instance'].xcom_push(key='orders_data', value=orders_data)
        
        logger.info("Successfully pushed orders data to XCom")
        
    except Exception as e:
        logger.error(f"Error extracting orders from PostgreSQL: {str(e)}")
        raise AirflowException(f"Failed to extract orders: {str(e)}")


# ==================== TRANSFORMATION & LOADING FUNCTIONS ====================

def transform_and_load_customers(**context) -> None:
    """
    Transform customer data and load to MySQL
    
    Transformations:
    - Phone: Remove non-digit characters, format as (XXX) XXX-XXXX
    - State: Convert to UPPERCASE
    
    Loading:
    - Use MySqlHook
    - UPSERT to dim_customers (INSERT ... ON DUPLICATE KEY UPDATE)
    """
    try:
        logger.info("Starting transformation and loading of customers")
        
        # Pull data from XCom
        customers = context['task_instance'].xcom_pull(
            task_ids='extract_customers',
            key='customers_data'
        )
        
        if not customers:
            logger.warning("No customer data found in XCom")
            return
        
        logger.info(f"Processing {len(customers)} customers")
        
        # Transform data
        transformed_customers = []
        for customer in customers:
            transformed_customer = customer.copy()
            
            # Transform phone: Remove non-digit characters, format as (XXX) XXX-XXXX
            phone = str(customer.get('phone', ''))
            digits = re.sub(r'\D', '', phone)
            
            if len(digits) == 10:
                formatted_phone = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            else:
                formatted_phone = phone  # Keep original if not 10 digits
            
            transformed_customer['phone'] = formatted_phone
            
            # Transform state: Convert to UPPERCASE
            state = str(customer.get('state', ''))
            transformed_customer['state'] = state.upper()
            
            transformed_customers.append(transformed_customer)
        
        # Initialize MySQL connection
        mysql_hook = MySqlHook(mysql_conn_id='mysql_data_warehouse')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        
        # UPSERT to dim_customers (INSERT ... ON DUPLICATE KEY UPDATE)
        upsert_query = """
            INSERT INTO dim_customers (
                customer_id, first_name, last_name, email, phone,
                address, city, state, zip_code, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                first_name = VALUES(first_name),
                last_name = VALUES(last_name),
                email = VALUES(email),
                phone = VALUES(phone),
                address = VALUES(address),
                city = VALUES(city),
                state = VALUES(state),
                zip_code = VALUES(zip_code),
                updated_at = VALUES(updated_at),
                etl_timestamp = CURRENT_TIMESTAMP
        """
        
        # Prepare data for insertion
        insert_data = []
        for customer in transformed_customers:
            row = (
                customer['customer_id'],
                customer['first_name'],
                customer['last_name'],
                customer['email'],
                customer['phone'],
                customer['address'],
                customer['city'],
                customer['state'],
                customer['zip_code'],
                customer['created_at'],
                customer['updated_at']
            )
            insert_data.append(row)
        
        # Execute batch insert
        cursor.executemany(upsert_query, insert_data)
        connection.commit()
        
        affected_rows = cursor.rowcount
        
        # Close connections
        cursor.close()
        connection.close()
        
        logger.info(f"Successfully loaded {affected_rows} customer records to MySQL")
        
    except Exception as e:
        logger.error(f"Error transforming/loading customers: {str(e)}")
        raise AirflowException(f"Failed to transform/load customers: {str(e)}")


def transform_and_load_products(**context) -> None:
    """
    Transform product data and load to MySQL
    
    Transformations:
    - Profit margin: Calculate ((price - cost) / price) * 100
    - Category: Convert to Title Case
    
    Loading:
    - Use MySqlHook
    - UPSERT to dim_products
    """
    try:
        logger.info("Starting transformation and loading of products")
        
        # Pull data from XCom
        products = context['task_instance'].xcom_pull(
            task_ids='extract_products',
            key='products_data'
        )
        
        if not products:
            logger.warning("No product data found in XCom")
            return
        
        logger.info(f"Processing {len(products)} products")
        
        # Transform data
        transformed_products = []
        for product in products:
            transformed_product = product.copy()
            
            # Calculate profit margin: ((price - cost) / price) * 100
            price = float(product.get('price', 0))
            cost = float(product.get('cost', 0))
            
            if price > 0:
                profit_margin = ((price - cost) / price) * 100
                profit_margin = round(profit_margin, 2)
            else:
                profit_margin = 0.0
            
            transformed_product['profit_margin'] = profit_margin
            
            # Transform category: Convert to Title Case
            category = str(product.get('category', ''))
            transformed_product['category'] = category.title()
            
            transformed_products.append(transformed_product)
        
        # Initialize MySQL connection
        mysql_hook = MySqlHook(mysql_conn_id='mysql_data_warehouse')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        
        # UPSERT to dim_products
        upsert_query = """
            INSERT INTO dim_products (
                product_id, product_name, supplier_id, supplier_name,
                category, price, cost, profit_margin, stock_quantity,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                supplier_id = VALUES(supplier_id),
                supplier_name = VALUES(supplier_name),
                category = VALUES(category),
                price = VALUES(price),
                cost = VALUES(cost),
                profit_margin = VALUES(profit_margin),
                stock_quantity = VALUES(stock_quantity),
                updated_at = VALUES(updated_at),
                etl_timestamp = CURRENT_TIMESTAMP
        """
        
        # Prepare data for insertion
        insert_data = []
        for product in transformed_products:
            row = (
                product['product_id'],
                product['product_name'],
                product['supplier_id'],
                product.get('supplier_name', ''),
                product['category'],
                product['price'],
                product['cost'],
                product['profit_margin'],
                product['stock_quantity'],
                product['created_at'],
                product['updated_at']
            )
            insert_data.append(row)
        
        # Execute batch insert
        cursor.executemany(upsert_query, insert_data)
        connection.commit()
        
        affected_rows = cursor.rowcount
        
        # Close connections
        cursor.close()
        connection.close()
        
        logger.info(f"Successfully loaded {affected_rows} product records to MySQL")
        
    except Exception as e:
        logger.error(f"Error transforming/loading products: {str(e)}")
        raise AirflowException(f"Failed to transform/load products: {str(e)}")


def transform_and_load_orders(**context) -> None:
    """
    Transform order data and load to MySQL
    
    Transformations:
    - Order status: Convert to lowercase
    - Total amount: Validate positive (if negative, set to 0 and log warning)
    
    Loading:
    - Use MySqlHook
    - UPSERT to fact_orders
    """
    try:
        logger.info("Starting transformation and loading of orders")
        
        # Pull data from XCom
        orders = context['task_instance'].xcom_pull(
            task_ids='extract_orders',
            key='orders_data'
        )
        
        if not orders:
            logger.warning("No order data found in XCom")
            return
        
        logger.info(f"Processing {len(orders)} orders")
        
        # Transform data
        transformed_orders = []
        for order in orders:
            transformed_order = order.copy()
            
            # Transform status: Convert to lowercase
            status = str(order.get('status', ''))
            transformed_order['status'] = status.lower()
            
            # Validate total amount: Ensure positive
            total_amount = float(order.get('total_amount', 0))
            if total_amount < 0:
                logger.warning(
                    f"Negative total amount ({total_amount}) found for order_id: {order.get('order_id')}. "
                    f"Setting to 0."
                )
                total_amount = 0.0
            
            transformed_order['total_amount'] = total_amount
            
            transformed_orders.append(transformed_order)
        
        # Initialize MySQL connection
        mysql_hook = MySqlHook(mysql_conn_id='mysql_data_warehouse')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        
        # UPSERT to fact_orders
        upsert_query = """
            INSERT INTO fact_orders (
                order_id, customer_id, product_id, quantity,
                unit_price, total_amount, order_date, status,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                customer_id = VALUES(customer_id),
                product_id = VALUES(product_id),
                quantity = VALUES(quantity),
                unit_price = VALUES(unit_price),
                total_amount = VALUES(total_amount),
                order_date = VALUES(order_date),
                status = VALUES(status),
                updated_at = VALUES(updated_at),
                etl_timestamp = CURRENT_TIMESTAMP
        """
        
        # Prepare data for insertion
        insert_data = []
        for order in transformed_orders:
            row = (
                order['order_id'],
                order['customer_id'],
                order['product_id'],
                order['quantity'],
                order['unit_price'],
                order['total_amount'],
                order['order_date'],
                order['status'],
                order['created_at'],
                order['updated_at']
            )
            insert_data.append(row)
        
        # Execute batch insert
        cursor.executemany(upsert_query, insert_data)
        connection.commit()
        
        affected_rows = cursor.rowcount
        
        # Close connections
        cursor.close()
        connection.close()
        
        logger.info(f"Successfully loaded {affected_rows} order records to MySQL")
        
    except Exception as e:
        logger.error(f"Error transforming/loading orders: {str(e)}")
        raise AirflowException(f"Failed to transform/load orders: {str(e)}")


# ==================== TASK DEFINITIONS ====================

# Extraction tasks
extract_customers = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customers_from_postgres,
    provide_context=True,
    dag=dag,
)

extract_products = PythonOperator(
    task_id='extract_products',
    python_callable=extract_products_from_postgres,
    provide_context=True,
    dag=dag,
)

extract_orders = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders_from_postgres,
    provide_context=True,
    dag=dag,
)

# Transformation and loading tasks
transform_load_customers = PythonOperator(
    task_id='transform_load_customers',
    python_callable=transform_and_load_customers,
    provide_context=True,
    dag=dag,
)

transform_load_products = PythonOperator(
    task_id='transform_load_products',
    python_callable=transform_and_load_products,
    provide_context=True,
    dag=dag,
)

transform_load_orders = PythonOperator(
    task_id='transform_load_orders',
    python_callable=transform_and_load_orders,
    provide_context=True,
    dag=dag,
)

# ==================== TASK DEPENDENCIES ====================

# Set dependencies
# Extraction tasks can run in parallel
# Each extraction must complete before its corresponding transformation
extract_customers >> transform_load_customers
extract_products >> transform_load_products
extract_orders >> transform_load_orders