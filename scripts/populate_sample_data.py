import psycopg2
import mysql.connector
import random
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection config
POSTGRES_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

def populate_postgres():
    """Populate PostgreSQL with sample data"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        logger.info("Populating PostgreSQL raw_data schema...")
        
        # Clear existing data
        cursor.execute("DELETE FROM raw_data.orders")
        cursor.execute("DELETE FROM raw_data.products")
        cursor.execute("DELETE FROM raw_data.suppliers")
        cursor.execute("DELETE FROM raw_data.customers")
        
        # Insert customers
        customers = [
            ('John', 'Doe', 'john@example.com', '555-123-4567', '123 Main St', 'New York', 'NY', '10001'),
            ('Jane', 'Smith', 'jane@example.com', '555-987-6543', '456 Oak Ave', 'Los Angeles', 'CA', '90001'),
            ('Bob', 'Johnson', 'bob@example.com', '555-456-7890', '789 Pine Rd', 'Chicago', 'IL', '60601'),
            ('Alice', 'Williams', 'alice@example.com', '555-321-0987', '321 Elm St', 'Houston', 'TX', '77001'),
            ('Charlie', 'Brown', 'charlie@example.com', '555-654-3210', '654 Maple Dr', 'Phoenix', 'az', '85001')
        ]
        
        for customer in customers:
            cursor.execute("""
                INSERT INTO raw_data.customers 
                (first_name, last_name, email, phone, address, city, state, zip_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, customer)
        
        # Insert suppliers
        suppliers = [
            ('TechGadgets Inc', 'Mike Chen', '555-111-2222', '100 Tech Park'),
            ('OfficeSupplies Co', 'Sarah Miller', '555-333-4444', '200 Business Blvd'),
            ('HomeGoods Ltd', 'David Wilson', '555-555-6666', '300 Home Ave')
        ]
        
        for supplier in suppliers:
            cursor.execute("""
                INSERT INTO raw_data.suppliers 
                (supplier_name, contact_name, contact_phone, address)
                VALUES (%s, %s, %s, %s)
            """, supplier)
        
        # Insert products
        products = [
            ('Laptop Pro', 1, 'Electronics', 1200.00, 800.00, 50),
            ('Wireless Mouse', 1, 'Electronics', 25.99, 10.00, 200),
            ('Office Chair', 2, 'Furniture', 299.99, 150.00, 30),
            ('Notebook Set', 2, 'Stationery', 19.99, 8.00, 500),
            ('Coffee Maker', 3, 'Home Appliances', 89.99, 45.00, 100),
            ('Desk Lamp', 3, 'Home Appliances', 34.99, 15.00, 150)
        ]
        
        for product in products:
            cursor.execute("""
                INSERT INTO raw_data.products 
                (product_name, supplier_id, category, price, cost, stock_quantity)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, product)
        
        # Insert orders (last 7 days)
        statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED']
        
        for i in range(1, 21):
            customer_id = random.randint(1, 5)
            product_id = random.randint(1, 6)
            quantity = random.randint(1, 5)
            
            cursor.execute("SELECT price FROM raw_data.products WHERE product_id = %s", (product_id,))
            price = cursor.fetchone()[0]
            total_amount = price * quantity
            
            order_date = datetime.now() - timedelta(days=random.randint(0, 7))
            status = random.choice(statuses)
            
            cursor.execute("""
                INSERT INTO raw_data.orders 
                (customer_id, product_id, quantity, unit_price, total_amount, order_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (customer_id, product_id, quantity, price, total_amount, order_date, status))
        
        conn.commit()
        logger.info(f"PostgreSQL populated successfully")
        
    except Exception as e:
        logger.error(f"Error populating PostgreSQL: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    populate_postgres()
    logger.info("Sample data population completed!")