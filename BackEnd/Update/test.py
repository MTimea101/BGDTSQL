import random
import datetime
import time

# Parameters for data generation
NUM_CATEGORIES = 5       # Reduced from 10
NUM_PRODUCTS = 100       # Reduced from 1000
NUM_CUSTOMERS = 50       # Reduced from 5000
NUM_ADDRESSES = 70       # Reduced from 7000
NUM_ORDERS = 200         # Reduced from 20000
NUM_ORDER_ITEMS = 100000 # Kept large - this is our main test table

# Helper function to execute SQL commands using your mini-DBMS
def execute_sql(sql):
    from BackEnd.Main.controller import handle_sql_commands
    
    # Add USE statement if it's not already there
    if not sql.strip().upper().startswith("USE"):
        sql = "USE Ecommerce;\n" + sql
        
    responses = handle_sql_commands(sql)
    return responses

# In-memory data structures to replace MongoDB lookups
customer_addresses = {}  # Maps customer_id -> list of address_ids
product_prices = {}      # Maps product_id -> price

def generate_database_schema():
    """Generate the database schema SQL"""
    schema_sql = """
    -- Drop and recreate the database
    DROP DATABASE Ecommerce;
    CREATE DATABASE Ecommerce;
    USE Ecommerce;

    -- Create tables with simplified structure for testing
    CREATE TABLE categories (
      CategoryID INT PRIMARY KEY,
      CategoryName VARCHAR(50),
      Description VARCHAR(200)
    );

    CREATE TABLE products (
      ProductID INT PRIMARY KEY,
      ProductName VARCHAR(100),
      CategoryID INT REFERENCES categories(CategoryID),
      UnitPrice FLOAT,
      InStock INT,
      Description VARCHAR(200)
    );

    CREATE TABLE customers (
      CustomerID INT PRIMARY KEY,
      FirstName VARCHAR(50),
      LastName VARCHAR(50),
      Email VARCHAR(100),
      Phone VARCHAR(20),
      RegistrationDate DATE
    );

    CREATE TABLE shipping_addresses (
      AddressID INT PRIMARY KEY,
      CustomerID INT REFERENCES customers(CustomerID),
      AddressLine VARCHAR(100),
      City VARCHAR(50),
      Country VARCHAR(50),
      PostalCode VARCHAR(20),
      IsDefault BOOL
    );

    CREATE TABLE orders (
      OrderID INT PRIMARY KEY,
      CustomerID INT REFERENCES customers(CustomerID),
      OrderDate DATE,
      AddressID INT REFERENCES shipping_addresses(AddressID),
      TotalAmount FLOAT,
      Status VARCHAR(20)
    );

    CREATE TABLE order_items (
      OrderItemID INT PRIMARY KEY,
      OrderID INT REFERENCES orders(OrderID),
      ProductID INT REFERENCES products(ProductID),
      Quantity INT,
      UnitPrice FLOAT,
      Discount FLOAT
    );

    -- Create just the essential indexes
    CREATE INDEX idx_products_category ON products (CategoryID);
    CREATE INDEX idx_order_items_order ON order_items (OrderID);
    """
    
    return schema_sql

def generate_categories_data():
    """Generate SQL for categories data"""
    category_names = ["Electronics", "Clothing", "Books", "Home & Kitchen", "Sports", "Beauty", "Toys", "Automotive", "Health", "Garden"]
    sql = ["USE Ecommerce;"]  # Start with USE statement
    
    for i in range(1, NUM_CATEGORIES + 1):
        category_name = category_names[i % len(category_names)]
        sql.append(f"INSERT INTO categories VALUES ({i}, '{category_name}', 'Description for {category_name}');")
    
    return "\n".join(sql)

def generate_products_data():
    """Generate SQL for products data"""
    product_adjectives = ["Premium", "Deluxe", "Basic", "Advanced", "Professional", "Standard", "Ultimate", "Superior", "Elite", "Classic"]
    product_nouns = ["Laptop", "Phone", "Camera", "Headphones", "Monitor", "Keyboard", "Mouse", "Tablet", "Watch", "Speaker"]
    
    # Generate product data in batches
    all_sql = []
    batch_size = 100  # Use smaller batches for string construction
    
    for batch_start in range(1, NUM_PRODUCTS + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, NUM_PRODUCTS)
        batch_sql = ["USE Ecommerce;"]  # Start each batch with USE statement
        
        for i in range(batch_start, batch_end + 1):
            product_name = f"{random.choice(product_adjectives)} {random.choice(product_nouns)} {i}"
            category_id = random.randint(1, NUM_CATEGORIES)
            unit_price = round(random.uniform(10.0, 1000.0), 2)
            
            # Store the price in our in-memory cache for later use
            product_prices[str(i)] = unit_price
            
            in_stock = random.randint(0, 500)
            
            batch_sql.append(f"INSERT INTO products VALUES ({i}, '{product_name}', {category_id}, {unit_price}, {in_stock}, 'Product description {i}');")
        
        all_sql.append("\n".join(batch_sql))
    
    return "\n".join(all_sql)

def generate_customers_data():
    """Generate SQL for customers data"""
    first_names = ["John", "Jane", "Michael", "Emma", "James", "Olivia", "Robert", "Sophia", "William", "Ava", "David", "Isabella", "Joseph", "Mia", "Thomas", "Charlotte", "Daniel", "Amelia", "Matthew", "Harper"]
    last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson"]
    
    # Generate customer data in batches
    all_sql = []
    batch_size = 100
    
    for batch_start in range(1, NUM_CUSTOMERS + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, NUM_CUSTOMERS)
        batch_sql = ["USE Ecommerce;"]  # Start each batch with USE statement
        
        for i in range(batch_start, batch_end + 1):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
            phone = f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            reg_date = f"{random.randint(2018, 2024)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            
            batch_sql.append(f"INSERT INTO customers VALUES ({i}, '{first_name}', '{last_name}', '{email}', '{phone}', '{reg_date}');")
        
        all_sql.append("\n".join(batch_sql))
    
    return "\n".join(all_sql)

def generate_addresses_data():
    """Generate SQL for shipping addresses data"""
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus", "San Francisco", "Charlotte", "Indianapolis", "Seattle", "Denver", "Washington"]
    countries = ["USA", "Canada", "UK", "Germany", "France", "Australia", "Japan", "Italy", "Spain", "Mexico"]
    streets = ["Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Washington", "Lake", "Hill"]
    street_types = ["St", "Ave", "Rd", "Blvd", "Dr", "Lane", "Way", "Court", "Plaza"]
    
    # Generate address data in batches
    all_sql = []
    batch_size = 100
    
    for batch_start in range(1, NUM_ADDRESSES + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, NUM_ADDRESSES)
        batch_sql = ["USE Ecommerce;"]  # Start each batch with USE statement
        
        for i in range(batch_start, batch_end + 1):
            customer_id = random.randint(1, NUM_CUSTOMERS)
            address_line = f"{random.randint(100, 9999)} {random.choice(streets)} {random.choice(street_types)}"
            city = random.choice(cities)
            country = random.choice(countries)
            postal_code = f"{random.randint(10000, 99999)}"
            is_default = 1 if random.random() < 0.7 else 0  # 70% chance of being default
            
            # Store this association in our in-memory map
            if str(customer_id) not in customer_addresses:
                customer_addresses[str(customer_id)] = []
            customer_addresses[str(customer_id)].append(i)
            
            batch_sql.append(f"INSERT INTO shipping_addresses VALUES ({i}, {customer_id}, '{address_line}', '{city}', '{country}', '{postal_code}', {is_default});")
        
        all_sql.append("\n".join(batch_sql))
    
    return "\n".join(all_sql)

def generate_orders_data():
    """Generate SQL for orders data"""
    order_statuses = ["Pending", "Processing", "Shipped", "Delivered", "Canceled"]
    
    # Generate orders data in batches
    all_sql = []
    batch_size = 100
    
    for batch_start in range(1, NUM_ORDERS + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, NUM_ORDERS)
        batch_sql = ["USE Ecommerce;"]  # Start each batch with USE statement
        
        for i in range(batch_start, batch_end + 1):
            customer_id = random.randint(1, NUM_CUSTOMERS)
            
            # Find addresses for this customer using our in-memory map
            customer_id_str = str(customer_id)
            if customer_id_str in customer_addresses and customer_addresses[customer_id_str]:
                address_id = random.choice(customer_addresses[customer_id_str])
            else:
                # Fallback to a random address if none found for customer
                address_id = random.randint(1, NUM_ADDRESSES)
            
            order_date = f"{random.randint(2020, 2024)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            total_amount = round(random.uniform(20.0, 2000.0), 2)
            status = random.choice(order_statuses)
            
            batch_sql.append(f"INSERT INTO orders VALUES ({i}, {customer_id}, '{order_date}', {address_id}, {total_amount}, '{status}');")
        
        all_sql.append("\n".join(batch_sql))
    
    return "\n".join(all_sql)

def generate_order_items_data():
    """Generate SQL for order items data (our large table with 100,000 rows)"""
    # Generate order items data in batches
    all_sql = []
    batch_size = 5000  # Larger batches for faster processing
    
    for batch_start in range(1, NUM_ORDER_ITEMS + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, NUM_ORDER_ITEMS)
        batch_sql = ["USE Ecommerce;"]  # Start each batch with USE statement
        
        print(f"Generating order items SQL {batch_start} to {batch_end}")
        
        for i in range(batch_start, batch_end + 1):
            # Limit the range of OrderIDs and ProductIDs to match our smaller dataset
            order_id = random.randint(1, NUM_ORDERS)
            product_id = random.randint(1, NUM_PRODUCTS)
            quantity = random.randint(1, 10)
            
            # Simplify price calculation for faster execution
            unit_price = round(random.uniform(10.0, 500.0), 2)
            discount = round(random.uniform(0.0, 0.3), 2)  # 0% to 30% discount
            
            batch_sql.append(f"INSERT INTO order_items VALUES ({i}, {order_id}, {product_id}, {quantity}, {unit_price}, {discount});")
        
        all_sql.append("\n".join(batch_sql))
    
    return "\n".join(all_sql)

def main():

    print("Generating order items data...")
    order_items_sql = generate_order_items_data()
    
    # Execute order items data in chunks - this is the big table
    print("Inserting order items data (100,000 rows)...")
    start_time = time.time()
    
    # Split the large order_items SQL into chunks
    order_items_sql_statements = order_items_sql.split('\n')
    
    # Group the statements, ensuring USE statements are kept together with their batches
    chunks = []
    current_chunk = []
    use_statement = None
    
    for stmt in order_items_sql_statements:
        if stmt.strip().upper().startswith("USE "):
            # If we encounter a new USE statement and already have one, start a new chunk
            if use_statement and current_chunk:
                chunks.append("\n".join(current_chunk))
                current_chunk = []
            use_statement = stmt
            current_chunk.append(stmt)
        elif stmt.strip():
            if use_statement:  # Only add statements if we have a USE statement
                current_chunk.append(stmt)
                # If chunk is getting large, finalize it
                if len(current_chunk) >= 10000:  # Increased chunk size for faster execution
                    chunks.append("\n".join(current_chunk))
                    current_chunk = [use_statement]  # Start a new chunk with the same USE statement
    
    # Add any remaining statements
    if current_chunk:
        chunks.append("\n".join(current_chunk))
    
    start_index = 13
    # Process chunks from the 13th index (14th chunk)
    for i, chunk in enumerate(chunks[start_index:], start=start_index):
        print(f"Processing order items chunk {i+1}/{len(chunks)}...")
        chunk_time = time.time()
        execute_sql(chunk)
        print(f"Chunk processed in {time.time() - chunk_time:.2f} seconds")

    print(f"All order items data inserted in {time.time() - start_time:.2f} seconds")
    print("Data generation complete!")

if __name__ == "__main__":
    main()