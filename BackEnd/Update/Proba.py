import random
import time

# Parameters for data generation
NUM_NEW_CUSTOMERS = 10000  # New customers to add

# Helper function to execute SQL commands using your mini-DBMS
def execute_sql(sql):
    from BackEnd.Main.controller import handle_sql_commands
    
    # Add USE statement if it's not already there
    if not sql.strip().upper().startswith("USE"):
        sql = "USE Ecommerce;\n" + sql
        
    responses = handle_sql_commands(sql)
    return responses

def generate_customers_data(start_id=1000):
    """Generate SQL for 10,000 new customers starting from a specific ID"""
    first_names = ["John", "Jane", "Michael", "Emma", "James", "Olivia", "Robert", "Sophia", "William", "Ava", 
                  "David", "Isabella", "Joseph", "Mia", "Thomas", "Charlotte", "Daniel", "Amelia", "Matthew", 
                  "Harper", "Andrew", "Evelyn", "Joshua", "Abigail", "Christopher", "Emily", "Jack", "Elizabeth", 
                  "Ryan", "Sofia", "Nicholas", "Avery", "Tyler", "Ella", "Alexander", "Madison", "Anthony", "Scarlett", 
                  "Ethan", "Victoria"]
    
    last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", 
                 "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", 
                 "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "King", 
                 "Wright", "Scott", "Green", "Baker", "Adams", "Nelson", "Hill", "Ramirez", "Campbell", "Mitchell", 
                 "Roberts"]
    
    # Generate customer data in batches
    all_sql = []
    batch_size = 1000  # Process in batches of 1000
    
    for batch_start in range(start_id, start_id + NUM_NEW_CUSTOMERS, batch_size):
        batch_end = min(batch_start + batch_size - 1, start_id + NUM_NEW_CUSTOMERS - 1)
        batch_sql = ["USE Ecommerce;"]  # Start each batch with USE statement
        
        for i in range(batch_start, batch_end + 1):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
            phone = f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            reg_date = f"{random.randint(2018, 2024)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            
            batch_sql.append(f"INSERT INTO customers VALUES ({i}, '{first_name}', '{last_name}', '{email}', '{phone}', '{reg_date}');")
        
        all_sql.append("\n".join(batch_sql))
    
    return all_sql  # Return as a list of batch SQL strings

def main():
    # Determine the starting ID for new customers
    # You might want to adjust this based on your existing data
    start_id = 5001  # Assuming existing customers have IDs up to 5000
    
    print(f"Generating data for {NUM_NEW_CUSTOMERS} new customers...")
    customers_sql_batches = generate_customers_data(start_id)
    
    # Execute customer data in batches
    print(f"Inserting {NUM_NEW_CUSTOMERS} customers in batches...")
    total_start_time = time.time()
    
    for i, batch_sql in enumerate(customers_sql_batches):
        print(f"Processing batch {i+1}/{len(customers_sql_batches)}...")
        batch_start_time = time.time()
        execute_sql(batch_sql)
        print(f"Batch processed in {time.time() - batch_start_time:.2f} seconds")
    
    print(f"All {NUM_NEW_CUSTOMERS} customers inserted in {time.time() - total_start_time:.2f} seconds")
    print("Data generation complete!")

if __name__ == "__main__":
    main()