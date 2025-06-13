import re
import os
import json
from BackEnd.Create.table import parse_create_table
from BackEnd.Create.database import create_database, get_metadata_file
from BackEnd.Insert_Get_From_Mongo.insert import parse_insert
from BackEnd.Insert_Get_From_Mongo.delete import parse_delete
from BackEnd.Create.drop import *
from BackEnd.Create.index import parse_create_index
from BackEnd.Select.select import parse_select

# Function to remove SQL comments from statements
def remove_sql_comments(sql_statement):
    """Remove all "-- comment" type comments to the end of line"""
    if not sql_statement:
        return ""
        
    lines = []
    for line in sql_statement.split('\n'):
        comment_pos = line.find('--')
        if comment_pos >= 0:
            line = line[:comment_pos]
        lines.append(line)
    return '\n'.join(lines)

def process_statement(stmt, current_database):
    # Remove comments before processing
    clean_stmt = remove_sql_comments(stmt.strip())
    
    # If after removing comments the statement is empty, return
    if not clean_stmt:
        return {"message": "Empty statement after removing comments"}
    
    def create_db():
        match = re.search(r'CREATE DATABASE (\w+)', clean_stmt, re.IGNORECASE)
        if match:
            dbname = match.group(1)
            return create_database(dbname)
        return {"error": f"Invalid CREATE DATABASE statement: {clean_stmt}"}

    def use_db():
        match = re.search(r'USE (\w+)', clean_stmt, re.IGNORECASE)
        if match:
            dbname = match.group(1)
            db_file = get_metadata_file(dbname)

            if not os.path.exists(db_file):
                return {"error": f"Database '{dbname}' does not exist"}

            return {"message": f"Database '{dbname}' in use", "database": dbname}
        return {"error": f"Invalid USE statement: {clean_stmt}"}

    def create_table():
        if current_database is None:
            return {"error": "No database selected"}

        table_data = parse_create_table(clean_stmt, current_database)

        if "error" in table_data:
            return table_data

        db_file = get_metadata_file(current_database)
        try:
            with open(db_file, 'r') as f:
                db_content = json.load(f)

            table_name = table_data["table_name"]
            db_content["tables"][table_name] = table_data

            with open(db_file, 'w') as f:
                json.dump(db_content, f, indent=4)

            return {"message": f"Table '{table_name}' created in '{current_database}'"}
        except FileNotFoundError:
            return {"error": f"Database metadata file not found for '{current_database}'"}
        except json.JSONDecodeError:
            return {"error": f"Invalid database metadata file for '{current_database}'"}

    def drop_table():
        if current_database == None:
            return {"error": f"No Database in USE"}
        
        table_data = parse_drop_table(clean_stmt, current_database)
        return table_data
    
    def drop_db():
        database_data = parse_drop_database(clean_stmt, current_database)
        return database_data
    
    def insert_data():
        return parse_insert(clean_stmt, current_database)

    def delete_data():
        return parse_delete(clean_stmt, current_database)
    
    def create_index():
        return parse_create_index(clean_stmt, current_database)
    
    # Determine the command type and call the appropriate function
    stmt_upper = clean_stmt.upper()
    if stmt_upper.startswith("CREATE DATABASE"):
        return create_db()
    elif stmt_upper.startswith("USE"):
        return use_db()
    elif stmt_upper.startswith("CREATE TABLE"):
        return create_table()
    elif stmt_upper.startswith("CREATE INDEX"):
        return create_index()
    elif stmt_upper.startswith("DROP TABLE"):
        return drop_table()
    elif stmt_upper.startswith("DROP DATABASE"):
        return drop_db()
    elif stmt_upper.startswith("INSERT INTO"):
        return insert_data()
    elif stmt_upper.startswith("DELETE FROM"):
        return delete_data()
    elif stmt_upper.startswith("SELECT"):
        return parse_select(clean_stmt, current_database)
    else:
        return {"error": f"Unsupported statement: {clean_stmt}"}


def handle_sql_commands(sql):
    """Split SQL commands by semicolon and process each statement"""
    # First, replace all newlines with spaces to handle multi-line statements better
    statements = []
    current_statement = ""
    i = 0
    in_quotes = False
    quote_char = None
    
    # Split statements handling comments and quoted strings properly
    while i < len(sql):
        char = sql[i]
        
        # Handle quotes (both single and double)
        if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
                quote_char = None
        
        # Handle comments (only when not in quotes)
        elif not in_quotes and i + 1 < len(sql) and sql[i:i+2] == "--":
            # Skip to end of line
            eol = sql.find('\n', i)
            if eol == -1:
                # If no newline, go to end of string
                i = len(sql)
                continue
            else:
                i = eol
        
        # Handle statement separator (only when not in quotes)
        elif char == ';' and not in_quotes:
            current_statement += char
            if current_statement.strip():
                statements.append(current_statement.strip())
            current_statement = ""
        else:
            current_statement += char
        
        i += 1
    
    # Add the last statement if it doesn't end with ';'
    if current_statement.strip():
        statements.append(current_statement.strip())
    
    current_database = None
    responses = []

    for stmt in statements:
        response = process_statement(stmt, current_database)
        responses.append(response)

        if response.get('database'):
            current_database = response['database']

    return responses