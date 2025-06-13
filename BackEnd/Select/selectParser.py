import re

def parse_select_statement(stmt):
    stmt = stmt.strip().rstrip(';')

    # Először ellenőrizzük, hogy van-e JOIN a statement-ben
    if re.search(r'\bjoin\b', stmt, re.IGNORECASE):
        return parse_join_statement(stmt)
    else:
        return parse_simple_select(stmt)

def parse_simple_select(stmt):
    """Egyszerű SELECT parsing with GROUP BY support"""
    # Updated pattern to handle GROUP BY
    pattern = r"(?i)select\s+(distinct\s+)?(.*?)\s+from\s+(\w+)(?:\s+where\s+(.*?))?(?:\s+group\s+by\s+(.*?))?(?:\s+order\s+by\s+(.*?))?$"
    match = re.match(pattern, stmt, re.IGNORECASE | re.DOTALL)
    if not match:
        return {"error": "Invalid SELECT syntax"}

    distinct_keyword = match.group(1)
    columns_str = match.group(2)
    table = match.group(3)
    where_clause = match.group(4)
    group_by_clause = match.group(5)
    order_by_clause = match.group(6)

    is_distinct = distinct_keyword is not None
    
    # Parse columns and detect aggregations
    columns, aggregations = parse_columns_with_aggregations(columns_str)
    
    conditions = []
    if where_clause:
        conditions = parse_where_conditions(where_clause.strip())
        if "error" in conditions:
            return conditions

    group_by_columns = []
    if group_by_clause:
        group_by_columns = [col.strip() for col in group_by_clause.split(',')]

    order_by_columns = []
    if order_by_clause:
        order_by_columns = parse_order_by(order_by_clause.strip())

    return {
        "type": "select",
        "columns": columns,
        "table": table,
        "where": conditions,
        "distinct": is_distinct,
        "joins": [],
        "group_by": group_by_columns,
        "aggregations": aggregations,
        "order_by": order_by_columns
    }

def parse_join_statement(stmt):
    """JOIN statement parsing with GROUP BY support"""
    # Normalize whitespace - replace multiple spaces and newlines with single spaces
    stmt = re.sub(r'\s+', ' ', stmt.strip())
    
    distinct_match = re.search(r'(?i)select\s+(distinct\s+)?', stmt)
    is_distinct = distinct_match.group(1) is not None if distinct_match else False
    
    if is_distinct:
        stmt = re.sub(r'(?i)select\s+distinct\s+', 'SELECT ', stmt)
    
    # More flexible pattern to handle complex column lists and table names with GROUP BY
    pattern = r"(?i)select\s+(.*?)\s+from\s+(\w+)\s+(.*)"
    match = re.match(pattern, stmt, re.IGNORECASE | re.DOTALL)
    
    if not match:
        return {"error": f"Invalid JOIN syntax: {stmt}"}
    
    columns_str = match.group(1).strip()
    main_table = match.group(2)
    remaining_part = match.group(3).strip()  # JOIN, WHERE, GROUP BY együtt
    
    # Parse the remaining part to extract JOIN, WHERE, GROUP BY, ORDER BY
    parsed_parts = parse_remaining_clauses(remaining_part)
    if "error" in parsed_parts:
        return parsed_parts
    
    join_part = parsed_parts["join_part"]
    where_clause = parsed_parts["where_clause"]
    group_by_clause = parsed_parts["group_by_clause"]
    order_by_clause = parsed_parts["order_by_clause"]
    
    # Parse columns and detect aggregations
    columns, aggregations = parse_columns_with_aggregations(columns_str)
    
    joins = parse_joins(join_part)
    if isinstance(joins, dict) and "error" in joins:
        return joins
    
    conditions = []
    if where_clause:
        conditions = parse_where_conditions(where_clause)
        if "error" in conditions:
            return conditions

    group_by_columns = []
    if group_by_clause:
        group_by_columns = [col.strip() for col in group_by_clause.split(',')]

    order_by_columns = []
    if order_by_clause:
        order_by_columns = parse_order_by(order_by_clause)
    
    return {
        "type": "join_select",
        "columns": columns,
        "table": main_table,
        "joins": joins,
        "where": conditions,
        "distinct": is_distinct,
        "group_by": group_by_columns,
        "aggregations": aggregations,
        "order_by": order_by_columns
    }

def parse_remaining_clauses(remaining_part):
    """Parse JOIN, WHERE, GROUP BY, ORDER BY from the remaining part"""
    # Find ORDER BY first (it's at the end)
    order_by_match = re.search(r'\s+order\s+by\s+(.+?)$', remaining_part, re.IGNORECASE)
    order_by_clause = None
    if order_by_match:
        order_by_clause = order_by_match.group(1).strip()
        remaining_part = remaining_part[:order_by_match.start()]
    
    # Find GROUP BY
    group_by_match = re.search(r'\s+group\s+by\s+(.+?)(?:\s+order\s+by|\s+where|$)', remaining_part, re.IGNORECASE)
    group_by_clause = None
    if group_by_match:
        group_by_clause = group_by_match.group(1).strip()
        remaining_part = remaining_part[:group_by_match.start()]
    
    # Find WHERE
    where_match = re.search(r'\s+where\s+(.+?)$', remaining_part, re.IGNORECASE)
    where_clause = None
    if where_match:
        where_clause = where_match.group(1).strip()
        remaining_part = remaining_part[:where_match.start()]
    
    # What's left should be JOIN part
    join_part = remaining_part.strip()
    
    return {
        "join_part": join_part,
        "where_clause": where_clause,
        "group_by_clause": group_by_clause,
        "order_by_clause": order_by_clause
    }

def parse_columns_with_aggregations(columns_str):
    """Parse columns and detect aggregation functions"""
    columns_str = re.sub(r'\s+', ' ', columns_str.strip())
    raw_columns = [col.strip() for col in columns_str.split(',')]
    
    columns = []
    aggregations = []
    
    for i, col in enumerate(raw_columns):
        # Check for aggregation functions
        agg_match = re.match(r'^(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*([a-zA-Z_][\w\.]*|\*)\s*\)$', col.strip(), re.IGNORECASE)

        if agg_match:
            func = agg_match.group(1).upper()
            target = agg_match.group(2).strip()
            
            # Handle COUNT(*) special case
            if func == "COUNT" and target == "*":
                target = "*"
            
            aggregations.append({
                "function": func,
                "column": target,
                "alias": f"{func}_{target}".replace("*", "ALL").replace(".", "_"),
                "index": i
            })
            
            # Add the aggregation result column name
           # columns.append(f"{func}({target})")
            columns.append(target)
        else:
            columns.append(col)
    return columns, aggregations

def parse_order_by(order_by_clause):
    """Parse ORDER BY clause"""
    order_columns = []
    
    parts = [part.strip() for part in order_by_clause.split(',')]
    for part in parts:
        # Check for ASC/DESC
        if part.upper().endswith(' DESC'):
            column = part[:-5].strip()
            direction = 'DESC'
        elif part.upper().endswith(' ASC'):
            column = part[:-4].strip()
            direction = 'ASC'
        else:
            column = part
            direction = 'ASC'  # Default
        
        order_columns.append({
            "column": column,
            "direction": direction
        })
    
    return order_columns

def parse_joins(join_part):
    """JOIN részek feldolgozása"""
    joins = []
    
    join_part = re.sub(r'\s+', ' ', join_part.strip())
    
    # More flexible pattern for multiple JOINs with dot notation
    join_patterns = re.findall(r'(?i)((?:inner\s+)?join)\s+(\w+)\s+on\s+([\w.]+)\s*=\s*([\w.]+)', join_part)
    
    if not join_patterns:
        return {"error": f"No valid JOIN conditions found in: {join_part}"}
    
    for join_match in join_patterns:
        join_type = join_match[0].strip().upper()
        if join_type == "JOIN":
            join_type = "INNER JOIN"
        
        table = join_match[1]
        left_column = join_match[2]
        right_column = join_match[3]
        
        joins.append({
            "type": join_type,
            "table": table,
            "left_column": left_column,
            "right_column": right_column
        })
    
    return joins

def parse_where_conditions(where_clause):
    conditions = []
    
    condition_parts = re.split(r"\s+and\s+", where_clause, flags=re.IGNORECASE)
    for cond in condition_parts:
        cond = cond.strip()
        
        # Először keressük meg az operátort
        op_match = re.search(r'\s*(=|>=|<=|<|>)\s*', cond)
        if not op_match:
            return {"error": f"No operator found in condition: '{cond}'"}
        
        op = op_match.group(1)
        op_start = op_match.start()
        op_end = op_match.end()
        
        # Oszlop név az operátor előtt
        column = cond[:op_start].strip()
        
        # Érték az operátor után
        value = cond[op_end:].strip()
        
        # Remove quotes from value if present
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]
        
        conditions.append({
            "column": column,
            "op": op,
            "value": value
        })
    
    return conditions
