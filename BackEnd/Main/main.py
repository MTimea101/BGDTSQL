from flask import Flask, request, jsonify
from .controller import *
from BackEnd.Create.database import *
from flask_cors import CORS
import os
import json

app = Flask(__name__)
CORS(app)

@app.route('/COMMAND', methods=['POST'])
def parse_sql():
    data = request.json
   
    if 'sql' not in data:
        return jsonify({"error": "Missing SQL statement"}), 400
    
    sql = data['sql']
    response = handle_sql_commands(sql)
    return jsonify(response), 200

@app.route('/tables', methods=['GET'])
def list_tables():
    dbname = request.args.get('db')
    if not dbname:
        return jsonify({"error": "No database selected"}), 400

    db_file = get_metadata_file(dbname)

    if not os.path.exists(db_file):
        return jsonify({"error": f"Database '{dbname}' not found"}), 404

    with open(db_file, 'r') as f:
        db_content = json.load(f)

    return jsonify(db_content.get("tables", {})), 200

@app.route('/databases', methods=['GET'])
def list_databases():
    db_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../MetaData")) 
    
    if not os.path.exists(db_folder):
        return jsonify([])

    db_files = [f for f in os.listdir(db_folder) if f.endswith('.json')]
    db_names = [os.path.splitext(f)[0] for f in db_files]

    return jsonify(db_names), 200

if __name__ == '__main__':
    app.run(debug=True)