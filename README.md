# BGDTSQL

**BGDTSQL** is a custom-built mini database management system (DBMS) developed in Python. It supports a subset of SQL-like commands such as `CREATE`, `INSERT`, `SELECT`, `DELETE`, and `JOIN`, with data stored in JSON format. Designed for educational purposes and experimentation, it also features optional MongoDB integration.

---

## Features

- 🛠️ Create and drop databases and tables
- 📥 Insert, delete, and select data using SQL-like syntax
- 📊 Support for single-table and multi-table JOIN queries
- 🔍 Indexing support for faster lookups
- 🧾 JSON-based persistent storage
- 🌐 Optional MongoDB integration
- ✅ Command parsing and basic syntax validation

---

## Tech Stack

- **Language:** Python
- **Storage:** JSON files, optional MongoDB
- **Command Interface:** Custom SQL-like parser
- **Data Structure:** JSON catalog for metadata, per-table JSON files
- **Frontend:** HTML interface (`main.html`) for user interaction

---

## Getting Started

1. **Clone the repository:**

   ```bash
   git clone https://github.com/MTimea101/BGDTSQL.git
   cd BGDTSQL
   ```

2. **Install dependencies (if any):**

   This project mostly uses Python standard libraries, but for MongoDB support, install:

   ```bash
   pip install pymongo
   ```

3. **Run the backend system:**

   ```bash
   python -m BackEnd.Main.main
   ```

4. **View the frontend (optional):**

   Open `main.html` using a Live Server extension (e.g., in VS Code) to interact with the system visually.

5. **Use SQL-like commands:**

   Example:

   ```
   CREATE DATABASE test;
   USE test;
   CREATE TABLE users (id INT, name TEXT);
   INSERT INTO users VALUES (1, 'Alice');
   SELECT * FROM users;
   ```

---

## Project Structure

```
BGDTSQL/
│
├── BackEnd/
│ └── Main/ # Entry point and core logic
│
├── engine/ # DBMS logic (execution, indexing)
├── storage/ # File-based data I/O
├── mongo/ # MongoDB integration
├── tests/ # Unit and integration tests
│
├── MetaData/ # Sample metadata files describing databases and schemas
│ ├── TestDB.json
│ ├── Ecommerce.json
│ └── ...
│
├── FrontEnd/ # Frontend UI and assets
│ ├── main.html # HTML interface
│ ├── style.css # Optional CSS styling
│ └── script.js # Optional JS logic for UI interaction
│
├── README.md
└── .gitignore
```

---

## Limitations

- Not a replacement for full SQL engines
- No support for transactions or concurrency
- Limited error handling (designed for learning, not production)

---

## Contributing

Contributions are welcome! Feel free to fork the project, report issues, or submit pull requests to improve the system.

---

## License

This project is licensed under the MIT License.

---

## Author

**Timea Majercsik**  
GitHub: [@MTimea101](https://github.com/MTimea101)
