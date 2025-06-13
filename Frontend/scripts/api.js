export const dbSelector = document.getElementById('dbSelector');

let _currentDb = null;

export function getCurrentDb() { // eltarolja a kivalasztott adatbazist
    return _currentDb;
}

export function setCurrentDb(value) {
    _currentDb = value;
}

export function fetchDatabases() { // lekeri az adatbazisokat a aszervertol
    fetch('http://127.0.0.1:5000/databases')
        .then(res => res.json())
        .then(data => {
            while (dbSelector.firstChild) {
                dbSelector.removeChild(dbSelector.firstChild);
            }

            const defaultOption = document.createElement('option');
            defaultOption.disabled = true;
            defaultOption.selected = true;
            defaultOption.value = "";
            defaultOption.textContent = "-- Select database --";
            dbSelector.appendChild(defaultOption);

            data.sort((a, b) => a.localeCompare(b)); 
            data.forEach(db => {
                const option = document.createElement('option');
                option.value = db;
                option.textContent = db;
                dbSelector.appendChild(option);
            });
        })
        .catch(err => {
            console.error("Error loading databases:", err);
        });
}

export function refreshDbList() {
    fetchDatabases();
}

export function fetchTables(callback) { // adot adatbazishoz tartozo tablak metaadatai
    const db = getCurrentDb();
    if (!db) return;

    fetch(`http://127.0.0.1:5000/tables?db=${db}`)
        .then(res => res.json())
        .then(data => {
            callback(data);
        })
        .catch(err => {
            const tablesContainer = document.getElementById('tablesContainer');
            tablesContainer.textContent = "Error occurred: " + err.message;
        });
}
