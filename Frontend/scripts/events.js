import { dbSelector, setCurrentDb, getCurrentDb } from './api.js';

export function bindDbSelector() { // egy adatbazis kivalasztasa eseten (USE)
    const loadBtn = document.getElementById('loadTables');
    dbSelector.addEventListener('change', () => {
        const selected = dbSelector.value;
        setCurrentDb(selected);
        loadBtn.disabled = !selected;
        loadBtn.classList.toggle('opacity-50', !selected);
        loadBtn.classList.toggle('cursor-not-allowed', !selected);

        const tablesContainer = document.getElementById('tablesContainer');
        if (!selected) {
            tablesContainer.innerHTML = "";
        }
    });
}

export function bindLoadButton(fetchTables, renderTables) { // load button -> meghivja a fetchtables-t majd a renderTables-t (dom)
    const loadBtn = document.getElementById('loadTables');
    loadBtn.addEventListener('click', () => {
        fetchTables(renderTables);
    });
}

export function bindFormSubmit(renderResponse, refreshDbList) { // execute gomb eseten Post keres a backend-nek -> valaszt a renderServerResponse adja majd
    const form = document.getElementById('sqlForm');
    const sqlInput = document.getElementById('sqlInput');
    const responseDiv = document.getElementById('response');
    const loadingIndicator = document.getElementById('loadingIndicator');

    form.addEventListener('submit', function(event) {
        event.preventDefault();
        const sqlQuery = sqlInput.value.trim();
        if (!sqlQuery) {
            responseDiv.textContent = "Please enter an SQL command!";
            responseDiv.classList.remove('text-gray-700');
            responseDiv.classList.add('text-red-600', 'font-semibold');
            return;
        }

        loadingIndicator.classList.remove('hidden');
        responseDiv.innerHTML = '';

        fetch('http://127.0.0.1:5000/COMMAND', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ sql: sqlQuery }),
            mode: 'cors'
        })
        .then(response => {
            if (!response.ok) throw new Error('Server error: ' + response.status);
            return response.json();
        })
        .then(data => {
            if (!Array.isArray(data)) data = [data];
            renderResponse(data);
            const createdDb = data.find(entry => entry.message?.toLowerCase().includes("database"));
            if (createdDb) {
                refreshDbList();
            }
        })
        .catch(error => {
            responseDiv.textContent = "Error occurred: " + error.message;
        })
        .finally(() => {
            loadingIndicator.classList.add('hidden');
        });
    });
}


export function bindToggleButton() {
    const toggleBtn = document.getElementById('toggleTables');
    const tablesContainer = document.getElementById('tablesContainer');

    toggleBtn.addEventListener('click', () => {
        const isHidden = tablesContainer.classList.toggle('hidden');
        const icon = toggleBtn.querySelector('i');
        const label = toggleBtn.querySelector('span');

        if (isHidden) {
            icon.className = 'ri-eye-line';
            label.textContent = 'Show Tables';
        } else {
            icon.className = 'ri-eye-off-line';
            label.textContent = 'Hide Tables';
        }
    });
}
