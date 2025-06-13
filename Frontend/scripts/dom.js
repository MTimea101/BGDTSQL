export function renderTables(data) { // megjeleniti az kivalasztott adatbazis osszes tablajat
    const tablesContainer = document.getElementById('tablesContainer');
    tablesContainer.innerHTML = '';

    if (data.error) {
        const errorMsg = document.createElement('div');
        errorMsg.textContent = "Error: " + data.error;
        errorMsg.classList.add('text-red-600', 'font-semibold', 'mt-2');
        tablesContainer.appendChild(errorMsg);
        return;
    }

    for (const [tableName, tableData] of Object.entries(data)) {
        const pkCols = (tableData.constraints?.primary_key || []);
        const fkCols = (tableData.constraints?.foreign_keys || []).map(fk => fk.column);

        const tableDiv = document.createElement('div');
        tableDiv.classList.add('table-box');

        const title = document.createElement('h3');
        title.textContent = `🧾 ${tableName}`;
        tableDiv.appendChild(title);

        const table = document.createElement('table');
        table.classList.add('data-table');

        const header = document.createElement('tr');
        const th1 = document.createElement('th');
        th1.textContent = "Column Name";
        const th2 = document.createElement('th');
        th2.textContent = "Type";
        header.append(th1, th2);
        table.appendChild(header);

        for (const col of tableData.columns) {
            const isPK = pkCols.includes(col.name);
            const isFK = fkCols.includes(col.name);

            const row = document.createElement('tr');

            const nameCell = document.createElement('td');
            nameCell.classList.add('font-semibold');
            nameCell.textContent = col.name;

            if (isPK) {
                const pkSpan = document.createElement('span');
                pkSpan.classList.add('text-yellow-600');
                pkSpan.textContent = ' 🔑';
                nameCell.appendChild(pkSpan);
            }

            if (isFK) {
                const fkSpan = document.createElement('span');
                fkSpan.classList.add('text-blue-600');
                fkSpan.textContent = ' 🔗';
                nameCell.appendChild(fkSpan);
            }

            const typeCell = document.createElement('td');
            typeCell.textContent = col.type;

            row.append(nameCell, typeCell);
            table.appendChild(row);
        }

        tableDiv.appendChild(table);

        if (tableData.constraints) {
            const constraintsDiv = document.createElement('div');
            constraintsDiv.classList.add('mt-2', 'text-sm', 'text-gray-600', 'space-y-1');

            if (pkCols.length > 0) {
                const pk = document.createElement('div');
                const pkLabel = document.createElement('span');
                pkLabel.classList.add('font-semibold', 'text-yellow-600');
                pkLabel.textContent = "Primary Key:";
                pk.appendChild(pkLabel);
                pk.append(` ${pkCols.join(", ")}`);
                constraintsDiv.appendChild(pk);
            }

            if (tableData.constraints.foreign_keys?.length > 0) {
                const fk = document.createElement('div');
                const fkLabel = document.createElement('span');
                fkLabel.classList.add('font-semibold', 'text-blue-600');
                fkLabel.textContent = "Foreign Keys:";
                const fks = tableData.constraints.foreign_keys
                    .map(fk => `${fk.column} → ${fk.references.table}(${fk.references.column})`)
                    .join(", ");
                fk.appendChild(fkLabel);
                fk.append(` ${fks}`);
                constraintsDiv.appendChild(fk);
            }

            tableDiv.appendChild(constraintsDiv);
        }

        tablesContainer.appendChild(tableDiv);
    }
}

export function renderServerResponse(data) { // feldolgozza a valaszt az sql parancsok utan
    const responseDiv = document.getElementById('response');
    const gridBox = document.getElementById('queryResultBox');
    const gridDiv = document.getElementById('grid-table');

    if (window.activeGridInstance) {
        window.activeGridInstance.destroy();
        window.activeGridInstance = null;
    }

    gridBox.classList.add('hidden');
    gridDiv.replaceChildren();
    responseDiv.innerHTML = '';

    const list = document.createElement('ul');
    list.classList.add('list-disc', 'ml-5', 'space-y-1');

    let selectCounter = 0; // csak SELECT utasításokat számoljuk

    data.forEach(entry => {
        if (entry.headers && entry.rows) { // ha SELECT megjelenit egy Grid.js tablazatot
            selectCounter++;
            gridBox.classList.remove('hidden');

            const tableWrapper = document.createElement('div');
            tableWrapper.classList.add('mb-6', 'p-4', 'bg-blue-50', 'border', 'border-blue-200', 'rounded-lg', 'shadow-sm');

            const title = document.createElement('h3');
            title.textContent = `Query #${selectCounter}`;
            title.classList.add('text-blue-600', 'font-semibold', 'mb-2');
            tableWrapper.appendChild(title);

            const individualTableDiv = document.createElement('div');
            tableWrapper.appendChild(individualTableDiv);

            const gridInstance = new gridjs.Grid({
                columns: entry.headers,
                data: entry.rows,
                pagination: { limit: 10 },
                height: 'auto',
                sort: true,
                search: true,
                resizable: true,
                className: {
                    table: 'border rounded text-sm'
                },
                style: {
                    th: { 'background-color': '#f9fafb', 'text-align': 'left' },
                    td: { 'text-align': 'left' }
                }
            });

            gridInstance.render(individualTableDiv);
            gridDiv.appendChild(tableWrapper);

        } else {
            const isError = !!entry.error;
            const msg = entry.message || entry.error || JSON.stringify(entry);

            const li = document.createElement('li');
            li.textContent = isError ? `❌ ${msg}` : msg;
            li.classList.add(isError ? 'text-red-600' : 'text-gray-600', 'font-semibold');

            list.appendChild(li);
        }
    });

    if (list.children.length > 0) {
        responseDiv.appendChild(list);
    }
}

