import { fetchDatabases, refreshDbList, fetchTables } from './api.js';
import { renderTables, renderServerResponse } from './dom.js';
import { bindFormSubmit, bindLoadButton, bindDbSelector, bindToggleButton } from './events.js';

document.addEventListener('DOMContentLoaded', () => {
    fetchDatabases(); 
    bindDbSelector();
    bindFormSubmit(renderServerResponse, refreshDbList);
    bindLoadButton(fetchTables, renderTables);
    bindToggleButton();
});
