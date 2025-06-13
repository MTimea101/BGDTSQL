document.getElementById('uploadSqlIcon').addEventListener('click', () => {
    document.getElementById('sqlFileInput').click();
});

document.getElementById('sqlFileInput').addEventListener('change', (event) => {
    const file = event.target.files[0];
    if (!file) return;

    if (!file.name.endsWith('.sql')) {
        alert('Please upload a valid .sql file!');
        return;
    }

    const reader = new FileReader();

    reader.onload = function (e) {
        const sqlContent = e.target.result.trim();

        // beleteszi az input-ot a szovegreszbe
        const sqlInput = document.getElementById('sqlInput');
        sqlInput.value = sqlContent;

        // nem toltheti fel egyszerre tobbszor ugyan azt a file-t
        document.getElementById('sqlFileInput').value = "";
    };

    reader.readAsText(file);
});
