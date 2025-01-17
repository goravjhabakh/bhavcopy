document.getElementById('submit-button').addEventListener('click', () => {
    let date = document.querySelector('#date').value;

    axios.post('http://127.0.0.1:5000/bhavcopy', {date: date}).then(res => {
        //console.log(res.data.body)
        generateTable(res.data.body)
    }).catch(error => {
        console.error(error)
    })
})

const generateTable = (data) => {
    let tableContainer = document.getElementById('table-container');
    tableContainer.innerHTML = '';

    let table = document.createElement('table');
    table.border = '1';
    let thead = document.createElement('thead');
    let tbody = document.createElement('tbody');

    let headerRow = document.createElement('tr');
    data[0].forEach(headerText => {
        let th = document.createElement('th');
        th.textContent = headerText;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    for (let i = 1; i < data.length; i++) {
        let row = document.createElement('tr');
        data[i].forEach(cellData => {
            let td = document.createElement('td');
            td.textContent = cellData;
            row.appendChild(td);
        });
        tbody.appendChild(row);
    }

    table.appendChild(thead);
    table.appendChild(tbody);
    tableContainer.appendChild(table);
}