function downloadTableAsCsv(tableId) {
    var tableEl = document.getElementById(tableId);
    var firstMeaningfulColumn = tableEl.classList.contains('filterable') ? 1 : 0;
    var csvText = ''
    for (var sectionEl of tableEl.children) { //the table must have <thead> and <tbody> elements
        for (var rowEl of sectionEl.children) {
            cells = [];
            for (i = firstMeaningfulColumn; i < rowEl.children.length; i++) {
                cells.push('"' + rowEl.children[i].textContent.trim().replace('"', '""') + '"');
            }
            csvText += cells.join(',') + '\n';
        }
    }
    var link = document.createElement('a');
    link.href = URL.createObjectURL(new Blob([csvText], {type: 'text/csv; charset=utf-8'}));
    link.download = tableId + '.csv';
    link.dispatchEvent(new MouseEvent('click')); //the regular click function only works if the link is in the document
}

function filterTable(tableId, q) {
    var tableEl = document.getElementById(tableId);
    var tbodyEl = tableEl.getElementsByTagName('tbody')[0];
    var firstMeaningfulColumn = tableEl.classList.contains('filterable') ? 1 : 0;
    q = q.toLowerCase();
    for (var rowEl of tbodyEl.children) {
        if (rowEl.children[firstMeaningfulColumn].textContent.toLowerCase().indexOf(q) == -1)
            rowEl.style.display = 'none';
        else
            rowEl.style.display = '';
    }
}

$('table.sortable').each(function() {
    var headers;

    // instead of sorting, check or uncheck all the boxes when clicking the "Filter" column header
    if ($(this).hasClass('filterable')) {
        var table = $(this);
        table.find('thead tr:first-child th:first-child').on('click', function () {
            var checkedBoxes = 0, uncheckedBoxes = 0;
            table.find('tbody td:first-child input').each(function() {
                if ($(this).prop('checked'))
                    checkedBoxes++;
                else
                    uncheckedBoxes++;
            });
            table.find('tbody td:first-child input').prop('checked', uncheckedBoxes >= checkedBoxes);
        });
        headers = {0: {sorter: false}};
    }

    $(this).tablesorter({
        /* the default class names are backwards */
        cssAsc: 'desc',
        cssDesc: 'asc',
        headers: headers,
    });
});
