function downloadTableAsCsv(tableId) {
    var table = document.getElementById(tableId);
    var firstMeaningfulColumn = table.classList.contains('filterable') ? 1 : 0;
    var csvText = ''
    for (sectionEl of table.children) { //the table must have <thead> and <tbody> elements
        for (rowEl of sectionEl.children) {
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
