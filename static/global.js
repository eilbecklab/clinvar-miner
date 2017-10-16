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
