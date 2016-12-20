function lineGraph(data, yAxisText, yAxisTickValues) {
    var width = 800, height = 600, margin = 100;
    var series = {};
    var totals = {};
    data.forEach(function(d) {
        series[d.serie] = true;
        if (typeof(totals[d.x]) == 'undefined')
            totals[d.x] = 0;
        totals[d.x] += d.y;
    });
    series = Object.keys(series);
    if (series.length > 1) {
        Object.keys(totals).forEach(function(x) {
            data.push({
                x: Date.parse(x),
                y: totals[x],
                serie: 'total',
            });
        });
        series.unshift('total');
    }

    var svg = d3
        .select('#graph')
        .attr('width', width)
        .attr('height', height);
    width -= margin * 2;
    height -= margin * 2;
    var g = svg
        .append('g')
        .attr('transform', 'translate(' + margin + ',' + margin + ')')
    var x = d3
        .scaleTime()
        .rangeRound([0, width])
        .domain(d3.extent(data, d => d.x));
    var y = d3
        .scaleLinear()
        .rangeRound([height, 0])
        .domain([0, yAxisTickValues[yAxisTickValues.length - 1]]);
    series.forEach((serie, i) => {
        var line = d3.line()
            .x(d => x(d.x))
            .y(d => y(d.y));
        g.append('path')
            .datum(data.filter(d => (d.serie == serie)))
            .attr('class', 'line')
            .attr('d', line)
            .attr('style', 'stroke:' + d3.schemeCategory10[i]);
    });
    var xAxis = d3
        .axisBottom(x)
        .ticks(d3.timeYear.every(1))
        .tickFormat(d3.timeFormat('%Y'));
    var yAxis = d3
        .axisLeft(y)
        .tickValues(yAxisTickValues);
    g.append('g')
        .attr('class', 'axis axis--x')
        .attr('transform', 'translate(0,' + height + ')')
        .call(xAxis);

    g.append('g')
        .attr('class', 'axis axis--y')
        .call(yAxis)
        .append('text')
        .attr('fill', '#000')
        .attr('transform', 'rotate(-90)')
        .attr('y', 6)
        .attr('dy', '0.71em')
        .style('text-anchor', 'end')
        .text(yAxisText);

    if (series.length > 1) {
        var legend = g.append('g')
            .attr('transform', 'translate(50,0)')
        series.forEach((serie, i) => {
            var legendItem = legend.append('g')
                .attr('transform', 'translate(0,' + (15 * i) + ')')

            legendItem.append('path')
                .attr('class', 'line')
                .attr('d', 'M0,-4L20,-4')
                .attr('style', 'stroke-width:5px; stroke:' + d3.schemeCategory10[i]);
            legendItem.append('text')
                .attr('transform', 'translate(30,0)')
                .text(serie);
        });
    }
}
