function lineGraph(data, yAxisText, yAxisTickValues) {
    var width = 800, height = 600, margin = 100;
    var series = [];
    var totals = {};

    data.forEach((d) => {
        // find the current value of each serie
        if (!series.hasOwnProperty(d.serie))
            series[d.serie] = {maxDate: null, valueAtMaxDate: 0};
        if (d.x > series[d.serie].maxDate) {
            series[d.serie].maxDate = d.x
            series[d.serie].valueAtMaxDate = d.y
        }

        // compute totals for each x-value
        if (!totals.hasOwnProperty(d.x))
            totals[d.x] = 0;
        totals[d.x] += d.y;
    });

    // make the first serie the serie with the highest current value,
    // and the last serie the serie with the lowest current value
    series = Object.keys(series).sort((a, b) => {
        if (series[a].valueAtMaxDate < series[b].valueAtMaxDate)
            return 1;
        else if (series[a].valueAtMaxDate > series[b].valueAtMaxDate)
            return -1;
        else
            return 0;
    });

    // add a "total" serie for the totals for each date
    if (series.length > 1) {
        Object.keys(totals).forEach((x) => {
            data.push({
                x: Date.parse(x),
                y: totals[x],
                serie: 'total',
            });
        });
        series.unshift('total');
    }

    // set up the graph
    var svg = d3
        .select('#graph')
        .attr('width', width)
        .attr('height', height);
    width -= margin * 2;
    height -= margin * 2;
    var g = svg
        .append('g')
        .attr('transform', 'translate(' + margin + ',' + margin + ')')

    // draw the lines
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

    // draw the axes
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

    // draw the legend
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
