document.addEventListener('DOMContentLoaded', function () {
const chart = Highcharts.chart('container', {
    chart: {
        type: 'bar'
    },
    title: title,
    xAxis: { categories: xAxis,
        labels: {
            formatter: function () {
                return '<a href="https://twitter.com/search?q=' + this.value + '&src=typed_query&f=live" target="_blank">' + this.value + '</a>';
            },
            useHTML: true
        }
    },
    yAxis: yAxis,
    series: series
});
});
