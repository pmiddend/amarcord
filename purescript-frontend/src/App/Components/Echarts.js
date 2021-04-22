"use strict";

exports.setEchartsOptions = chartObject => newOptions => () => {
    // console.log("Set chart options");
    chartObject.setOption(newOptions);
}

exports.initEcharts = elem => () => {
    // console.log("Initializing echarts");
    return echarts.init(elem);
}

exports.finalizeEcharts = chartObject => () => {
    // console.log("Finalizing echarts");
    echarts.dispose(chartObject);
}

