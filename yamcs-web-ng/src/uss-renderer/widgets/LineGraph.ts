import * as utils from '../utils';

import { AbstractWidget } from './AbstractWidget';
import { Parameter } from '../Parameter';

export class LineGraph extends AbstractWidget {

  private chart: any;

  private xAutoRange: boolean;
  private xRange: number;

  parseAndDraw(svg: any, parent: any, e: Node) {
    svg.group(parent, this.id, {transform: `translate(${this.x},${this.y})`});
    const type = utils.parseStringChild(e, 'Type');
    const title = utils.parseStringChild(e, 'Title');
    const labelsStyle = {style: {fontFamily: 'sans-serif', fontSize: '10px'}};

    const titleStyle = utils.parseTextStyle(utils.findChild(e, 'TitleTextStyle'));

    const settings: any = {
      chart: {
        renderTo: this.id,
        type: 'line',
        width: this.width,
        height: this.height,
        animation: false,
        clientZoom: false,
        spacingTop: 0,
        spacingBottom: 5,
      },
      legend: { enabled: false },
      credits: { enabled: false },
      title: { text: title, style: titleStyle },
      plotOptions: {
        line: {
          color: 'black',
          animation: false,
          shadow: false,
          marker: { enabled: false },
          enableMouseTracking: false
        },
        series: { lineWidth: 1 }
      }
    };
    settings.xAxis = this.parseDomainAxis(utils.findChild(e, 'DefaultDomainAxis'));
    settings.yAxis = this.parseRangeAxis(utils.findChild(e, 'DefaultRangeAxis'));
    settings.series = [{
      id: 'series-1',
      name: this.dataBindings[0].parameterName,
      data: []
    }];
    this.chart = new Highcharts.Chart(settings);
  }

  parseRangeAxis(e: Node) {
    const titleStyle = {fontFamily: 'sans-serif', fontSize: '12px', fontWeight: 'normal'};
    const labelStyle = {fontFamily: 'sans-serif', fontSize: '10px', fontWeight: 'normal'};

    labelStyle.color = titleStyle.color = utils.parseColorChild(e, 'AxisColor', 'black');
    const yaxis: any = {
      lineWidth: 1,
      lineColor: labelStyle.color,
      gridLineDashStyle: 'dash',
      labels: { style: labelStyle },
      startOnTick: false,
      tickPixelInterval: 20,
      endOnTick: false,
    };

    const label = utils.parseStringChild(e, 'Label');
    yaxis.title = {text: label, style: titleStyle};

    if (!utils.parseBooleanChild(e, 'AutoRange')) {
      const axisRangeNode = utils.findChild(e, 'AxisRange');
      yaxis.min = utils.parseFloatChild(axisRangeNode, 'Lower');
      yaxis.max = utils.parseFloatChild(axisRangeNode, 'Upper');
    }

    if (utils.parseBooleanChild(e, 'StickyZero', false)) {
      yaxis.min = 0;
    }
    return yaxis;
  }

  parseDomainAxis(e: Node) {
    const titleStyle = {fontFamily: 'sans-serif', fontSize: '12px', fontWeight: 'normal'};
    const labelStyle = {fontFamily: 'sans-serif', fontSize: '12px', fontWeight: 'normal'};

    const type = utils.parseStringChild(e, 'AxisMode').toLowerCase();
    if (type !== 'time_based_absolute') {
        console.log('TODO xaxis of type', type);
        return null;
    }
    labelStyle.color = titleStyle.color = utils.parseColorChild(e, 'AxisColor', 'black');
    const xaxis: any = {
      type: 'datetime',
      gridLineDashStyle: 'dash',
      labels: { style: labelStyle },
      startOnTick: false,
      gridLineWidth: 1,
    };

    const label = utils.parseStringChild(e, 'Label');
    xaxis.title = {text: label, style: titleStyle};

    this.xAutoRange = utils.parseBooleanChild(e, 'AutoRange');
    if (!this.xAutoRange) {
        const axisRangeNode = utils.findChild(e, 'AxisRange');
        xaxis.min = utils.parseFloatChild(axisRangeNode, 'Lower');
        xaxis.max = utils.parseFloatChild(axisRangeNode, 'Upper');
        this.xRange = xaxis.max - xaxis.min;
    }
    return xaxis;
  }

  updateValue(para: Parameter, usingRaw: boolean) {
    const series = this.chart.get('series-1');
    const value = this.getParameterValue(para, usingRaw);
    const t = para.generationTime;
    const xaxis = series.xAxis;
    if (!this.xAutoRange) {
      const extr = xaxis.getExtremes();
      if (extr.max < t) {
        const s = this.xRange / 3;
        xaxis.setExtremes(t + s - this.xRange, t + s);
      }
    }
    series.addPoint([t, value]);
  }
}
