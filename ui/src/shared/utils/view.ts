// Constants
import {INFERNO, NINETEEN_EIGHTY_FOUR} from '@influxdata/giraffe'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {DEFAULT_CELL_NAME} from 'src/dashboards/constants/index'
import {
  DEFAULT_GAUGE_COLORS,
  DEFAULT_THRESHOLDS_LIST_COLORS,
} from 'src/shared/constants/thresholds'
import {DEFAULT_CHECK_EVERY} from 'src/alerting/constants'

// Types
import {
  ViewType,
  Base,
  XYViewProperties,
  HistogramViewProperties,
  HeatmapViewProperties,
  ScatterViewProperties,
  LinePlusSingleStatProperties,
  SingleStatViewProperties,
  MarkdownViewProperties,
  TableViewProperties,
  GaugeViewProperties,
  NewView,
  ViewProperties,
  DashboardQuery,
  BuilderConfig,
  Axis,
  Color,
  CheckViewProperties,
  CheckType,
} from 'src/types'

function defaultView() {
  return {
    name: DEFAULT_CELL_NAME,
  }
}

export function defaultViewQuery(): DashboardQuery {
  return {
    name: '',
    text: '',
    editMode: 'builder',
    builderConfig: defaultBuilderConfig(),
  }
}

export function defaultBuilderConfig(): BuilderConfig {
  return {
    buckets: [],
    tags: [{key: '_measurement', values: []}],
    functions: [],
    aggregateWindow: {period: 'auto'},
  }
}

function defaultLineViewProperties() {
  return {
    queries: [defaultViewQuery()],
    colors: DEFAULT_LINE_COLORS as Color[],
    legend: {},
    note: '',
    showNoteWhenEmpty: false,
    axes: {
      x: {
        bounds: ['', ''],
        label: '',
        prefix: '',
        suffix: '',
        base: '10',
        scale: 'linear',
      } as Axis,
      y: {
        bounds: ['', ''],
        label: '',
        prefix: '',
        suffix: '',
        base: '10' as Base,
        scale: 'linear',
      } as Axis,
    },
  }
}

function defaultGaugeViewProperties() {
  return {
    queries: [defaultViewQuery()],
    colors: DEFAULT_GAUGE_COLORS as Color[],
    prefix: '',
    suffix: '',
    note: '',
    showNoteWhenEmpty: false,
    decimalPlaces: {
      isEnforced: true,
      digits: 2,
    },
  }
}

function defaultSingleStatViewProperties() {
  return {
    queries: [defaultViewQuery()],
    colors: DEFAULT_THRESHOLDS_LIST_COLORS as Color[],
    prefix: '',
    suffix: '',
    note: '',
    showNoteWhenEmpty: false,
    decimalPlaces: {
      isEnforced: true,
      digits: 2,
    },
  }
}

// Defines the zero values of the various view types
const NEW_VIEW_CREATORS = {
  xy: (): NewView<XYViewProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: 'xy',
      shape: 'chronograf-v2',
      geom: 'line',
      xColumn: null,
      yColumn: null,
      position: 'overlaid',
    },
  }),
  histogram: (): NewView<HistogramViewProperties> => ({
    ...defaultView(),
    properties: {
      queries: [],
      type: 'histogram',
      shape: 'chronograf-v2',
      xColumn: '_value',
      xDomain: null,
      xAxisLabel: '',
      fillColumns: null,
      position: 'stacked',
      binCount: 30,
      colors: DEFAULT_LINE_COLORS as Color[],
      note: '',
      showNoteWhenEmpty: false,
    },
  }),
  heatmap: (): NewView<HeatmapViewProperties> => ({
    ...defaultView(),
    properties: {
      queries: [],
      type: 'heatmap',
      shape: 'chronograf-v2',
      xColumn: null,
      yColumn: null,
      xDomain: null,
      yDomain: null,
      xAxisLabel: '',
      yAxisLabel: '',
      xPrefix: '',
      xSuffix: '',
      yPrefix: '',
      ySuffix: '',
      colors: INFERNO,
      binSize: 10,
      note: '',
      showNoteWhenEmpty: false,
    },
  }),
  'single-stat': (): NewView<SingleStatViewProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultSingleStatViewProperties(),
      type: 'single-stat',
      shape: 'chronograf-v2',
      legend: {},
    },
  }),
  gauge: (): NewView<GaugeViewProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultGaugeViewProperties(),
      type: 'gauge',
      shape: 'chronograf-v2',
      legend: {},
    },
  }),
  'line-plus-single-stat': (): NewView<LinePlusSingleStatProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      ...defaultSingleStatViewProperties(),
      type: 'line-plus-single-stat',
      shape: 'chronograf-v2',
      xColumn: null,
      yColumn: null,
      position: 'overlaid',
    },
  }),
  table: (): NewView<TableViewProperties> => ({
    ...defaultView(),
    properties: {
      type: 'table',
      shape: 'chronograf-v2',
      queries: [defaultViewQuery()],
      colors: DEFAULT_THRESHOLDS_LIST_COLORS as Color[],
      tableOptions: {
        verticalTimeAxis: true,
        sortBy: null,
        fixFirstColumn: false,
      },
      fieldOptions: [],
      decimalPlaces: {
        isEnforced: false,
        digits: 2,
      },
      timeFormat: 'YYYY-MM-DD HH:mm:ss',
      note: '',
      showNoteWhenEmpty: false,
    },
  }),
  markdown: (): NewView<MarkdownViewProperties> => ({
    ...defaultView(),
    properties: {
      type: 'markdown',
      shape: 'chronograf-v2',
      note: '',
    },
  }),
  scatter: (): NewView<ScatterViewProperties> => ({
    ...defaultView(),
    properties: {
      type: 'scatter',
      shape: 'chronograf-v2',
      queries: [defaultViewQuery()],
      colors: NINETEEN_EIGHTY_FOUR,
      note: '',
      showNoteWhenEmpty: false,
      fillColumns: null,
      symbolColumns: null,
      xColumn: null,
      xDomain: null,
      yColumn: null,
      yDomain: null,
      xAxisLabel: '',
      yAxisLabel: '',
      xPrefix: '',
      xSuffix: '',
      yPrefix: '',
      ySuffix: '',
    },
  }),
  threshold: (): NewView<CheckViewProperties> => ({
    name: 'check',
    properties: {
      type: 'check',
      shape: 'chronograf-v2',
      checkID: '',
      queries: [
        {
          name: '',
          text: '',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [{key: '_measurement', values: []}],
            functions: [{name: 'mean'}],
            aggregateWindow: {period: DEFAULT_CHECK_EVERY},
          },
        },
      ],
      colors: NINETEEN_EIGHTY_FOUR,
    },
  }),
  deadman: (): NewView<CheckViewProperties> => ({
    name: 'check',
    properties: {
      type: 'check',
      shape: 'chronograf-v2',
      checkID: '',
      queries: [
        {
          name: '',
          text: '',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [{key: '_measurement', values: []}],
            functions: [],
          },
        },
      ],
      colors: NINETEEN_EIGHTY_FOUR,
    },
  }),
}

type CreateViewType = ViewType | CheckType

export function createView<T extends ViewProperties = ViewProperties>(
  viewType: CreateViewType = 'xy'
): NewView<T> {
  const creator = NEW_VIEW_CREATORS[viewType]

  if (!creator) {
    throw new Error(`no view creator implemented for view of type ${viewType}`)
  }

  return creator() as NewView<T>
}
