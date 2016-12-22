package web

import (
	// "io/ioutil"
	"net/http"
)

func (h *handler) index(resp http.ResponseWriter, req *http.Request) {
	if !h.authenticate(resp, req) {
		return
	}

	resp.Header().Set("Content-Type", "text/html")
	resp.WriteHeader(http.StatusOK)
	// bytes, _ := ioutil.ReadFile("index.html")
	// resp.Write(bytes)
	resp.Write(indexHTML)
}

var indexHTML = []byte(`
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <!-- The above 3 meta tags *must* come first in the head; any other head content must come *after* these tags -->
  <title>ZenoDB</title>

  <!-- Bootstrap 3 -->
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css" integrity="sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u" crossorigin="anonymous">

  <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/page.js/1.7.1/page.min.js"></script>

  <style type="text/css" media="screen">
    #sql {
      width: 100%;
      height: 200px;
    }

    table .dim {
      text-align: left;
    }

    table .field {
      text-align: left;
    }

    h4 {
      margin-top: 20px;
    }

    .error {
      font-size: 1.5em;
      font-weight: bold;
      color: red;
      vertical-align: middle;
      margin-left: 10px;
    }

    .summary {
      font-size: 1.5em;
      vertical-align: middle;
      margin-left: 10px;
    }

    .defaultHide {
      display: none;
    }

    /* Spinning courtesy of http://chadkuehn.com/animated-font-spinners/ */
    .glyphicon-spin {
      -webkit-animation: spin 1000ms infinite linear;
      animation: spin 1000ms infinite linear;
    }
    @-webkit-keyframes spin {
        0% {
            -webkit-transform: rotate(0deg);
            transform: rotate(0deg);
        }
        100% {
            -webkit-transform: rotate(359deg);
            transform: rotate(359deg);
        }
    }
    @keyframes spin {
        0% {
            -webkit-transform: rotate(0deg);
            transform: rotate(0deg);
        }
        100% {
            -webkit-transform: rotate(359deg);
            transform: rotate(359deg);
        }
    }
  </style>
</head>
<body style="padding: 0px 10px 10px 10px;">
  <div id='container'></div>
  <script id='template' type='text/ractive'>
    <h3>ZenoDB | SQL Query</h3>

    <div id="sql">{{ sql }}</div>

    <div style="margin-top: 10px;">
      <button type="button" class="btn btn-default" aria-label="Left Align" on-click="run" {{#if running}}disabled{{/if}}>
        <span class="glyphicon {{#if running}}glyphicon-refresh glyphicon-spin{{else}}glyphicon-play{{/if}}" aria-hidden="true"></span> Run
      </button>
      {{#if !running}}
        {{#if error}}<span class="error">Error: {{ error }}</span>{{else}}<span class="summary">{{#if result.Rows}}{{ result.Rows.length}}{{else}}No{{/if}} results at {{ date }}</span>{{/if}}
      {{/if}}
    </div>

    <div class="defaultHide" style="margin: 10px; {{#if plottingNotSupported}}display: block;{{/if}}">
      <h3>Autoplotting Not Supported for this Query</h3>
      <p>Zeno currently supports three types of auto-plot:</p>

      <h4>1. Time Series</h4>
      <ul>
        <li>Supports arbitrary number of fields</li>
        <li>Works only when grouping by 0 dimensions</li>
        <li>Must include more than 1 _time</li>
      </ul>

      <h5>Example</h5>
      <pre><code>SELECT requests, load_avg, _points
FROM combined
GROUP BY _
ORDER BY _time</code></pre>

      <h4>2. Bubble Chart</h4>
      <ul>
        <li>Works when selecting 2 or 3 fields</li>
        <li>If 3rd field specified, it is used to size the bubbles, otherwise the bubbles are all equally sized</li>
        <li>Works only when grouping by 1 dimension</li>
        <li>Must not include multiple time periods</li>
      </ul>

      <h5>Example</h5>
      <pre><code>SELECT requests, load_avg, _points
FROM combined
GROUP BY server, period(24h)
ORDER BY server</code></pre>

      <h4>3. Bar Chart</h4>
      <ul>
        <li>Supports arbitrary number of fields other than 2 or 3 (see Bubble Chart above)</li>
        <li>Works only when grouping by 1 dimension</li>
        <li>Must not include multiple time periods</li>
      </ul>

      <h5>Example</h5>
      <pre><code>SELECT _points
FROM combined
GROUP BY server, period(24h)
ORDER BY server</code></pre>
    </div>

    {{#if result && result.Rows}}
      <div id="chartDiv" class="defaultHide" style="margin-top: 20px; width: 100%; {{#if showTimeSeriesChart}}display: block;{{/if}}"></div>
      <canvas id="chartCanvas" width="600" height="200" class="defaultHide" style="margin-top: 20px; {{#if showOtherChart}}display: block;{{/if}}"></canvas>

      <table class="table table-striped" style="margin-top: 10px;">
        <thead>
          <tr>
            {{#if result.TSCardinality > 1}}
              <th>Time</th>
            {{/if}}
            {{#each result.Dims}}
              <th class="dim"><th>{{ . }}</th>
            {{/each}}
            {{#each result.Fields}}
              <th class="field">{{ . }}</th>
            {{/each}}
          </tr>
        </thead>
        <tbody>
          {{#each result.Rows as row}}
          <tr>
            {{#if result.TSCardinality > 1}}
              <th>{{ formatTS(row.TS) }}</th>
            {{/if}}
            {{#each result.Dims as dim}}
              <th class="dim"><th>{{ row.Key[dim] }}</th>
            {{/each}}
            {{#each row.Vals as val}}
              <th class="field">{{ val }}</th>
            {{/each}}
          </tr>
          {{/each}}
        </tbody>
      </table>
    {{/if}}
  </script>

  <!-- ACE Code Editor -->
  <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/ace/1.2.6/ace.js"></script>

  <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/ractive/0.8.7/ractive-legacy.js"></script>

  <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/dygraph/1.1.1/dygraph-combined.js"></script>

  <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.4.0/Chart.min.js"></script>

  <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/seedrandom/2.4.2/seedrandom.min.js"></script>

  <script type="text/javascript">
    // Set up two-way data binding with ractive
    var ractive = new Ractive({el: '#container', template: '#template', data: {
      "sql": "",
      "running": false,
      "result": null,
      "error": null,
      "formatTS": formatTS,
      "date": null,
      "showTimeSeriesChart": false,
      "showOtherChart": false,
    }});
    ractive.on("run", runQuery);

    // Set up ace editor
    var editor = ace.edit("sql");
    editor.setTheme("ace/theme/monokai");
    editor.getSession().setMode("ace/mode/mysql");

    var sqlInitialized = false;
    // Handle routing
    function handlePage(context, next) {
      if (!sqlInitialized) {
        console.log(context);
        if (context.querystring) {
          editor.setValue(context.querystring);
          runQuery();
        }
        sqlInitialized = true;
      }
    }

    page('/', '/query');
    page('/query', handlePage);
    page({
      decodeURLComponents: true,
    });

    // Periodically check editor value and update link
    setInterval(function() {
      var sql = editor.getValue();
      if (sql) {
        page('/query?' + encodeURIComponent(sql));
      }
    }, 1000);

    function runQuery() {
      ractive.set("running", true);
      ractive.set("result", null);
      ractive.set("error", null);
      ractive.set("showTimeSeriesChart", false);
      ractive.set("showOtherChart", false);
      ractive.set("plottingNotSupported", false);

      var query = editor.getValue();
      console.log("Running query", query);
      var xhr = new XMLHttpRequest();
      xhr.open('GET', '/run?' + encodeURIComponent(query), true);

      xhr.onreadystatechange = function(e) {
        if (this.readyState == 4) {
          if (this.status == 200) {
            ractive.set("date", new Date());
            var result = JSON.parse(this.responseText);
            console.log(result);
            ractive.set("result", result);

            if (result.Rows) {
              plot(result);
            }
          } else {
            ractive.set("error", this.status + " - " + this.responseText);
          }
        }

        ractive.set("running", false);
      };

      xhr.send();
    }

    function plot(result) {
      // Always seed random number generator with same value so that colors are
      // generated in a repeatable way.
      Math.seedrandom('always generate colors in same order');
      if (result.TSCardinality > 1) {
        // Timeseries, use dygraph
        if (result.Dims.length == 0) {
          ractive.set("showTimeSeriesChart", true);
          return timeseriesMultiField(result);
        }
      } else {
        // Not a timeseries, use chart.js
        var canvas = document.getElementById("chartCanvas");
        if (result.Dims.length == 1) {
          ractive.set("showOtherChart", true);
          if (result.Fields.length >= 2 && result.Fields.length <= 3) {
            return bubblechart(canvas, result);
          }
          return barchartMultiField(canvas, result);
        }
      }

      ractive.set("plottingNotSupported", true);
    }

    function timeseriesMultiField(result) {
      var labels = [];
      labels.push("time");
      labels = labels.concat(result.Fields);
      var data = [];
      result.Rows.forEach(function(row) {
        var rowData = [];
        rowData.push(new Date(row.TS));
        rowData = rowData.concat(row.Vals);
        data.push(rowData);
      });

      var g = new Dygraph(
        // containing div
        document.getElementById("chartDiv"),
        data,
        {
          labels: labels,
          includeZero: true,
          legend: 'always',
        });
    }

    function barchartMultiField(canvas, result) {
      var labels = [];
      var series = [];
      result.Fields.forEach(function() {
        series.push([]);
      });
      result.Rows.forEach(function(row) {
        labels.push(row.Key[result.Dims[0]]);
        row.Vals.forEach(function(val, idx) {
          series[idx].push(val);
        })
      });

      var datasets = [];
      series.forEach(function(series, idx) {
        var color = randomColor();
        datasets.push({
          label: result.Fields[idx],
          data: series,
          backgroundColor: color,
          hoverBackgroundColor: color,
        });
      });

      var myChart = new Chart(canvas, {
        type: 'bar',
        data: {
          labels: labels,
          datasets: datasets,
        },
        options: {
          scales: {
            yAxes: [{
              ticks: {
                beginAtZero:true
              }
            }]
          }
        }
      });
    }

    function bubblechart(canvas, result) {
      var calculateR = result.Fields.length == 3;
      var datasets = [];

      result.Rows.forEach(function(row) {
        var datum = {
          x: row.Vals[0],
          y: row.Vals[1],
          r: 10,
        }
        if (calculateR) {
          datum.r = row.Vals[2];
        }
        var color = randomColor();
        datasets.push({
          label: row.Key[result.Dims[0]],
          data: [datum],
          backgroundColor: color,
          hoverBackgroundColor: color,
        });
      });

      var myChart = new Chart(canvas, {
        type: 'bubble',
        data: {
          datasets: datasets,
        },
        options: {
          legend: {
            display: false,
          },
          scales: {
            xAxes: [{
              scaleLabel: {
                display: true,
                labelString: result.Fields[0],
              },
              ticks: {
                beginAtZero:true
              }
            }],
            yAxes: [{
              scaleLabel: {
                display: true,
                labelString: result.Fields[1],
              },
              ticks: {
                beginAtZero:true
              }
            }]
          }
        }
      });
    }

    function formatTS(ts) {
      return new Date(ts).toString();
    }

    // Courtesty of http://stackoverflow.com/questions/25594478/different-color-for-each-bar-in-a-bar-chart-chartjs
    function randomColor() {
      var letters = '0123456789ABCDEF'.split('');
      var color = '#';
      for (var i = 0; i < 6; i++ ) {
        color += letters[Math.floor(Math.random() * 16)];
      }
      return color;
    }
  </script>
</body>
</html>
`)
