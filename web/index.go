package web

import (
	"net/http"
)

func (s *server) index(resp http.ResponseWriter, req *http.Request) {
	if !s.authenticate(resp, req) {
		return
	}

	resp.Header().Set("Content-Type", "text/html")
	resp.WriteHeader(http.StatusOK)
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
    </div>

    {{#if result}}
    <h3>Results at {{ date }}</h3>

    <table class="table table-striped" style="margin-top: 10px;">
      <thead>
        <tr>
          <th>Time</th>
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
          <th>{{ formatTS(row.TS) }}</th>
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

  <script src='https://cdnjs.cloudflare.com/ajax/libs/ractive/0.8.7/ractive-legacy.js'></script>

  <script type="text/javascript">
    function formatTS(ts) {
      return new Date(ts/1000).toString();
    }

    function runQuery() {
      ractive.set("running", true);
      var query = editor.getValue();
      console.log("Running query", query);
      var xhr = new XMLHttpRequest();
      xhr.open('GET', encodeURI('/run/' + query), true);

      xhr.onreadystatechange = function(e) {
        if (this.readyState == 4 && this.status == 200) {
          ractive.set("date", new Date());
          ractive.set("result", JSON.parse(this.responseText));
        }
        ractive.set("running", false);
      };

      xhr.send();
    }

    // Set up two-way data binding with ractive
    var ractive = new Ractive({el: '#container', template: '#template', data: {
      "sql": "",
      "running": false,
      "result": null,
      "formatTS": formatTS,
      "date": null,
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
        if (context.params.sql) {
          editor.setValue(context.params.sql);
          runQuery();
        }
        sqlInitialized = true;
      }
    }

    page('/', '/query');
    page('/query/:sql', handlePage);
    page({
      decodeURLComponents: false,
    });

    // Periodically check editor value and update link
    setInterval(function() {
      page(encodeURI('/query/' + editor.getValue()));
    }, 1000);
  </script>
</body>
</html>
`)
