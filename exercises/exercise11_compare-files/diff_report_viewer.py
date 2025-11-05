from flask import Flask, render_template_string
import pyarrow.parquet as pq
import pandas as pd

app = Flask(__name__)


@app.route("/")
def show_diff_report():
    # Load difference report Parquet file (assumed downloaded locally as diff_report.parquet)
    table = pq.read_table("diff_report/diff_report.parquet")
    df = table.to_pandas()

    # Generate HTML using DataTables with Bootstrap styling
    html_table = df.to_html(classes="display nowrap table table-striped table-bordered", index=False)

    html_page = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>Parquet Difference Report</title>
      <link rel="stylesheet"
        href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap4.min.css"/>
      <link rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css"/>
      <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
      <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
      <script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap4.min.js"></script>
      <style>
        .container {{ margin-top: 20px; }}
        table.dataTable tbody tr:hover {{
          background-color: #f1f1f1;
        }}
      </style>
    </head>
    <body>
      <div class="container">
        <h2>Parquet Files Difference Report</h2>
        <p>Use the search box or column headers to filter and sort.</p>
        {html_table}
      </div>

      <script>
        $(document).ready(function() {{
          $('table.display').DataTable({{
            "scrollX": true,
            "pageLength": 25,
            "lengthMenu": [10, 25, 50, 100],
            "fixedHeader": true
          }});
        }});
      </script>
    </body>
    </html>
    """
    return render_template_string(html_page)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
