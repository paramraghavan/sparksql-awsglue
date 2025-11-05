from flask import Flask, request, render_template_string
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

app = Flask(__name__)
spark = SparkSession.builder.appName("DiffReportHTML").getOrCreate()

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Paginated Sorted Filtered Difference Report</title>
    <style>
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; }
        th { background-color: #f2f2f2; cursor: pointer; }
        tr.diff-row { background-color: #ffe6e6; }
        td.diff-cell { background-color: #ff9999; font-weight: bold; }
        .pagination { margin: 10px 0; }
        .pagination a { margin: 0 5px; text-decoration: none; }
        .pagination a.current { font-weight: bold; }
        .filter-input { margin: 10px 5px; }
    </style>
</head>
<body>
    <h2>Difference Report</h2>

    <form method="get">
        <label>Filter Column:
            <select name="filter_col">
                <option value="">--None--</option>
                {% for col in columns %}
                  <option value="{{ col }}" {% if col == filter_col %}selected{% endif %}>{{ col }}</option>
                {% endfor %}
            </select>
        </label>
        <label>Filter Value:
            <input type="text" name="filter_val" value="{{ filter_val|default('') }}" class="filter-input"/>
        </label>
        <input type="submit" value="Apply Filter"/>
    </form>

    <table>
        <thead>
            <tr>
                {% for col in columns %}
                    <th>
                        <a href="?page=1&sort_col={{ col }}&sort_dir={% if sort_col==col and sort_dir=='asc' %}desc{% else %}asc{% endif %}{% if filter_col %}&filter_col={{ filter_col }}{% endif %}{% if filter_val %}&filter_val={{ filter_val }}{% endif %}">
                            {{ col }}
                            {% if sort_col==col %}
                                {% if sort_dir == 'asc' %}▲{% else %}▼{% endif %}
                            {% endif %}
                        </a>
                    </th>
                {% endfor %}
            </tr>
        </thead>
        <tbody>
            {% for row in data %}
                <tr class="{{ 'diff-row' if row.is_diff else '' }}">
                {% for col in columns %}
                    <td class="{{ 'diff-cell' if col.endswith('_diff') and row[col] else '' }}">
                        {{ row[col] }}
                    </td>
                {% endfor %}
                </tr>
            {% endfor %}
        </tbody>
    </table>

    <div class="pagination">
        {% for p in range(1, total_pages+1) %}
            {% if p == page %}
                <a href="?page={{ p }}{% if sort_col %}&sort_col={{ sort_col }}{% endif %}{% if sort_dir %}&sort_dir={{ sort_dir }}{% endif %}{% if filter_col %}&filter_col={{ filter_col }}{% endif %}{% if filter_val %}&filter_val={{ filter_val }}{% endif %}" class="current">{{ p }}</a>
            {% else %}
                <a href="?page={{ p }}{% if sort_col %}&sort_col={{ sort_col }}{% endif %}{% if sort_dir %}&sort_dir={{ sort_dir }}{% endif %}{% if filter_col %}&filter_col={{ filter_col }}{% endif %}{% if filter_val %}&filter_val={{ filter_val }}{% endif %}">{{ p }}</a>
            {% endif %}
        {% endfor %}
    </div>
</body>
</html>
"""

@app.route("/")
def paginated_report():
    page = int(request.args.get("page", 1))
    page_size = 50
    sort_col = request.args.get("sort_col", "id")
    sort_dir = request.args.get("sort_dir", "asc")
    filter_col = request.args.get("filter_col", None)
    filter_val = request.args.get("filter_val", None)

    df = spark.read.parquet("s3://bucket/path/diff_report/")

    # Filtering
    if filter_col and filter_val:
        df = df.filter(col(filter_col).like(f"%{filter_val}%"))

    total_count = df.count()
    total_pages = (total_count + page_size - 1) // page_size
    offset = (page - 1) * page_size

    # Sorting
    if sort_col in df.columns:
        if sort_dir == "asc":
            df = df.orderBy(sort_col)
        else:
            df = df.orderBy(df[sort_col].desc())

    # Pagination logic
    page_data_df = df.limit(offset + page_size)
    if offset > 0:
        page_data_df = spark.createDataFrame(page_data_df.tail(page_size), df.schema)

    page_data_pdf = page_data_df.toPandas()
    rows = page_data_pdf.to_dict(orient="records")

    for row in rows:
        row["is_diff"] = any(row.get(col, False) for col in df.columns if col.endswith("_diff"))

    return render_template_string(
        HTML_TEMPLATE,
        data=rows,
        columns=df.columns,
        page=page,
        total_pages=total_pages,
        sort_col=sort_col,
        sort_dir=sort_dir,
        filter_col=filter_col,
        filter_val=filter_val,
    )


if __name__ == "__main__":
    app.run(debug=True)
