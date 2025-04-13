#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Jupyter Notebook to Python Converter with Optimization Analysis
This script converts Jupyter notebooks to Python files and analyzes
the code for potential optimizations in Spark, TensorFlow, and other frameworks.
"""

import json
import re
import os
import sys
import ast
import argparse
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional, Any


class OptimizationAnalyzer:
    """Analyzes code for potential optimizations in Spark, TensorFlow, and other frameworks."""

    def __init__(self):
        # Define patterns to look for in the code
        self.spark_patterns = {
            "cache_or_persist": (
                r"\.cache\(\)|\.persist\(\)",
                "Consider optimizing DataFrame caching strategy. Use persist() with appropriate storage level instead of cache() for fine-grained control."
            ),
            "collect": (
                r"\.collect\(\)",
                "Be cautious with collect() as it brings all data to the driver node. Consider using take(), sample(), or other actions that return smaller results."
            ),
            "repartition": (
                r"\.repartition\(\d+\)",
                "Check if repartition is necessary. It's an expensive operation that causes a full shuffle."
            ),
            "select_star": (
                r"\.select\(\*\)|\.select\(['\"].*['\"]\.?\)",
                "Avoid SELECT * patterns. Only select columns that are needed to reduce data transfer."
            ),
            "join_without_hint": (
                r"\.join\(.*\)",
                "Consider using join hints (broadcast, merge, shuffle_hash, etc.) for large tables to optimize join strategy."
            ),
            "multiple_actions": (
                r"(\.count\(\).*\.collect\(\))|(\.collect\(\).*\.count\(\))",
                "Multiple actions on the same RDD/DataFrame trigger multiple jobs. Consider caching if the same data is used multiple times."
            ),
            "sparkcontext_parallelize": (
                r"sc\.parallelize\(.*\)",
                "For large collections, consider using more efficient data loading methods than parallelize()."
            ),
            "many_small_partitions": (
                r"\.repartition\(\d{3,}\)",
                "Too many small partitions can cause overhead. Aim for partition sizes of at least 100MB each."
            ),
            "udf_use": (
                r"udf\(|UserDefinedFunction",
                "UDFs in PySpark are slower than built-in functions due to serialization costs. Consider using Pandas UDFs or built-in functions."
            ),
            "broadcast_large_table": (
                r"broadcast\(.*\)",
                "Ensure broadcasted tables are small enough to fit in memory. Large broadcast joins can cause out of memory errors."
            )
        }

        self.tensorflow_patterns = {
            "eager_execution": (
                r"tf\.executing_eagerly\(\)|tensorflow\.executing_eagerly\(\)",
                "Consider using tf.function to enable graph execution for better performance in production."
            ),
            "session_run": (
                r"\.run\(|sess\.run\(|session\.run\(",
                "Consider migrating from TensorFlow 1.x session.run() to TensorFlow 2.x eager execution or tf.function."
            ),
            "small_batch_size": (
                r"batch_size\s*=\s*[1-9]\d{0,1}(?!\d)",
                "Small batch sizes may underutilize GPU/TPU. Consider increasing if memory allows."
            ),
            "inefficient_data_pipeline": (
                r"for\s+.*\s+in\s+dataset",
                "For-loops over datasets may be inefficient. Use tf.data pipeline with prefetching and parallel processing."
            ),
            "missing_prefetch": (
                r"\.batch\(.*\)(?!\.prefetch\()",
                "Consider adding .prefetch() to data pipeline for parallel data loading and model execution."
            ),
            "no_gpu_memory_growth": (
                r"tf\.config\.experimental\.set_memory_growth|tensorflow\.config\.experimental\.set_memory_growth",
                "GPU memory growth is being configured, which is good for memory management."
            ),
            "no_xla_optimization": (
                r"jit_compile\s*=\s*(?:True|False)|tf\.function\(.*jit_compile\s*=\s*(?:True|False)\)",
                "XLA compilation is being configured, which is good for optimizing computational graphs."
            ),
            "inefficient_layer_usage": (
                r"for\s+.*\s+in\s+range.*:.*model\(|while\s+.*:.*model\(",
                "Sequential calls to model in a loop might be slower. Consider vectorizing operations."
            ),
            "no_mixed_precision": (
                r"mixed_precision\.set_global_policy|tensorflow\.keras\.mixed_precision",
                "Mixed precision training is being configured, which is good for performance on supporting hardware."
            )
        }

        self.pandas_patterns = {
            "apply_instead_of_vectorized": (
                r"\.apply\(lambda.*:.*\)",
                "Use vectorized operations instead of .apply() with lambda functions for better performance."
            ),
            "iterrows": (
                r"\.iterrows\(\)",
                "iterrows() is slow. Use vectorized operations or .apply() if vectorization is not possible."
            ),
            "memory_copy": (
                r"df\[.*\]\s*=",
                "Check for unnecessary DataFrame copies. Use inplace=True or methods like loc/iloc when appropriate."
            ),
            "ignore_index_merge": (
                r"\.merge\(.*\)(?!.*ignore_index)",
                "Consider using ignore_index=True in merge() for better performance if original indices are not needed."
            ),
            "read_csv_without_dtypes": (
                r"pd\.read_csv\((?!.*dtype=)",
                "Specify dtypes in read_csv() to avoid pandas inferring types and improve loading performance."
            ),
            "inefficient_groupby": (
                r"\.groupby\(.*\)\.apply\(",
                "GroupBy operations with apply() can be slow. Consider using built-in aggregation methods."
            ),
            "copy_warning": (
                r"\.copy\(\)",
                "Check if explicit copies are necessary. Unnecessary copies can consume memory."
            )
        }

        self.common_patterns = {
            "nested_loops": (
                r"for\s+.*\s+in\s+.*:\s*\n\s*for\s+.*\s+in\s+.*:",
                "Nested loops can lead to O(n²) or worse complexity. Consider vectorization or more efficient algorithms."
            ),
            "inefficient_list_building": (
                r"for\s+.*\s+in\s+.*:\s*\n\s*.*\.append\(",
                "Building lists with append in loops is inefficient. Consider list comprehensions or generators."
            ),
            "memory_leak_potential": (
                r"(?:plt\.figure|plt\.subplot|figure\()\s*.*(?!\s*plt\.close)",
                "Matplotlib figures that aren't closed can cause memory leaks in long-running notebooks."
            ),
            "global_variables": (
                r"(?<!\s*def\s|\s*class\s|\s*import\s|\s*from\s)^[a-zA-Z_][a-zA-Z0-9_]*\s*=",
                "Global variables can cause unexpected behavior and memory issues. Consider encapsulating in functions."
            ),
            "hardcoded_paths": (
                r"['\"](?:/[^/\n]*)+['\"]|['\"](?:[A-Za-z]:\\[^\\\\]*)+['\"]",
                "Hardcoded file paths can make code less portable. Consider using configuration files or environment variables."
            ),
            "large_data_chunk": (
                r"chunk_size\s*=\s*(?:\d{7,}|[1-9]\d{1,2}(?:_\d{3}){1,3})",
                "Very large chunk sizes might cause memory issues. Consider streaming or processing in smaller chunks."
            ),
            "magic_numbers": (
                r"(?<!\s*def\s|\s*class\s|\s*import\s|\s*from\s)=\s*\d{3,}(?!\s*\.\d)",
                "Magic numbers make code harder to maintain. Consider using named constants."
            )
        }

        # Combine all patterns
        self.all_patterns = {}
        self.all_patterns.update(self.spark_patterns)
        self.all_patterns.update(self.tensorflow_patterns)
        self.all_patterns.update(self.pandas_patterns)
        self.all_patterns.update(self.common_patterns)

        # Initialize results
        self.results = {}

    def analyze_code(self, code: str, cell_number: int = None) -> Dict[str, List[str]]:
        """
        Analyze a code block for potential optimizations.

        Args:
            code: The code string to analyze
            cell_number: Optional cell number for reference

        Returns:
            Dictionary with pattern categories and suggestions
        """
        cell_results = {}

        for pattern_name, (regex, suggestion) in self.all_patterns.items():
            matches = re.findall(regex, code)
            if matches:
                category = self._get_category_from_pattern(pattern_name)
                if category not in cell_results:
                    cell_results[category] = []

                # Format the message differently based on whether we have a cell number
                if cell_number is not None:
                    message = f"Cell {cell_number}: {suggestion} (pattern: {pattern_name})"
                else:
                    message = f"{suggestion} (pattern: {pattern_name})"

                cell_results[category].append(message)

        # Advanced AST analysis for more complex patterns
        try:
            tree = ast.parse(code)
            ast_results = self._analyze_ast(tree, cell_number)

            # Merge AST results with regex results
            for category, suggestions in ast_results.items():
                if category not in cell_results:
                    cell_results[category] = []
                cell_results[category].extend(suggestions)
        except SyntaxError:
            # If code can't be parsed (e.g., contains magic commands), skip AST analysis
            pass

        return cell_results

    def _analyze_ast(self, tree: ast.AST, cell_number: Optional[int] = None) -> Dict[str, List[str]]:
        """
        Perform advanced analysis using the AST.

        Args:
            tree: The AST tree to analyze
            cell_number: Optional cell number for reference

        Returns:
            Dictionary with categories and suggestions
        """
        results = {}

        # Function to check for large literal collections
        large_collections = []

        class LargeCollectionVisitor(ast.NodeVisitor):
            def visit_List(self, node):
                if len(node.elts) > 100:
                    large_collections.append(("list", len(node.elts)))
                self.generic_visit(node)

            def visit_Dict(self, node):
                if len(node.keys) > 100:
                    large_collections.append(("dict", len(node.keys)))
                self.generic_visit(node)

            def visit_Set(self, node):
                if len(node.elts) > 100:
                    large_collections.append(("set", len(node.elts)))
                self.generic_visit(node)

        LargeCollectionVisitor().visit(tree)

        if large_collections:
            if "Memory Optimization" not in results:
                results["Memory Optimization"] = []

            for coll_type, size in large_collections:
                msg = f"{'Cell ' + str(cell_number) + ': ' if cell_number is not None else ''}Large {coll_type} literal with {size} elements found. Consider loading from file or generating programmatically."
                results["Memory Optimization"].append(msg)

        return results

    def _get_category_from_pattern(self, pattern_name: str) -> str:
        """Determine the category based on the pattern name."""
        if pattern_name in self.spark_patterns:
            return "Spark Optimization"
        elif pattern_name in self.tensorflow_patterns:
            return "TensorFlow Optimization"
        elif pattern_name in self.pandas_patterns:
            return "Pandas Optimization"
        else:
            return "General Optimization"


class NotebookConverter:
    """Converts Jupyter notebooks to Python files with optimization analysis."""

    def __init__(self, include_markdown: bool = True, include_output: bool = False):
        """
        Initialize the converter.

        Args:
            include_markdown: Whether to include markdown cells as comments
            include_output: Whether to include cell outputs as comments
        """
        self.include_markdown = include_markdown
        self.include_output = include_output
        self.analyzer = OptimizationAnalyzer()
        self.all_suggestions = {}

    def convert(self, notebook_path: str) -> Tuple[str, Dict]:
        """
        Convert a Jupyter notebook to a Python file with optimization analysis.

        Args:
            notebook_path: Path to the notebook file

        Returns:
            Tuple of (python_code, optimization_suggestions)
        """
        try:
            with open(notebook_path, 'r', encoding='utf-8') as f:
                notebook = json.load(f)
        except json.JSONDecodeError:
            raise ValueError(f"The file at {notebook_path} is not a valid JSON file.")
        except Exception as e:
            raise Exception(f"Error opening notebook: {str(e)}")

        # Check if it's a valid Jupyter notebook
        if 'cells' not in notebook or 'nbformat' not in notebook:
            raise ValueError(f"The file at {notebook_path} does not appear to be a valid Jupyter notebook.")

        python_code = []

        # Add a header
        python_code.append(f"#!/usr/bin/env python3")
        python_code.append(f"# -*- coding: utf-8 -*-")
        python_code.append(f"# Converted from {os.path.basename(notebook_path)}")
        python_code.append(f"# Conversion date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        python_code.append("")

        # Process each cell
        for i, cell in enumerate(notebook['cells']):
            cell_type = cell.get('cell_type', '')
            source = ''.join(cell.get('source', []))

            if cell_type == 'markdown' and self.include_markdown:
                # Format markdown as comments
                markdown_comment = self._format_markdown_as_comment(source)
                python_code.append(markdown_comment)
                python_code.append("")

            elif cell_type == 'code':
                # Check if the cell contains magics or shell commands
                if source.strip().startswith('%') or source.strip().startswith('!'):
                    python_code.append(f"# Cell {i + 1}: Contains magics or shell commands")
                    for line in source.split('\n'):
                        if line.strip():
                            python_code.append(f"# {line}")
                else:
                    # Add a cell marker
                    python_code.append(f"# Cell {i + 1}")

                    # Add the source code
                    python_code.append(source)

                    # Analyze the code for optimization opportunities
                    suggestions = self.analyzer.analyze_code(source, i + 1)
                    if suggestions:
                        self.all_suggestions[i + 1] = suggestions

                # Include cell outputs if requested
                if self.include_output and 'outputs' in cell and cell['outputs']:
                    python_code.append("\n# Cell output:")
                    for output in cell['outputs']:
                        if 'text' in output:
                            for line in ''.join(output['text']).split('\n'):
                                python_code.append(f"# {line}")
                        elif 'data' in output and 'text/plain' in output['data']:
                            for line in ''.join(output['data']['text/plain']).split('\n'):
                                python_code.append(f"# {line}")

                python_code.append("")

        return "\n".join(python_code), self.all_suggestions

    def _format_markdown_as_comment(self, markdown: str) -> str:
        """Format markdown text as Python comments."""
        result = []
        result.append("# " + "-" * 78)
        result.append("# MARKDOWN CELL")
        result.append("# " + "-" * 78)

        for line in markdown.split('\n'):
            if line.strip():
                result.append(f"# {line}")
            else:
                result.append("#")

        return "\n".join(result)

    def generate_html_report(self, notebook_path: str, suggestions: Dict) -> str:
        """
        Generate an HTML report of optimization suggestions.

        Args:
            notebook_path: Path to the notebook file
            suggestions: Dictionary of suggestions

        Returns:
            HTML string
        """
        html = []
        html.append("<!DOCTYPE html>")
        html.append("<html lang='en'>")
        html.append("<head>")
        html.append("    <meta charset='UTF-8'>")
        html.append("    <meta name='viewport' content='width=device-width, initial-scale=1.0'>")
        html.append(f"    <title>Optimization Report: {os.path.basename(notebook_path)}</title>")
        html.append("    <style>")
        html.append(
            "        body { font-family: Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; color: #333; max-width: 1200px; margin: 0 auto; }")
        html.append("        h1 { color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px; }")
        html.append("        h2 { color: #3498db; margin-top: 30px; }")
        html.append("        h3 { color: #2c3e50; margin-top: 20px; }")
        html.append(
            "        .summary { background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }")
        html.append("        .category { margin-bottom: 30px; }")
        html.append("        .suggestion { margin: 10px 0 10px 20px; }")
        html.append("        .cell-link { color: #3498db; text-decoration: none; font-weight: bold; }")
        html.append("        .cell-link:hover { text-decoration: underline; }")
        html.append("        .framework-summary { display: flex; justify-content: space-around; flex-wrap: wrap; }")
        html.append(
            "        .framework-box { background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 10px; min-width: 200px; text-align: center; }")
        html.append("        .framework-box h3 { margin-top: 0; }")
        html.append("        .high { color: #e74c3c; }")
        html.append("        .medium { color: #f39c12; }")
        html.append("        .low { color: #27ae60; }")
        html.append(
            "        .toggle-button { background-color: #3498db; color: white; border: none; padding: 8px 15px; border-radius: 4px; cursor: pointer; margin: 10px 0; }")
        html.append("        .toggle-button:hover { background-color: #2980b9; }")
        html.append("        .hidden { display: none; }")
        html.append("    </style>")
        html.append("</head>")
        html.append("<body>")

        # Header
        html.append(f"    <h1>Optimization Report: {os.path.basename(notebook_path)}</h1>")
        html.append(f"    <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")

        # Summary statistics
        total_suggestions = sum(
            len(suggestions[cell][category]) for cell in suggestions for category in suggestions[cell])
        category_counts = {}
        for cell in suggestions:
            for category in suggestions[cell]:
                if category not in category_counts:
                    category_counts[category] = 0
                category_counts[category] += len(suggestions[cell][category])

        html.append("    <div class='summary'>")
        html.append(f"        <h2>Summary</h2>")
        html.append(f"        <p>Total cells with optimization suggestions: {len(suggestions)}</p>")
        html.append(f"        <p>Total suggestions: {total_suggestions}</p>")

        html.append("        <div class='framework-summary'>")
        for category, count in category_counts.items():
            html.append(f"            <div class='framework-box'>")
            html.append(f"                <h3>{category}</h3>")
            html.append(f"                <p>{count} suggestions</p>")
            html.append("            </div>")
        html.append("        </div>")
        html.append("    </div>")

        # Toggle buttons for sections
        html.append("    <button id='toggle-all' class='toggle-button'>Expand/Collapse All</button>")
        html.append("    <button id='toggle-by-cell' class='toggle-button'>View By Cell</button>")
        html.append("    <button id='toggle-by-category' class='toggle-button'>View By Category</button>")

        # Suggestions by cell
        html.append("    <div id='by-cell-view'>")
        html.append("        <h2>Suggestions by Cell</h2>")

        for cell in sorted(suggestions.keys()):
            html.append(f"        <h3>Cell {cell}</h3>")

            for category in sorted(suggestions[cell].keys()):
                html.append(f"        <div class='category'>")
                html.append(f"            <h4>{category}</h4>")

                for suggestion in suggestions[cell][category]:
                    html.append(f"            <div class='suggestion'>{suggestion}</div>")

                html.append("        </div>")

        html.append("    </div>")

        # Suggestions by category
        html.append("    <div id='by-category-view' class='hidden'>")
        html.append("        <h2>Suggestions by Category</h2>")

        # Collect all suggestions by category
        by_category = {}
        for cell in suggestions:
            for category in suggestions[cell]:
                if category not in by_category:
                    by_category[category] = []
                by_category[category].extend(suggestions[cell][category])

        for category in sorted(by_category.keys()):
            html.append(f"        <div class='category'>")
            html.append(f"            <h3>{category}</h3>")

            for suggestion in by_category[category]:
                html.append(f"            <div class='suggestion'>{suggestion}</div>")

            html.append("        </div>")

        html.append("    </div>")

        # JavaScript for toggle functionality
        html.append("    <script>")
        html.append("        document.getElementById('toggle-all').addEventListener('click', function() {")
        html.append("            const sections = document.querySelectorAll('.category');")
        html.append("            sections.forEach(section => {")
        html.append("                section.classList.toggle('hidden');")
        html.append("            });")
        html.append("        });")
        html.append("        ")
        html.append("        document.getElementById('toggle-by-cell').addEventListener('click', function() {")
        html.append("            document.getElementById('by-cell-view').classList.remove('hidden');")
        html.append("            document.getElementById('by-category-view').classList.add('hidden');")
        html.append("        });")
        html.append("        ")
        html.append("        document.getElementById('toggle-by-category').addEventListener('click', function() {")
        html.append("            document.getElementById('by-cell-view').classList.add('hidden');")
        html.append("            document.getElementById('by-category-view').classList.remove('hidden');")
        html.append("        });")
        html.append("    </script>")

        html.append("</body>")
        html.append("</html>")

        return "\n".join(html)

    def generate_text_report(self, notebook_path: str, suggestions: Dict) -> str:
        """
        Generate a text report of optimization suggestions.

        Args:
            notebook_path: Path to the notebook file
            suggestions: Dictionary of suggestions

        Returns:
            Text string
        """
        text = []
        text.append("=" * 80)
        text.append(f"OPTIMIZATION REPORT: {os.path.basename(notebook_path)}")
        text.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        text.append("=" * 80)
        text.append("")

        # Summary statistics
        total_suggestions = sum(
            len(suggestions[cell][category]) for cell in suggestions for category in suggestions[cell])
        category_counts = {}
        for cell in suggestions:
            for category in suggestions[cell]:
                if category not in category_counts:
                    category_counts[category] = 0
                category_counts[category] += len(suggestions[cell][category])

        text.append("SUMMARY")
        text.append("-" * 80)
        text.append(f"Total cells with optimization suggestions: {len(suggestions)}")
        text.append(f"Total suggestions: {total_suggestions}")
        text.append("")

        for category, count in category_counts.items():
            text.append(f"{category}: {count} suggestions")

        text.append("")
        text.append("SUGGESTIONS BY CELL")
        text.append("-" * 80)

        for cell in sorted(suggestions.keys()):
            text.append(f"Cell {cell}")
            text.append("-" * 40)

            for category in sorted(suggestions[cell].keys()):
                text.append(f"{category}:")

                for suggestion in suggestions[cell][category]:
                    text.append(f"  - {suggestion}")

                text.append("")

        text.append("SUGGESTIONS BY CATEGORY")
        text.append("-" * 80)

        # Collect all suggestions by category
        by_category = {}
        for cell in suggestions:
            for category in suggestions[cell]:
                if category not in by_category:
                    by_category[category] = []
                by_category[category].extend(suggestions[cell][category])

        for category in sorted(by_category.keys()):
            text.append(f"{category}")
            text.append("-" * 40)

            for suggestion in by_category[category]:
                text.append(f"  - {suggestion}")

            text.append("")

        return "\n".join(text)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Convert Jupyter notebooks to Python with optimization analysis')
    parser.add_argument('notebook', help='Path to the Jupyter notebook file')
    parser.add_argument('-o', '--output', help='Output Python file path (default: same name with .py extension)')
    parser.add_argument('-r', '--report', choices=['html', 'text', 'both'], default='html',
                        help='Type of optimization report to generate (default: html)')
    parser.add_argument('--no-markdown', action='store_true', help='Do not include markdown cells as comments')
    parser.add_argument('--include-output', action='store_true', help='Include cell outputs as comments')

    args = parser.parse_args()

    # Determine output file path
    if args.output:
        output_path = args.output
    else:
        base_name = os.path.splitext(args.notebook)[0]
        output_path = f"{base_name}.py"

    # Determine report paths
    html_report_path = f"{os.path.splitext(args.notebook)[0]}_optimization_report.html"
    text_report_path = f"{os.path.splitext(args.notebook)[0]}_optimization_report.txt"

    # Convert the notebook
    converter = NotebookConverter(
        include_markdown=(not args.no_markdown),
        include_output=args.include_output
    )

    try:
        print(f"Converting {args.notebook} to {output_path}...")
        python_code, suggestions = converter.convert(args.notebook)

        # Save the Python file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(python_code)
        print(f"Conversion complete. Python file saved to {output_path}")

        # Generate and save reports
        if args.report in ['html', 'both']:
            html_report = converter.generate_html_report(args.notebook, suggestions)
            with open(html_report_path, 'w', encoding='utf-8') as f:
                f.write(html_report)
            print(f"HTML optimization report saved to {html_report_path}")

        if args.report in ['text', 'both']:
            text_report = converter.generate_text_report(args.notebook, suggestions)
            with open(text_report_path, 'w', encoding='utf-8') as f:
                f.write(text_report)
            print(f"Text optimization report saved to {text_report_path}")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()