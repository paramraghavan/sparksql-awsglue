#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jupyter Notebook to Python Converter with Optimization Analysis
Updated for improved performance, modularity, extensibility, and reporting.
"""

import json
import re
import os
import sys
import ast
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor


class OptimizationAnalyzer:
    """Analyzes code for potential optimizations across frameworks."""

    def __init__(self):
        self.patterns = self._compile_patterns()

    def _compile_patterns(self) -> Dict[str, Tuple[re.Pattern, str, str]]:
        categories = {
            "Spark Optimization": {
                "cache_or_persist": (r"\.cache\(\)|\.persist\(\)", "Consider optimizing caching strategy."),
                "collect": (r"\.collect\(\)", "Avoid collect() unless necessary."),
                "repartition": (r"\.repartition\(\d+\)", "Repartition may cause a full shuffle."),
            },
            "TensorFlow Optimization": {
                "eager_execution": (r"tf\.executing_eagerly\(\)", "Use tf.function for performance."),
            },
            "Pandas Optimization": {
                "iterrows": (r"\.iterrows\(\)", "iterrows() is slow; prefer vectorized ops."),
            },
            "General Optimization": {
                "nested_loops": (r"for .* in .*:\n\s*for .* in .*:", "Avoid nested loops; vectorize."),
            }
        }

        compiled = {}
        for category, items in categories.items():
            for key, (regex, message) in items.items():
                compiled[key] = (re.compile(regex), message, category)
        return compiled

    def analyze_code(self, code: str, cell_number: Optional[int] = None) -> Dict[str, List[str]]:
        results = {}
        for name, (pattern, suggestion, category) in self.patterns.items():
            matches = pattern.findall(code)
            if matches:
                results.setdefault(category, []).append(
                    f"Cell {cell_number}: {suggestion} (pattern: {name})" if cell_number else f"{suggestion} (pattern: {name})"
                )
        try:
            tree = ast.parse(code)
            ast_results = self._analyze_ast(tree, cell_number)
            for cat, messages in ast_results.items():
                results.setdefault(cat, []).extend(messages)
        except SyntaxError:
            pass
        return results

    def _analyze_ast(self, tree: ast.AST, cell_number: Optional[int] = None) -> Dict[str, List[str]]:
        results = {}

        class Visitor(ast.NodeVisitor):
            def __init__(self):
                self.messages = []

            def visit_List(self, node):
                if len(node.elts) > 100:
                    self.messages.append(("Memory Optimization", f"Cell {cell_number}: Large list literal with {len(node.elts)} elements."))
                self.generic_visit(node)

        visitor = Visitor()
        visitor.visit(tree)

        for cat, msg in visitor.messages:
            results.setdefault(cat, []).append(msg)
        return results


class NotebookConverter:
    """Converts notebooks to Python scripts with optimization suggestions."""

    def __init__(self, include_markdown=True):
        self.include_markdown = include_markdown
        self.analyzer = OptimizationAnalyzer()

    def convert(self, notebook_path: str) -> Tuple[str, Dict[int, Dict[str, List[str]]]]:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)

        python_code = [
            "#!/usr/bin/env python3",
            "# -*- coding: utf-8 -*-",
            f"# Converted from {os.path.basename(notebook_path)}",
            f"# Conversion date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        ]

        suggestions = {}

        def process_cell(i, cell):
            cell_code = []
            if cell['cell_type'] == 'markdown' and self.include_markdown:
                lines = cell['source']
                cell_code.append("# MARKDOWN CELL\n" + '\n'.join(f"# {line.strip()}" for line in lines))
            elif cell['cell_type'] == 'code':
                lines = ''.join(cell['source'])
                cell_code.append(f"# Cell {i+1}\n{lines}")
                sug = self.analyzer.analyze_code(lines, i+1)
                if sug:
                    suggestions[i+1] = sug
            return '\n'.join(cell_code)

        with ThreadPoolExecutor() as executor:
            cell_blocks = list(executor.map(lambda i: process_cell(i[0], i[1]), enumerate(notebook['cells'])))

        python_code.extend(cell_blocks)
        return '\n\n'.join(python_code), suggestions

    def generate_text_report(self, suggestions: Dict[int, Dict[str, List[str]]]) -> str:
        report = ["Optimization Report\n"]
        for cell in sorted(suggestions):
            report.append(f"Cell {cell}:")
            for category, messages in suggestions[cell].items():
                report.append(f"  {category}:")
                for msg in messages:
                    report.append(f"    - {msg}")
        return '\n'.join(report)

    def generate_html_report(self, suggestions: Dict[int, Dict[str, List[str]]]) -> str:
        html = ["<html><head><style>body{font-family:sans-serif}</style></head><body>", "<h1>Optimization Report</h1>"]
        for cell in sorted(suggestions):
            html.append(f"<h2>Cell {cell}</h2>")
            for category, messages in suggestions[cell].items():
                html.append(f"<h3>{category}</h3><ul>")
                for msg in messages:
                    html.append(f"<li>{msg}</li>")
                html.append("</ul>")
        html.append("</body></html>")
        return '\n'.join(html)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('notebook', help='Path to .ipynb file')
    parser.add_argument('-o', '--output', help='Output .py file')
    parser.add_argument('-r', '--report', choices=['html', 'text', 'both'], default='html')
    args = parser.parse_args()

    output_path = args.output or args.notebook.replace('.ipynb', '.py')
    base_path = os.path.splitext(args.notebook)[0]

    converter = NotebookConverter()
    code, suggestions = converter.convert(args.notebook)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(code)
    print(f"Python file saved to {output_path}")

    if args.report in ('text', 'both'):
        with open(base_path + '_report.txt', 'w') as f:
            f.write(converter.generate_text_report(suggestions))
        print("Text report saved.")

    if args.report in ('html', 'both'):
        with open(base_path + '_report.html', 'w') as f:
            f.write(converter.generate_html_report(suggestions))
        print("HTML report saved.")


if __name__ == "__main__":
    main()
