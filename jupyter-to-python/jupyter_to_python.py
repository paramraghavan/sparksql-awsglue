#!/usr/bin/env python3
"""
Enhanced Jupyter Notebook to Python Code Converter

This script converts a Jupyter Notebook into a standalone Python script,
handling various notebook features including magic commands, markdown comments,
and specialized environment setups.

Usage:
    python jupyter_to_python.py input_notebook.ipynb output_script.py [options]

Options:
    --keep-markdown     Convert markdown cells to comments in the output script
    --keep-output       Add code output as comments in the output script
    --execute-magics    Try to convert some magic commands to equivalent Python code
    --include-header    Add a detailed header with notebook metadata
"""

import json
import sys
import re
import argparse
from pathlib import Path
import datetime


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Convert Jupyter Notebook to Python script')
    parser.add_argument('input_notebook', help='Input Jupyter notebook (.ipynb file)')
    parser.add_argument('output_script', help='Output Python script (.py file)')
    parser.add_argument('--keep-markdown', action='store_true', help='Convert markdown cells to comments')
    parser.add_argument('--keep-output', action='store_true', help='Add cell outputs as comments')
    parser.add_argument('--execute-magics', action='store_true',
                        help='Convert magic commands to Python code when possible')
    parser.add_argument('--include-header', action='store_true', help='Add detailed header with notebook metadata')

    return parser.parse_args()


def convert_markdown_to_comments(markdown_text):
    """Convert markdown text to Python comments."""
    lines = markdown_text.split('\n')
    commented_lines = []

    commented_lines.append('# ' + '-' * 70)
    commented_lines.append('# MARKDOWN CELL')
    commented_lines.append('# ' + '-' * 70)

    for line in lines:
        if line.strip():
            commented_lines.append('# ' + line)
        else:
            commented_lines.append('#')

    commented_lines.append('# ' + '-' * 70)
    return '\n'.join(commented_lines)


def convert_output_to_comments(outputs):
    """Convert cell outputs to Python comments."""
    if not outputs:
        return ""

    result = []
    result.append('\n# ' + '-' * 70)
    result.append('# OUTPUT')
    result.append('# ' + '-' * 70)

    for output in outputs:
        if 'text/plain' in output.get('data', {}):
            output_text = output['data']['text/plain']
            if isinstance(output_text, list):
                output_text = ''.join(output_text)

            for line in output_text.split('\n'):
                result.append(f'# {line}')

        elif 'text' in output:
            text = output['text']
            if isinstance(text, list):
                text = ''.join(text)

            for line in text.split('\n'):
                result.append(f'# {line}')

    result.append('# ' + '-' * 70)
    return '\n'.join(result)


def convert_magic_to_python(line):
    """Convert Jupyter magic commands to equivalent Python code when possible."""
    line = line.strip()

    # Handle common magic commands
    if line.startswith('%matplotlib inline'):
        return "import matplotlib.pyplot as plt"

    elif line.startswith('%time '):
        code = line[6:].strip()
        return f"""
import time
_start_time = time.time()
{code}
print(f"CPU times: {{time.time() - _start_time:.2f}} seconds")
""".strip()

    elif line.startswith('%pip install '):
        packages = line[12:].strip()
        return f"""
import subprocess
import sys
print(f"Installing packages: {packages}")
subprocess.check_call([sys.executable, "-m", "pip", "install", {', '.join([f'"{p.strip()}"' for p in packages.split()])}])
""".strip()

    elif line.startswith('!'):
        cmd = line[1:].strip()
        return f"""
import subprocess
subprocess.run('''{cmd}''', shell=True)
""".strip()

    # If no conversion is possible, return a commented line
    return f"# Magic command (not converted): {line}"


def process_code_cell(cell, args):
    """Process a code cell and return Python code."""
    source = cell.get('source', [])
    if isinstance(source, list):
        source = ''.join(source)

    lines = source.split('\n')
    processed_lines = []

    for line in lines:
        if line.strip().startswith('%') or line.strip().startswith('!'):
            if args.execute_magics:
                processed_lines.append(convert_magic_to_python(line))
            else:
                processed_lines.append(f"# {line}")
        else:
            processed_lines.append(line)

    result = '\n'.join(processed_lines)

    # Add cell outputs as comments if requested
    if args.keep_output and 'outputs' in cell:
        result += convert_output_to_comments(cell['outputs'])

    return result


def extract_notebook_metadata(notebook):
    """Extract useful metadata from the notebook."""
    metadata = notebook.get('metadata', {})
    result = {}

    # Extract kernel info
    if 'kernelspec' in metadata:
        result['kernel_name'] = metadata['kernelspec'].get('name', 'unknown')
        result['kernel_display_name'] = metadata['kernelspec'].get('display_name', 'Unknown')

    # Extract language info
    if 'language_info' in metadata:
        result['language'] = metadata['language_info'].get('name', 'python')
        result['language_version'] = metadata['language_info'].get('version', 'unknown')

    # Extract author info if available
    if 'author' in metadata:
        result['author'] = metadata['author']

    return result


def create_script_header(notebook_path, metadata):
    """Create a detailed header for the Python script."""
    header = [
        "#!/usr/bin/env python3",
        "\"\"\"",
        f"Python script generated from notebook: {Path(notebook_path).name}",
        f"Conversion date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ""
    ]

    if metadata:
        header.append("Notebook metadata:")
        for key, value in metadata.items():
            header.append(f"- {key}: {value}")

    header.extend([
        "\"\"\"",
        "",
        "# Standard library imports",
        "import os",
        "import sys",
        "import json",
        "import datetime",
        "",
        "# Third-party imports",
        "# (Imports will be preserved from the notebook cells)",
        "",
        ""
    ])

    return '\n'.join(header)


def detect_environment_setup(cells):
    """Detect common patterns for environment setup in notebook cells."""
    # Check for PySpark
    has_pyspark = False
    has_spark_session = False

    # Check for common ML libraries
    has_tensorflow = False
    has_pytorch = False
    has_sklearn = False

    for cell in cells:
        if cell.get('cell_type') != 'code':
            continue

        source = cell.get('source', [])
        if isinstance(source, list):
            source = ''.join(source)

        if 'import pyspark' in source or 'from pyspark' in source:
            has_pyspark = True
            if 'SparkSession' in source and ('.builder' in source or '.getOrCreate()' in source):
                has_spark_session = True

        if 'import tensorflow' in source or 'from tensorflow' in source:
            has_tensorflow = True

        if 'import torch' in source or 'from torch' in source:
            has_pytorch = True

        if 'import sklearn' in source or 'from sklearn' in source:
            has_sklearn = True

    return {
        'pyspark': has_pyspark,
        'spark_session': has_spark_session,
        'tensorflow': has_tensorflow,
        'pytorch': has_pytorch,
        'sklearn': has_sklearn
    }


def add_environment_boilerplate(environment_info):
    """Add appropriate boilerplate code based on detected environment."""
    boilerplate = []

    if environment_info['pyspark'] and not environment_info['spark_session']:
        boilerplate.extend([
            "# PySpark setup detected - adding SparkSession initialization",
            "from pyspark.sql import SparkSession",
            "",
            "# Initialize Spark session",
            "spark = SparkSession.builder \\",
            "    .appName(\"Converted Notebook\") \\",
            "    .getOrCreate()",
            ""
        ])

    if environment_info['tensorflow']:
        boilerplate.extend([
            "# TensorFlow environment setup",
            "import tensorflow as tf",
            "",
            "# Check TensorFlow version and GPU availability",
            "print(f\"TensorFlow version: {tf.__version__}\")",
            "print(f\"GPU available: {len(tf.config.list_physical_devices('GPU')) > 0}\")",
            ""
        ])

    if environment_info['pytorch']:
        boilerplate.extend([
            "# PyTorch environment setup",
            "import torch",
            "",
            "# Check PyTorch version and GPU availability",
            "print(f\"PyTorch version: {torch.__version__}\")",
            "print(f\"GPU available: {torch.cuda.is_available()}\")",
            "device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')",
            "print(f\"Using device: {device}\")",
            ""
        ])

    return '\n'.join(boilerplate)


def add_main_block(environment_info):
    """Add a main block at the end of the script."""
    main_block = [
        "",
        "if __name__ == \"__main__\":",
        "    # Main execution starts here",
        "    print(\"Executing converted notebook script\")"
    ]

    if environment_info['pyspark']:
        main_block.extend([
            "    ",
            "    # Don't forget to stop the Spark session when done",
            "    if 'spark' in locals() or 'spark' in globals():",
            "        print(\"Stopping Spark session...\")",
            "        spark.stop()",
            "        print(\"Spark session stopped\")"
        ])

    return '\n'.join(main_block)


def convert_notebook_to_script(notebook_path, output_path, args):
    """Convert a Jupyter notebook to a Python script with advanced options."""
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: {notebook_path} is not a valid JSON file.")
        return False
    except FileNotFoundError:
        print(f"Error: File {notebook_path} not found.")
        return False

    # Validate that this is a Jupyter notebook
    if 'cells' not in notebook:
        print(f"Error: {notebook_path} doesn't appear to be a Jupyter notebook.")
        return False

    cells = notebook.get('cells', [])

    # Detect environment setup
    environment_info = detect_environment_setup(cells)

    # Extract metadata if header is requested
    metadata = {}
    if args.include_header:
        metadata = extract_notebook_metadata(notebook)

    # Prepare the output script
    script_lines = []

    # Add header if requested
    if args.include_header:
        script_lines.append(create_script_header(notebook_path, metadata))
    else:
        script_lines.extend([
            "#!/usr/bin/env python3",
            f"# Python script generated from {Path(notebook_path).name}",
            ""
        ])

    # Add environment-specific boilerplate
    boilerplate = add_environment_boilerplate(environment_info)
    if boilerplate:
        script_lines.append(boilerplate)

    # Process all cells
    for cell in cells:
        cell_type = cell.get('cell_type')

        if cell_type == 'code':
            script_lines.append(process_code_cell(cell, args))
            script_lines.append('')  # Add blank line after each cell

        elif cell_type == 'markdown' and args.keep_markdown:
            source = cell.get('source', [])
            if isinstance(source, list):
                source = ''.join(source)
            script_lines.append(convert_markdown_to_comments(source))
            script_lines.append('')  # Add blank line after each cell

    # Add a main block at the end
    script_lines.append(add_main_block(environment_info))

    # Write the output script
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(script_lines))

    print(f"Successfully converted {notebook_path} to {output_path}")
    return True


"""
Handles Multiple Cell Types:

Converts code cells to Python code
Optionally converts markdown cells to comments
Preserves cell outputs as comments if requested


Magic Command Handling:

Can either comment out magic commands (like %matplotlib inline)
Or convert common magic commands to equivalent Python code


Environment Detection:

Identifies common libraries/frameworks (PySpark, TensorFlow, PyTorch)
Adds appropriate initialization code


Command-line Options:
python jupyter_to_python.py notebook.ipynb script.py [options]

--keep-markdown: Convert markdown cells to comments
--keep-output: Add cell outputs as comments
--execute-magics: Convert magic commands to Python when possible
--include-header: Add detailed metadata header


Metadata Handling:
Extracts notebook metadata (kernel info, language version)
Creates descriptive headers

Usage:
python jupyter_to_python.py your_notebook.ipynb output_script.py --keep-markdown --execute-magics
python jupyter_to_python.py --help
"""


if __name__ == "__main__":
    args = parse_arguments()

    if convert_notebook_to_script(args.input_notebook, args.output_script, args):
        print("Conversion completed successfully!")
        sys.exit(0)
    else:
        print("Conversion failed.")
        sys.exit(1)