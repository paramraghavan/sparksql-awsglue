#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Jupyter to PySpark Converter
============================

This module contains utilities to convert Jupyter notebook code snippets into
well-structured, efficient PySpark modules.

"""

import os
import argparse
import logging
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class SparkConfig:
    """Configuration parameters for Spark session."""
    app_name: str
    master: str = "local[*]"
    log_level: str = "WARN"
    additional_configs: Dict[str, str] = None


class SparkSessionManager:
    """Manages Spark Session lifecycle with proper configuration."""

    def __init__(self, config: SparkConfig):
        """Initialize with SparkConfig."""
        self.config = config
        self._spark = None

    def __enter__(self):
        """Context manager entry point - creates and configures Spark session."""
        from pyspark.sql import SparkSession

        # Start building the session
        builder = SparkSession.builder.appName(self.config.app_name) \
            .master(self.config.master)

        # Add any additional configurations
        if self.config.additional_configs:
            for key, value in self.config.additional_configs.items():
                builder = builder.config(key, value)

        # Create the session
        self._spark = builder.getOrCreate()

        # Set log level
        self._spark.sparkContext.setLogLevel(self.config.log_level)

        logger.info(f"Created Spark session: {self.config.app_name}")
        return self._spark

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - properly stops SparkSession."""
        if self._spark:
            self._spark.stop()
            logger.info(f"Stopped Spark session: {self.config.app_name}")


def optimize_dataframe_operations(code: str) -> str:
    """
    Replace inefficient DataFrame operations with optimized versions.

    Args:
        code: PySpark code string to optimize

    Returns:
        Optimized PySpark code
    """
    # Common replacements for better performance
    replacements = {
        # Replace collect() with more efficient alternatives where appropriate
        r'\.collect\(\)': '# Consider: .take(N) or .limit(N).toPandas() instead of .collect()',

        # Replace multiple filter operations with a single one
        r'\.filter\(.*\)\.filter\(': '.filter(...) # OPTIMIZE: Combine multiple filters with & operator - ',

        # Replace slow UDF operations when standard functions can be used
        r'udf\(lambda': '# Consider: Use built-in Spark functions instead of UDFs when possible\nudf(lambda',

        # Flag potential cross joins which can be extremely expensive
        r'\.crossJoin\(': '# WARNING: Cross joins are expensive. Consider if a different join type is appropriate\n.crossJoin('
    }

    # Apply the replacements
    optimized_code = code
    for pattern, replacement in replacements.items():
        import re
        optimized_code = re.sub(pattern, replacement, optimized_code)

    return optimized_code


def structure_as_module(notebook_cells: List[str],
                        module_name: str,
                        author: str = "Anonymous") -> str:
    """
    Converts notebook cells into a well-structured Python module.

    Args:
        notebook_cells: List of code cells from the notebook
        module_name: Name for the module
        author: Author of the code

    Returns:
        Structured Python module as a string
    """
    # Module header with documentation
    header = f'''#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
{module_name}
{'=' * len(module_name)}

PySpark module generated from Jupyter notebook code.

Author: {author}
Date: {import datetime; datetime.datetime.now().strftime('%Y-%m-%d')}
"""

import os
import logging
from typing import Dict, List, Any, Optional, Union

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

'''

    # Extract imports, functions, and main code
    imports = []
    functions = []
    main_code = []

    for cell in notebook_cells:
        # Skip empty cells
        if not cell.strip():
            continue

        # Handle imports (this is simplified - would need more parsing in reality)
        if cell.startswith('import ') or cell.startswith('from '):
            imports.append(cell)
        # Handle function definitions
        elif cell.startswith('def '):
            functions.append(cell)
        # Everything else is main code
        else:
            main_code.append(cell)

    # Assemble module with proper structure
    module_parts = [
        header,
        '\n# Additional imports\n' + '\n'.join(imports),
        '\n\n# Function definitions\n' + '\n\n'.join(functions),
        '\n\n# Wrap main code in a function\ndef main():\n    """Main execution function."""',
    ]

    # Indent main code to fit inside main function
    indented_main = '\n'.join(['    ' + line for line in '\n'.join(main_code).split('\n')])
    module_parts.append(indented_main)

    # Add CLI entry point
    module_parts.append('''

if __name__ == "__main__":
    # Configure argument parser
    parser = argparse.ArgumentParser(description=f"{module_name} - PySpark processing")
    parser.add_argument('--input', type=str, help='Input data path')
    parser.add_argument('--output', type=str, help='Output data path')
    args = parser.parse_args()

    # Execute main function
    main()
''')

    return '\n'.join(module_parts)


def extract_cells_from_notebook(notebook_path: str) -> List[str]:
    """
    Extracts code cells from a Jupyter notebook file.

    Args:
        notebook_path: Path to the notebook file

    Returns:
        List of code strings from the notebook cells
    """
    import json

    with open(notebook_path, 'r', encoding='utf-8') as file:
        notebook = json.load(file)

    code_cells = []
    for cell in notebook['cells']:
        if cell['cell_type'] == 'code':
            # Join the lines of code in the cell
            code = ''.join(cell['source'])
            if code.strip():  # Skip empty cells
                code_cells.append(code)

    return code_cells


def convert_notebook_to_pyspark(notebook_path: str,
                                output_path: str,
                                module_name: str = None,
                                author: str = "Anonymous") -> None:
    """
    Main function to convert a Jupyter notebook to a well-structured PySpark module.

    Args:
        notebook_path: Path to the input notebook
        output_path: Path for the output Python module
        module_name: Name for the module (defaults to notebook filename)
        author: Author name to include in the module header
    """
    if not module_name:
        module_name = os.path.splitext(os.path.basename(notebook_path))[0]

    # Extract code cells from the notebook
    try:
        code_cells = extract_cells_from_notebook(notebook_path)
        logger.info(f"Extracted {len(code_cells)} code cells from {notebook_path}")
    except Exception as e:
        logger.error(f"Failed to extract cells from notebook: {e}")
        raise

    # Optimize the code for PySpark
    optimized_cells = [optimize_dataframe_operations(cell) for cell in code_cells]

    # Structure the code as a well-organized module
    module_code = structure_as_module(optimized_cells, module_name, author)

    # Write the module to the output path
    try:
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(module_code)
        logger.info(f"Successfully wrote PySpark module to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write output module: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Jupyter notebooks to PySpark modules")
    parser.add_argument('notebook', help='Path to the input Jupyter notebook')
    parser.add_argument('--output', '-o', help='Output Python file path')
    parser.add_argument('--name', '-n', help='Module name (defaults to notebook filename)')
    parser.add_argument('--author', '-a', default="Anonymous", help='Author name')

    args = parser.parse_args()

    # If output path is not specified, use the notebook name with .py extension
    if not args.output:
        args.output = os.path.splitext(args.notebook)[0] + '.py'

    convert_notebook_to_pyspark(args.notebook, args.output, args.name, args.author)