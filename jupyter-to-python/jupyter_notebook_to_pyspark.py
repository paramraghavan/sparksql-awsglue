#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Jupyter Shell Command to Boto3/Python Converter
==============================================

This module converts Jupyter notebook shell commands (! prefix) to Python boto3
calls or subprocess calls.


Run the converter script:
jupyter_shell_converter.py your_notebook.ipynb --output converted_notebook.py --format python

Options:
--format notebook: Creates a new Jupyter notebook with converted code (default)
--format python: Creates a standalone Python module

"""

import re
import os
import argparse
import json
import logging
from typing import Dict, List, Any, Union, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_cells_from_notebook(notebook_path: str) -> List[Dict[str, Any]]:
    """
    Extract all cells from a Jupyter notebook.

    Args:
        notebook_path: Path to the notebook file

    Returns:
        List of cell dictionaries
    """
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)

    return notebook.get('cells', [])


def identify_shell_commands(cell_source: List[str]) -> List[Tuple[int, str]]:
    """
    Identify lines that start with '!' in a cell.

    Args:
        cell_source: Source lines from a Jupyter cell

    Returns:
        List of tuples (line_number, command) for shell commands
    """
    shell_commands = []

    for i, line in enumerate(cell_source):
        # Check if line starts with ! and is not inside a comment
        if line.strip().startswith('!') and not line.strip().startswith('#'):
            # Extract the actual command without the '!'
            command = line.strip()[1:].strip()
            shell_commands.append((i, command))

    return shell_commands


def convert_aws_cli_to_boto3(command: str) -> str:
    """
    Convert AWS CLI commands to boto3 Python code.

    Args:
        command: AWS CLI command string

    Returns:
        Python boto3 equivalent code
    """
    # Split the command into parts
    parts = command.split()

    # Check if it's an AWS command
    if len(parts) >= 1 and parts[0] == 'aws':
        service = parts[1] if len(parts) > 1 else ""
        operation = parts[2] if len(parts) > 2 else ""

        # S3 operations
        if service == 's3':
            if operation == 'ls':
                # aws s3 ls [s3://bucket[/prefix]]
                if len(parts) > 3:
                    bucket_path = parts[3]
                    # Handle s3:// prefix
                    if bucket_path.startswith('s3://'):
                        bucket_path = bucket_path[5:]

                    # Split bucket and prefix
                    if '/' in bucket_path:
                        bucket, prefix = bucket_path.split('/', 1)
                        return f"""import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# List objects with prefix
response = s3.list_objects_v2(
    Bucket='{bucket}',
    Prefix='{prefix}'
)

# Print objects
for obj in response.get('Contents', []):
    print(f"{{obj['LastModified']}}\\t{{obj['Size']}}\\t{{obj['Key']}}")
"""
                    else:
                        # Just bucket, no prefix
                        return f"""import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# List objects in bucket
response = s3.list_objects_v2(
    Bucket='{bucket_path}'
)

# Print objects
for obj in response.get('Contents', []):
    print(f"{{obj['LastModified']}}\\t{{obj['Size']}}\\t{{obj['Key']}}")
"""
                else:
                    # List buckets
                    return """import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# List buckets
response = s3.list_buckets()

# Print buckets
for bucket in response['Buckets']:
    print(f"{bucket['CreationDate']}\\t{bucket['Name']}")
"""

            elif operation == 'cp':
                # aws s3 cp source destination
                if len(parts) >= 5:
                    source = parts[3]
                    destination = parts[4]

                    # S3 to local
                    if source.startswith('s3://') and not destination.startswith('s3://'):
                        bucket, key = source[5:].split('/', 1)
                        return f"""import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# Download file
s3.download_file(
    Bucket='{bucket}',
    Key='{key}',
    Filename='{destination}'
)
print(f"Downloaded s3://{bucket}/{key} to {destination}")
"""
                    # Local to S3
                    elif not source.startswith('s3://') and destination.startswith('s3://'):
                        bucket, key = destination[5:].split('/', 1)
                        return f"""import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# Upload file
s3.upload_file(
    Filename='{source}',
    Bucket='{bucket}',
    Key='{key}'
)
print(f"Uploaded {source} to s3://{bucket}/{key}")
"""
                    # S3 to S3
                    elif source.startswith('s3://') and destination.startswith('s3://'):
                        src_bucket, src_key = source[5:].split('/', 1)
                        dst_bucket, dst_key = destination[5:].split('/', 1)
                        return f"""import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# Copy object
s3.copy_object(
    CopySource={{'Bucket': '{src_bucket}', 'Key': '{src_key}'}},
    Bucket='{dst_bucket}',
    Key='{dst_key}'
)
print(f"Copied s3://{src_bucket}/{src_key} to s3://{dst_bucket}/{dst_key}")
"""

            elif operation == 'rm':
                # aws s3 rm s3://bucket/key
                if len(parts) > 3 and parts[3].startswith('s3://'):
                    path = parts[3][5:]  # Remove s3:// prefix
                    bucket, key = path.split('/', 1)
                    return f"""import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# Delete object
s3.delete_object(
    Bucket='{bucket}',
    Key='{key}'
)
print(f"Deleted s3://{bucket}/{key}")
"""

        # EC2 operations
        elif service == 'ec2':
            if operation == 'describe-instances':
                return """import boto3

# Initialize EC2 client
ec2 = boto3.client('ec2')

# Describe instances
response = ec2.describe_instances()

# Print instance details
for reservation in response['Reservations']:
    for instance in reservation['Instances']:
        print(f"Instance ID: {instance['InstanceId']}")
        print(f"Instance Type: {instance['InstanceType']}")
        print(f"State: {instance['State']['Name']}")
        print(f"Public IP: {instance.get('PublicIpAddress', 'N/A')}")
        print(f"Private IP: {instance.get('PrivateIpAddress', 'N/A')}")
        print("-" * 50)
"""

        # Dynamodb operations
        elif service == 'dynamodb':
            if operation == 'list-tables':
                return """import boto3

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# List tables
response = dynamodb.list_tables()

# Print table names
for table_name in response['TableNames']:
    print(table_name)
"""

    # For unrecognized commands, recommend using subprocess
    return convert_to_subprocess(command)


def convert_gsutil_to_gcs(command: str) -> str:
    """
    Convert gsutil commands to Google Cloud Storage Python code.

    Args:
        command: gsutil command string

    Returns:
        Python GCS client equivalent code
    """
    parts = command.split()

    if len(parts) >= 1 and parts[0] == 'gsutil':
        operation = parts[1] if len(parts) > 1 else ""

        if operation == 'ls':
            # gsutil ls [gs://bucket[/prefix]]
            if len(parts) > 2:
                bucket_path = parts[2]
                # Handle gs:// prefix
                if bucket_path.startswith('gs://'):
                    bucket_path = bucket_path[5:]

                # Split bucket and prefix
                if '/' in bucket_path:
                    bucket, prefix = bucket_path.split('/', 1)
                    return f"""from google.cloud import storage

# Initialize storage client
storage_client = storage.Client()

# Get bucket
bucket = storage_client.bucket('{bucket}')

# List blobs with prefix
blobs = bucket.list_blobs(prefix='{prefix}')

# Print blobs
for blob in blobs:
    print(blob.name)
"""
                else:
                    # Just bucket, no prefix
                    return f"""from google.cloud import storage

# Initialize storage client
storage_client = storage.Client()

# Get bucket
bucket = storage_client.bucket('{bucket_path}')

# List blobs
blobs = bucket.list_blobs()

# Print blobs
for blob in blobs:
    print(blob.name)
"""
            else:
                # List buckets
                return """from google.cloud import storage

# Initialize storage client
storage_client = storage.Client()

# List buckets
buckets = storage_client.list_buckets()

# Print buckets
for bucket in buckets:
    print(bucket.name)
"""

        elif operation == 'cp':
            # gsutil cp source destination
            if len(parts) >= 4:
                source = parts[2]
                destination = parts[3]

                # GCS to local
                if source.startswith('gs://') and not destination.startswith('gs://'):
                    bucket_path = source[5:]
                    bucket, blob_name = bucket_path.split('/', 1)
                    return f"""from google.cloud import storage

# Initialize storage client
storage_client = storage.Client()

# Get bucket and blob
bucket = storage_client.bucket('{bucket}')
blob = bucket.blob('{blob_name}')

# Download file
blob.download_to_filename('{destination}')
print(f"Downloaded gs://{bucket.name}/{blob_name} to {destination}")
"""
                # Local to GCS
                elif not source.startswith('gs://') and destination.startswith('gs://'):
                    bucket_path = destination[5:]
                    bucket, blob_name = bucket_path.split('/', 1)
                    return f"""from google.cloud import storage

# Initialize storage client
storage_client = storage.Client()

# Get bucket and create blob
bucket = storage_client.bucket('{bucket}')
blob = bucket.blob('{blob_name}')

# Upload file
blob.upload_from_filename('{source}')
print(f"Uploaded {source} to gs://{bucket.name}/{blob_name}")
"""

    # For unrecognized commands, recommend using subprocess
    return convert_to_subprocess(command)


def convert_to_subprocess(command: str) -> str:
    """
    Convert a shell command to Python subprocess call.

    Args:
        command: Shell command to convert

    Returns:
        Python subprocess code
    """
    # Simple shell command conversion using subprocess.run
    return f"""import subprocess

# Execute command
result = subprocess.run(
    {command.split()},
    capture_output=True,
    text=True,
    check=False  # Set to True to raise an exception if the command fails
)

# Print output
if result.stdout:
    print(result.stdout)

# Print errors
if result.stderr:
    print(f"Error: {result.stderr}")

# Check return code
print(f"Return code: {result.returncode}")
"""


def convert_shell_to_python(cell_content: List[str]) -> List[str]:
    """
    Convert shell commands in a cell to Python code.

    Args:
        cell_content: List of source lines from a cell

    Returns:
        List of modified source lines
    """
    # Make a copy of the cell content
    modified_content = cell_content.copy()

    # Find shell commands
    shell_commands = identify_shell_commands(cell_content)

    # Process commands in reverse order to avoid line number issues
    for line_num, command in reversed(shell_commands):
        # Determine command type and convert
        python_code = ""

        if command.startswith('aws '):
            python_code = convert_aws_cli_to_boto3(command)
        elif command.startswith('gsutil '):
            python_code = convert_gsutil_to_gcs(command)
        else:
            python_code = convert_to_subprocess(command)

        # Replace the shell command with Python code
        python_lines = python_code.strip().split('\n')

        # Add comment to indicate the original command
        python_lines.insert(0, f"# Original shell command: {command}")
        python_lines.insert(1, "")

        # Replace the line containing the shell command
        modified_content[line_num] = python_lines[0]

        # Insert the rest of the Python code
        for i, py_line in enumerate(python_lines[1:], start=1):
            modified_content.insert(line_num + i, py_line)

    return modified_content


def process_notebook(notebook_path: str, output_path: str) -> None:
    """
    Process a notebook, converting shell commands to Python.

    Args:
        notebook_path: Path to the input notebook
        output_path: Path for the output notebook
    """
    # Extract cells from the notebook
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read notebook: {e}")
        raise

    # Process each cell
    cells = notebook.get('cells', [])
    shell_command_count = 0

    for cell in cells:
        if cell['cell_type'] == 'code':
            # Convert shell commands in the cell
            original_source = cell['source']
            modified_source = convert_shell_to_python(original_source)

            # Update cell source if changes were made
            if original_source != modified_source:
                cell['source'] = modified_source
                shell_command_count += 1

    # Write the modified notebook
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(notebook, f, indent=2)
        logger.info(f"Successfully converted {shell_command_count} cells with shell commands")
        logger.info(f"Output written to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write output notebook: {e}")
        raise


def create_python_module(notebook_path: str, output_path: str) -> None:
    """
    Create a standalone Python module from a notebook with shell commands converted.

    Args:
        notebook_path: Path to the input notebook
        output_path: Path for the output Python module
    """
    # Extract cells from the notebook
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read notebook: {e}")
        raise

    # Open output file
    with open(output_path, 'w', encoding='utf-8') as out_file:
        # Write module header
        module_name = os.path.splitext(os.path.basename(output_path))[0]
        header = f'''#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
{module_name}
{'=' * len(module_name)}

Python module generated from Jupyter notebook with shell commands converted to Python.

"""

import os
import logging
import subprocess
import boto3
from typing import Dict, List, Any, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

'''
        out_file.write(header + "\n\n")

        # Process each cell
        cells = notebook.get('cells', [])
        for cell in cells:
            if cell['cell_type'] == 'code':
                # Convert shell commands in the cell
                modified_source = convert_shell_to_python(cell['source'])

                # Write cell content to output file with a separator comment
                out_file.write("#" + "-" * 78 + "\n")
                out_file.write("# Cell content\n")
                out_file.write("#" + "-" * 78 + "\n\n")

                for line in modified_source:
                    out_file.write(line + "\n")

                out_file.write("\n\n")

        # Add main function
        out_file.write("if __name__ == '__main__':\n")
        out_file.write("    # Add main execution code here\n")
        out_file.write("    pass\n")

    logger.info(f"Successfully created Python module at {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Jupyter notebook shell commands to Python")
    parser.add_argument('notebook', help='Path to the input Jupyter notebook')
    parser.add_argument('--output', '-o', help='Output file path (notebook or .py file)')
    parser.add_argument('--format', '-f', choices=['notebook', 'python'], default='notebook',
                        help='Output format: notebook (default) or python module')

    args = parser.parse_args()

    # Determine output path
    if not args.output:
        base_name = os.path.splitext(args.notebook)[0]
        if args.format == 'notebook':
            args.output = f"{base_name}_converted.ipynb"
        else:
            args.output = f"{base_name}.py"

    # Process the notebook
    if args.format == 'notebook':
        process_notebook(args.notebook, args.output)
    else:
        create_python_module(args.notebook, args.output)