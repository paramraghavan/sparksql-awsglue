#!/bin/bash
# Script to package project dependencies for EMR

# Ensure we're in the project root directory
cd "$(dirname "$0")/.."

# Clean any previous zip files
rm -f src.zip

# Create zip of the src directory
echo "Creating src.zip package..."
zip -r src.zip src/

# Upload to S3
echo "Uploading dependencies to S3..."
aws s3 cp src.zip s3://my-bucket/dependencies/src.zip

echo "Dependencies packaged and uploaded successfully."