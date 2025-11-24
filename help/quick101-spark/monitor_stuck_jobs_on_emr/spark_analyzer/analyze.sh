#!/bin/bash

# Spark Job Analyzer - Quick Launch Script
# This script helps you quickly analyze Spark jobs

echo "╔════════════════════════════════════════════════════════════╗"
echo "║         🔍 SPARK JOB ANALYZER                             ║"
echo "║         Detect stuck jobs, data skew, and bottlenecks     ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Function to show usage
show_usage() {
    echo "Usage: $0 [MODE]"
    echo ""
    echo "Modes:"
    echo "  web     - Launch Flask web interface (recommended)"
    echo "  cli     - Run command-line analyzer"
    echo "  help    - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 web                              # Start web interface"
    echo "  $0 cli                              # Run CLI analyzer (will prompt for inputs)"
    echo ""
}

# Function to check dependencies
check_dependencies() {
    echo "Checking dependencies..."
    
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python 3 is not installed. Please install Python 3 first."
        exit 1
    fi
    
    # Check for required Python packages
    python3 -c "import requests" 2>/dev/null
    if [ $? -ne 0 ]; then
        echo "📦 Installing required Python packages..."
        pip3 install requests Flask
    fi
    
    echo "✅ All dependencies satisfied"
    echo ""
}

# Function to launch web interface
launch_web() {
    echo "🚀 Launching Flask Web Interface..."
    echo ""
    echo "The web interface will be available at: http://localhost:5000"
    echo "Press Ctrl+C to stop the server"
    echo ""
    echo "Opening in 3 seconds..."
    sleep 3
    
    # Try to open browser (works on Mac and some Linux distros)
    if command -v open &> /dev/null; then
        open http://localhost:5000 &
    elif command -v xdg-open &> /dev/null; then
        xdg-open http://localhost:5000 &
    fi
    
    python3 spark_analyzer_flask.py
}

# Function to run CLI
run_cli() {
    echo "🖥️  Command Line Analyzer"
    echo ""
    
    # Prompt for History Server URL
    read -p "Enter Spark History Server URL (e.g., http://emr-master:18080): " HISTORY_SERVER
    
    if [ -z "$HISTORY_SERVER" ]; then
        echo "❌ History Server URL is required"
        exit 1
    fi
    
    # Prompt for Application ID
    read -p "Enter Application ID (e.g., application_1234567890123_0001): " APP_ID
    
    if [ -z "$APP_ID" ]; then
        echo "❌ Application ID is required"
        exit 1
    fi
    
    # Ask if user wants to save output
    read -p "Save results to JSON file? (y/n): " SAVE_JSON
    
    echo ""
    echo "Analyzing job..."
    echo ""
    
    if [ "$SAVE_JSON" = "y" ] || [ "$SAVE_JSON" = "Y" ]; then
        OUTPUT_FILE="spark_analysis_$(date +%Y%m%d_%H%M%S).json"
        python3 spark_job_analyzer.py \
            --history-server "$HISTORY_SERVER" \
            --app-id "$APP_ID" \
            --output "$OUTPUT_FILE"
        
        if [ $? -eq 0 ]; then
            echo ""
            echo "✅ Analysis complete! Results saved to: $OUTPUT_FILE"
        fi
    else
        python3 spark_job_analyzer.py \
            --history-server "$HISTORY_SERVER" \
            --app-id "$APP_ID"
    fi
}

# Main script logic
main() {
    # Check dependencies first
    check_dependencies
    
    # Determine mode
    MODE="${1:-help}"
    
    case "$MODE" in
        web)
            launch_web
            ;;
        cli)
            run_cli
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            echo "❌ Invalid mode: $MODE"
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
