#!/bin/bash
# Example wrapper script for email integration
# Save this as: /home/hadoop/monitor_and_email.sh

YARN_URL="http://localhost:8088"
EMAIL="your-email@example.com"
THRESHOLD=85

# Run the monitor
OUTPUT=$(python3 /home/hadoop/monitor_cluster_health.py \
    --yarn-url "$YARN_URL" \
    --memory-threshold $THRESHOLD \
    --vcpu-threshold $THRESHOLD \
    --quiet 2>&1)

EXIT_CODE=$?

# If alert (exit code 1), send email
if [ $EXIT_CODE -eq 1 ]; then
    # Option 1: Using mail command
    echo "$OUTPUT" | mail -s "EMR Cluster Alert" "$EMAIL"
    
    # Option 2: Using AWS SES (if configured)
    # aws ses send-email \
    #   --from noreply@example.com \
    #   --to "$EMAIL" \
    #   --subject "EMR Cluster Alert" \
    #   --text "$OUTPUT"
    
    # Option 3: Using sendmail
    # echo -e "Subject: EMR Cluster Alert\n\n$OUTPUT" | sendmail "$EMAIL"
    
    echo "Alert sent to $EMAIL"
fi

exit $EXIT_CODE
