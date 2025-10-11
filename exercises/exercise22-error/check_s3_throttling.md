# Check S3 Rate Limiting - AWS Console & CLI

### Method 1: CloudWatch Metrics (Best Method)

#### Via AWS Console:

1. **Navigate to CloudWatch:**
    - AWS Console → CloudWatch → Metrics → All metrics
    - Select "S3" → "Storage Metrics" or "Request Metrics"

2. **Key Metrics to Check:**
    - `4xxErrors` - Shows throttling (503 errors)
    - `AllRequests` - Total request rate
    - `PutRequests` - Write request rate
    - `5xxErrors` - Server errors

3. **Create Custom Dashboard:**
    - Set time range to when your job ran (October 11, 22:06)
    - Look for spikes in `4xxErrors` during job execution

#### Via AWS CLI:

```bash
# 1. Check 4xx Errors (Rate Limiting)
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name 4xxErrors \
  --dimensions Name=BucketName,Value=aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --start-time 2025-10-11T22:00:00Z \
  --end-time 2025-10-11T23:00:00Z \
  --period 300 \
  --statistics Sum \
  --region us-east-1

# 2. Check Total Requests
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name AllRequests \
  --dimensions Name=BucketName,Value=aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --start-time 2025-10-11T22:00:00Z \
  --end-time 2025-10-11T23:00:00Z \
  --period 60 \
  --statistics Sum \
  --region us-east-1

# 3. Check PUT Requests specifically
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name PutRequests \
  --dimensions Name=BucketName,Value=aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --start-time 2025-10-11T22:00:00Z \
  --end-time 2025-10-11T23:00:00Z \
  --period 60 \
  --statistics Sum \
  --region us-east-1

# 4. Check 5xx Errors
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name 5xxErrors \
  --dimensions Name=BucketName,Value=aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --start-time 2025-10-11T22:00:00Z \
  --end-time 2025-10-11T23:00:00Z \
  --period 300 \
  --statistics Sum \
  --region us-east-1
```

### Method 2: Check Request Metrics by Prefix

```bash
# First, check if request metrics are enabled for your prefix
aws s3api list-bucket-metrics-configurations \
  --bucket aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --region us-east-1

# Enable request metrics for specific prefix (if not enabled)
aws s3api put-bucket-metrics-configuration \
  --bucket aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --id prepay-metrics \
  --metrics-configuration '{
    "Id": "prepay-metrics",
    "Filter": {
      "Prefix": "prepay/results/"
    }
  }' \
  --region us-east-1

# Then check metrics for specific prefix (available after ~15 min)
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name 4xxErrors \
  --dimensions Name=BucketName,Value=aws-icn31-s3-us-east-1-getdata-uattop-raw \
               Name=FilterId,Value=prepay-metrics \
  --start-time 2025-10-11T22:00:00Z \
  --end-time 2025-10-11T23:00:00Z \
  --period 60 \
  --statistics Sum \
  --region us-east-1
```

### Method 3: Enable and Check S3 Server Access Logs

```bash
# 1. Enable server access logging (if not already enabled)
aws s3api put-bucket-logging \
  --bucket aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --bucket-logging-status '{
    "LoggingEnabled": {
      "TargetBucket": "aws-icn31-s3-us-east-1-getdata-uattop-raw",
      "TargetPrefix": "logs/"
    }
  }' \
  --region us-east-1

# 2. Wait for logs (can take a few hours), then download and analyze
aws s3 cp s3://aws-icn31-s3-us-east-1-getdata-uattop-raw/logs/ ./s3-logs/ --recursive

# 3. Check for 503 errors (rate limiting) in logs
grep " 503 " ./s3-logs/*.log | wc -l

# 4. Count requests per minute to see if you exceeded limits
awk '{print $3}' ./s3-logs/*.log | cut -d: -f1-2 | sort | uniq -c | sort -rn | head -20
```

### Method 4: CloudTrail for Detailed API Analysis

```bash
# Check CloudTrail events for S3 API calls
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=ResourceName,AttributeValue=aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --start-time 2025-10-11T22:00:00Z \
  --end-time 2025-10-11T23:00:00Z \
  --max-results 50 \
  --region us-east-1 \
  --query 'Events[*].[EventTime,EventName,Resources[0].ResourceName]' \
  --output table

# Filter for errors
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=ResourceName,AttributeValue=aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --start-time 2025-10-11T22:00:00Z \
  --end-time 2025-10-11T23:00:00Z \
  --region us-east-1 \
  --query 'Events[?contains(CloudTrailEvent, `errorCode`)]' \
  --output json
```

### Method 5: Quick Python Script to Analyze Metrics

```python
import boto3
from datetime import datetime, timedelta
import json


def check_s3_rate_limiting(bucket_name, start_time, end_time, region='us-east-1'):
    """
    Check S3 rate limiting and concurrency issues
    """
    cloudwatch = boto3.client('cloudwatch', region_name=region)

    print(f"\n{'=' * 70}")
    print(f"S3 RATE LIMITING ANALYSIS")
    print(f"Bucket: {bucket_name}")
    print(f"Time Range: {start_time} to {end_time}")
    print(f"{'=' * 70}\n")

    metrics_to_check = [
        ('4xxErrors', 'Throttling/Client Errors'),
        ('5xxErrors', 'Server Errors'),
        ('AllRequests', 'Total Requests'),
        ('PutRequests', 'PUT Requests'),
        ('GetRequests', 'GET Requests'),
    ]

    for metric_name, description in metrics_to_check:
        print(f"\n📊 {description} ({metric_name}):")
        print("-" * 70)

        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/S3',
                MetricName=metric_name,
                Dimensions=[
                    {'Name': 'BucketName', 'Value': bucket_name}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,  # 1 minute intervals
                Statistics=['Sum', 'Average', 'Maximum']
            )

            datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])

            if not datapoints:
                print("   No data available")
                continue

            total_sum = sum(d.get('Sum', 0) for d in datapoints)
            max_value = max(d.get('Maximum', 0) for d in datapoints)

            print(f"   Total: {total_sum:,.0f}")
            print(f"   Peak: {max_value:,.0f} per minute")

            if metric_name == 'PutRequests' and max_value > 3000:
                print(f"   ⚠️  WARNING: Approaching S3 limit (3,500 PUT/sec per prefix)")

            if metric_name == '4xxErrors' and total_sum > 0:
                print(f"   🔴 CRITICAL: {total_sum:,.0f} throttling errors detected!")
                print(f"   → This confirms rate limiting is occurring")

            # Show top 5 peaks
            if len(datapoints) > 5:
                print(f"\n   Top 5 peak minutes:")
                top_peaks = sorted(datapoints, key=lambda x: x.get('Sum', 0), reverse=True)[:5]
                for dp in top_peaks:
                    timestamp = dp['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                    value = dp.get('Sum', 0)
                    print(f"      {timestamp}: {value:,.0f}")

        except Exception as e:
            print(f"   Error retrieving {metric_name}: {e}")

    # Calculate request rate
    print(f"\n{'=' * 70}")
    print("ANALYSIS:")
    print(f"{'=' * 70}")

    try:
        put_response = cloudwatch.get_metric_statistics(
            Namespace='AWS/S3',
            MetricName='PutRequests',
            Dimensions=[{'Name': 'BucketName', 'Value': bucket_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Sum']
        )

        error_response = cloudwatch.get_metric_statistics(
            Namespace='AWS/S3',
            MetricName='4xxErrors',
            Dimensions=[{'Name': 'BucketName', 'Value': bucket_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Sum']
        )

        total_puts = sum(d.get('Sum', 0) for d in put_response['Datapoints'])
        total_errors = sum(d.get('Sum', 0) for d in error_response['Datapoints'])

        if total_puts > 0:
            error_rate = (total_errors / total_puts) * 100
            print(f"\nTotal PUT requests: {total_puts:,.0f}")
            print(f"Total 4xx errors: {total_errors:,.0f}")
            print(f"Error rate: {error_rate:.2f}%")

            if error_rate > 5:
                print(f"\n🔴 HIGH ERROR RATE!")
                print(f"   Recommendations:")
                print(f"   1. Reduce concurrent writes: df.coalesce(30)")
                print(f"   2. Use partitionBy() to spread across prefixes")
                print(f"   3. Increase retry attempts in Spark config")
        else:
            print("\n⚠️  No PUT requests found in time range")
            print("   → Check if time range is correct")
            print("   → Metrics may have 15min delay")

    except Exception as e:
        print(f"Error in analysis: {e}")

    print(f"\n{'=' * 70}\n")


# Usage
bucket = "aws-icn31-s3-us-east-1-getdata-uattop-raw"
start = datetime(2025, 10, 11, 22, 0, 0)  # Your job start time
end = datetime(2025, 10, 11, 23, 0, 0)  # Your job end time

check_s3_rate_limiting(bucket, start, end, region='us-east-1')
```

### Method 6: Check Bucket Configuration

```bash
# 1. Check if bucket has any policies limiting requests
aws s3api get-bucket-policy \
  --bucket aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --region us-east-1

# 2. Check bucket versioning (can slow down writes)
aws s3api get-bucket-versioning \
  --bucket aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --region us-east-1

# 3. Check lifecycle policies (can interfere with writes)
aws s3api get-bucket-lifecycle-configuration \
  --bucket aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --region us-east-1

# 4. Check inventory configuration (can add load)
aws s3api list-bucket-inventory-configurations \
  --bucket aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --region us-east-1

# 5. Count objects in target prefix (too many can cause issues)
aws s3 ls s3://aws-icn31-s3-us-east-1-getdata-uattop-raw/prepay/results/20251011_220600/ \
  --recursive \
  --summarize | tail -2
```

### Interpreting Results

**Rate Limiting is Confirmed if:**

- `4xxErrors` > 100 during job execution
- `PutRequests` > 3,000 per second consistently
- Error rate > 5%
- Logs show 503 SlowDown errors

**Example Output Interpretation:**

```bash
# Good (No throttling)
4xxErrors: 0-10
PutRequests: 500-1000 per minute
Error rate: < 1%

# Throttling (Problem!)
4xxErrors: 500+
PutRequests: 5000+ per minute (exceeds 3,500/sec limit)
Error rate: > 10%
```

## Quick One-Liner Check

```bash
# Quick check for errors during your job time
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name 4xxErrors \
  --dimensions Name=BucketName,Value=aws-icn31-s3-us-east-1-getdata-uattop-raw \
  --start-time 2025-10-11T22:00:00Z \
  --end-time 2025-10-11T23:00:00Z \
  --period 300 \
  --statistics Sum \
  --region us-east-1 \
  --query 'Datapoints[*].[Timestamp,Sum]' \
  --output table

# If Sum > 0, you have throttling!
```
