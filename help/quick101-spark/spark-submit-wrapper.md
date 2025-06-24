# Spark Submit Wrapper

```text
spark-submit \
  --conf spark.dynamicAllocation.enabled=false \
  --num-executors 20 \
  --executor-cores 4 \
  --executor-memory 8g \
  myjob.py

The myjob.py is  jupyter notebook file converted to python code. the modellers have written a lot of unstuctured code without proper error handling. It starts from line 1 and runs line by line till end of line 
I run this myjob.py using spark-submit command above.

What i want to do is to be able to wrap the python code - myjob.py  and catch exception/errors and handle these appropirately. This spark job is a long running process table 2-3 hours to run to completion

Will wrapping this spark-submit line using subprocess help or what is the best way.

We do not want to modify the Spark Job Code Directly
```

You have several good options to wrap the spark-submit command and handle exceptions appropriately. Here are the best
approaches:

## Option 1: Subprocess wrapper with comprehensive error handling (Recommended)

Create a wrapper script that uses subprocess to run spark-submit:

```python
import subprocess
import sys
import time
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spark_job_wrapper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


def run_spark_job():
    spark_cmd = [
        'spark-submit',
        '--conf', 'spark.dynamicAllocation.enabled=false',
        '--num-executors', '20',
        '--executor-cores', '4',
        '--executor-memory', '8g',
        'myjob.py'
    ]

    start_time = datetime.now()
    logging.info(f"Starting Spark job at {start_time}")
    logging.info(f"Command: {' '.join(spark_cmd)}")

    try:
        # Run the process and capture output
        process = subprocess.Popen(
            spark_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )

        # Stream output in real-time
        for line in process.stdout:
            print(line.rstrip())
            logging.info(line.rstrip())

        # Wait for completion
        return_code = process.wait()
        end_time = datetime.now()
        duration = end_time - start_time

        if return_code == 0:
            logging.info(f"Spark job completed successfully in {duration}")
            return True
        else:
            logging.error(f"Spark job failed with return code {return_code} after {duration}")
            return False

    except subprocess.TimeoutExpired:
        logging.error("Spark job timed out")
        process.kill()
        return False
    except Exception as e:
        logging.error(f"Unexpected error running Spark job: {str(e)}")
        return False


if __name__ == "__main__":
    success = run_spark_job()
    sys.exit(0 if success else 1)
```

## Option 2: Enhanced wrapper with retry logic and notifications

```python
import subprocess
import sys
import time
import logging
import smtplib
from email.mime.text import MIMEText
from datetime import datetime


class SparkJobWrapper:
    def __init__(self, max_retries=2, timeout_hours=4):
        self.max_retries = max_retries
        self.timeout_seconds = timeout_hours * 3600
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'spark_job_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )

    def send_notification(self, subject, message):
        # Implement your notification logic here
        # Could be email, Slack, Teams, etc.
        logging.info(f"NOTIFICATION: {subject} - {message}")

    def run_spark_job(self):
        spark_cmd = [
            'spark-submit',
            '--conf', 'spark.dynamicAllocation.enabled=false',
            '--num-executors', '20',
            '--executor-cores', '4',
            '--executor-memory', '8g',
            'myjob.py'
        ]

        for attempt in range(self.max_retries + 1):
            try:
                start_time = datetime.now()
                logging.info(f"Attempt {attempt + 1}/{self.max_retries + 1} - Starting Spark job at {start_time}")

                process = subprocess.Popen(
                    spark_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True
                )

                # Monitor output with timeout
                output_lines = []
                try:
                    stdout, _ = process.communicate(timeout=self.timeout_seconds)
                    output_lines = stdout.split('\n')

                    for line in output_lines:
                        if line.strip():
                            print(line)
                            logging.info(line)

                except subprocess.TimeoutExpired:
                    process.kill()
                    logging.error(f"Job timed out after {self.timeout_seconds / 3600} hours")
                    if attempt < self.max_retries:
                        logging.info("Will retry...")
                        continue
                    else:
                        self.send_notification("Spark Job Failed", "Job timed out after all retries")
                        return False

                end_time = datetime.now()
                duration = end_time - start_time

                if process.returncode == 0:
                    logging.info(f"Spark job completed successfully in {duration}")
                    self.send_notification("Spark Job Success", f"Job completed in {duration}")
                    return True
                else:
                    logging.error(f"Job failed with return code {process.returncode}")
                    if attempt < self.max_retries:
                        logging.info(f"Retrying in 60 seconds...")
                        time.sleep(60)
                    else:
                        self.send_notification("Spark Job Failed", f"Job failed after all retries")
                        return False

            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")
                if attempt < self.max_retries:
                    time.sleep(60)
                else:
                    self.send_notification("Spark Job Error", f"Unexpected error: {str(e)}")
                    return False

        return False


if __name__ == "__main__":
    wrapper = SparkJobWrapper(max_retries=2, timeout_hours=4)
    success = wrapper.run_spark_job()
    sys.exit(0 if success else 1)
```

## Option 3: Simple bash wrapper (if you prefer shell scripting)

```bash
#!/bin/bash

LOG_FILE="spark_job_$(date +%Y%m%d_%H%M%S).log"
START_TIME=$(date)

echo "Starting Spark job at $START_TIME" | tee -a $LOG_FILE

spark-submit \
  --conf spark.dynamicAllocation.enabled=false \
  --num-executors 20 \
  --executor-cores 4 \
  --executor-memory 8g \
  myjob.py 2>&1 | tee -a $LOG_FILE

EXIT_CODE=${PIPESTATUS[0]}
END_TIME=$(date)

if [ $EXIT_CODE -eq 0 ]; then
    echo "Spark job completed successfully at $END_TIME" | tee -a $LOG_FILE
    # Send success notification here
else
    echo "Spark job failed with exit code $EXIT_CODE at $END_TIME" | tee -a $LOG_FILE
    # Send failure notification here
fi

exit $EXIT_CODE
```

## Key Benefits of these approaches:

1. **No modification to myjob.py** - Your requirement is met
2. **Comprehensive logging** - All output is captured and logged
3. **Error handling** - Proper exception handling and return codes
4. **Monitoring** - Real-time output streaming and timeout handling
5. **Notifications** - Can easily add email/Slack notifications
6. **Retry logic** - Option 2 includes retry mechanism
7. **Exit codes** - Proper exit codes for downstream orchestration

## Recommendation:

Use **Option 1** for simplicity, or **Option 2** if you need retry logic and notifications. The subprocess approach is
definitely the right way to go - it gives you full control over the process while keeping your Spark job code untouched.