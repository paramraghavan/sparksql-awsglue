import boto3
import sys
import time
import logging
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('emr_spark_job_wrapper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


class EMRSparkJobRunner:
    def __init__(self, cluster_id, region='us-east-1'):
        """
        Initialize EMR Spark Job Runner

        Args:
            cluster_id (str): EMR cluster ID (e.g., 'j-XXXXXXXXXX')
            region (str): AWS region
        """
        self.cluster_id = cluster_id
        self.region = region
        self.emr_client = boto3.client('emr', region_name=region)

    def submit_spark_step(self, script_path, step_name="Spark Job"):
        """
        Submit a Spark job as an EMR step

        Args:
            script_path (str): Path to the Python script (S3 path or local path)
            step_name (str): Name for the EMR step

        Returns:
            str: Step ID if successful, None if failed
        """
        # Convert your spark-submit arguments to EMR step format
        step_args = [
            'spark-submit',
            '--conf', 'spark.dynamicAllocation.enabled=false',
            '--num-executors', '20',
            '--executor-cores', '4',
            '--executor-memory', '8g',
            script_path
        ]

        step_config = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',  # Options: TERMINATE_CLUSTER, CANCEL_AND_WAIT, CONTINUE
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': step_args
            }
        }

        try:
            logging.info(f"Submitting step to EMR cluster {self.cluster_id}")
            logging.info(f"Step arguments: {' '.join(step_args)}")

            response = self.emr_client.add_job_flow_steps(
                JobFlowId=self.cluster_id,
                Steps=[step_config]
            )

            step_id = response['StepIds'][0]
            logging.info(f"Step submitted successfully. Step ID: {step_id}")
            return step_id

        except ClientError as e:
            logging.error(f"AWS error submitting step: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error submitting step: {e}")
            return None

    def wait_for_step_completion(self, step_id, poll_interval=30, timeout=3600):
        """
        Wait for EMR step to complete and monitor progress

        Args:
            step_id (str): EMR step ID
            poll_interval (int): Seconds between status checks
            timeout (int): Maximum seconds to wait

        Returns:
            bool: True if successful, False if failed
        """
        start_time = datetime.now()
        logging.info(f"Monitoring step {step_id} - started at {start_time}")

        while True:
            try:
                # Check if timeout exceeded
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > timeout:
                    logging.error(f"Step monitoring timed out after {timeout} seconds")
                    return False

                # Get step status
                response = self.emr_client.describe_step(
                    ClusterId=self.cluster_id,
                    StepId=step_id
                )

                step = response['Step']
                state = step['Status']['State']

                logging.info(f"Step {step_id} status: {state}")

                # Check terminal states
                if state == 'COMPLETED':
                    end_time = datetime.now()
                    duration = end_time - start_time
                    logging.info(f"Step completed successfully in {duration}")
                    return True

                elif state in ['CANCELLED', 'FAILED', 'INTERRUPTED']:
                    failure_reason = step['Status'].get('FailureDetails', {}).get('Reason', 'Unknown')
                    logging.error(f"Step failed with state: {state}, reason: {failure_reason}")
                    return False

                elif state in ['PENDING', 'RUNNING']:
                    # Continue monitoring
                    time.sleep(poll_interval)

                else:
                    logging.warning(f"Unknown step state: {state}")
                    time.sleep(poll_interval)

            except ClientError as e:
                logging.error(f"AWS error checking step status: {e}")
                return False
            except Exception as e:
                logging.error(f"Unexpected error checking step status: {e}")
                return False

    def get_step_logs(self, step_id):
        """
        Get step execution details and log locations

        Args:
            step_id (str): EMR step ID
        """
        try:
            response = self.emr_client.describe_step(
                ClusterId=self.cluster_id,
                StepId=step_id
            )

            step = response['Step']

            # Log basic step info
            logging.info(f"Step Name: {step['Name']}")
            logging.info(f"Step State: {step['Status']['State']}")

            # Get creation and completion times
            if 'CreationDateTime' in step['Status']:
                logging.info(f"Created: {step['Status']['CreationDateTime']}")
            if 'EndDateTime' in step['Status']:
                logging.info(f"Ended: {step['Status']['EndDateTime']}")

            # Log S3 log locations (if available)
            cluster_response = self.emr_client.describe_cluster(ClusterId=self.cluster_id)
            log_uri = cluster_response['Cluster'].get('LogUri')

            if log_uri:
                step_log_path = f"{log_uri}/{self.cluster_id}/steps/{step_id}/"
                logging.info(f"Step logs available at: {step_log_path}")
                logging.info(f"  - stdout: {step_log_path}stdout")
                logging.info(f"  - stderr: {step_log_path}stderr")

        except Exception as e:
            logging.error(f"Error retrieving step logs: {e}")


def run_spark_job(cluster_id, script_path, region='us-east-1'):
    """
    Main function to run Spark job via EMR steps

    Args:
        cluster_id (str): EMR cluster ID
        script_path (str): Path to Python script
        region (str): AWS region

    Returns:
        bool: True if successful, False if failed
    """
    runner = EMRSparkJobRunner(cluster_id, region)

    # Submit the step
    step_id = runner.submit_spark_step(script_path, "My Spark Job")

    if not step_id:
        logging.error("Failed to submit step")
        return False

    # Wait for completion
    success = runner.wait_for_step_completion(step_id)

    # Get step logs info
    runner.get_step_logs(step_id)

    return success


if __name__ == "__main__":
    # Configuration - Update these values
    CLUSTER_ID = "j-XXXXXXXXXX"  # Replace with your EMR cluster ID
    SCRIPT_PATH = "s3://your-bucket/myjob.py"  # Replace with your S3 script path
    REGION = "us-east-1"

    # You can also pass these as command line arguments
    if len(sys.argv) >= 2:
        CLUSTER_ID = sys.argv[1]
    if len(sys.argv) >= 3:
        SCRIPT_PATH = sys.argv[2]
    if len(sys.argv) >= 4:
        REGION = sys.argv[3]

    logging.info(f"Starting EMR Spark job on cluster {CLUSTER_ID}")
    logging.info(f"Script: {SCRIPT_PATH}")

    success = run_spark_job(CLUSTER_ID, SCRIPT_PATH, REGION)

    if success:
        logging.info("Job completed successfully!")
    else:
        logging.error("Job failed!")

    sys.exit(0 if success else 1)