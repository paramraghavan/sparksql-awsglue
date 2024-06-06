How to access dag programatically using spark event log and parse into yaml using python
> not tested

## To access the Directed Acyclic Graph (DAG) programmatically using Spark event logs and parse it into YAML using Python, follow these steps:

### Step 1: Enable Spark Event Logging

First, ensure that your Spark application is configured to log events. Add the following properties to your `spark-defaults.conf` file or set them programmatically:

```properties
spark.eventLog.enabled true
spark.eventLog.dir file:///path/to/eventLogDir
```

### Step 2: Submit Your Spark Job

Run your Spark job normally. The event logs will be generated in the specified directory.

### Step 3: Parse Event Logs Using Python

Use Python to parse the event logs and extract the DAG information. Here's an example script to read the event log and convert it to YAML format.

#### Python Script:

```python
import json
import os
import yaml

# Directory where Spark event logs are stored
event_log_dir = "/path/to/eventLogDir"

def parse_event_log(file_path):
    with open(file_path, 'r') as file:
        events = []
        for line in file:
            events.append(json.loads(line.strip()))
    return events

def extract_dag(events):
    dag = {}
    for event in events:
        if event['Event'] == 'SparkListenerJobStart':
            job_id = event['Job ID']
            dag[job_id] = {
                'Stages': []
            }
            for stage_info in event['Stage Infos']:
                stage_id = stage_info['Stage ID']
                num_tasks = stage_info['Number of Tasks']
                dag[job_id]['Stages'].append({
                    'Stage ID': stage_id,
                    'Number of Tasks': num_tasks
                })
    return dag

def convert_to_yaml(data, output_file):
    with open(output_file, 'w') as file:
        yaml.dump(data, file, default_flow_style=False)

def main():
    all_events = []
    for filename in os.listdir(event_log_dir):
        file_path = os.path.join(event_log_dir, filename)
        all_events.extend(parse_event_log(file_path))
    
    dag = extract_dag(all_events)
    output_file = "dag.yaml"
    convert_to_yaml(dag, output_file)
    print(f"DAG information has been written to {output_file}")

if __name__ == "__main__":
    main()
```

### Explanation

1. **Enable Event Logging**: Ensure event logging is enabled in your Spark configuration.
2. **Submit Your Spark Job**: Run your Spark job to generate event logs.
3. **Parse Event Logs**:
    - The `parse_event_log` function reads the event log file and loads each JSON event.
    - The `extract_dag` function extracts DAG information from the parsed events, focusing on job start events (`SparkListenerJobStart`).
4. **Convert to YAML**: The `convert_to_yaml` function writes the extracted DAG information to a YAML file.
5. **Main Function**: The `main` function processes all event log files in the specified directory, extracts the DAG information, and converts it to YAML format.

This script will generate a `dag.yaml` file containing the DAG information from your Spark event logs. Adjust the paths and any specific parsing logic as necessary for your environment and Spark version.