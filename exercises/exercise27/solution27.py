from pyspark.sql import functions as F
from datetime import datetime

# 1. Your configuration
# s3_paths = {"dataset1": "s3://bucket/path/one", "dataset2": "s3://bucket/path/two"}
dataset_configs = {
    "dataset1": "release_date",
    "dataset2": "effective_date"
}


def get_monthly_counts_from_s3(s3_paths, config_dict, reference_date=None):
    # Set up the reference date
    if reference_date is None:
        reference_date = datetime.now()

    ref_date_str = reference_date.strftime('%Y-%m-%d')
    results = {}

    for ds_name, date_col in config_dict.items():
        if ds_name in s3_paths:
            path = s3_paths[ds_name]
            print(f"Processing {ds_name} from {path}...")

            try:
                # 2. Read the dataset (assuming Parquet, change to .csv if needed)
                df = spark.read.parquet(path)

                # 3. Filter and Count
                # F.trunc(..., 'MM') sets the date to the 1st of that month
                count = df.filter(
                    F.col(date_col) >= F.trunc(F.lit(ref_date_str), "MM")
                ).count()

                results[ds_name] = count
                print(f"Result -> {ds_name}: {count} rows")

                # Clean up memory/cache
                df.unpersist()

            except Exception as e:
                print(f"Error processing {ds_name}: {e}")

    return results


# Run the process
final_counts = get_monthly_counts_from_s3(s3_paths, dataset_configs)