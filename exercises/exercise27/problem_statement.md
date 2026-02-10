I have a dictionary of datasets with their corresponding s3 paths, each dataset may have diff columns to check for example dataset1 could be release_date,
dataset2 could be effective_date and so on. using pyspark i want to loop over each dataset and check for rows counts
where dataset1.release_date >= rundate. Truncate run date to first of every month
sahre pyspsark code