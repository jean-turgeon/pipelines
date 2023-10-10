# pipelines

Python ETL pipeline demos. First serie of pipeline are based on Pandas and tries out different ways to optimize the pipeline.



## Requirements

Needed:

- fastparquet
- numpy
- pandas
- pyarrow
- typing

Optional:

- parquet-tools


## Pipelines

  1. Basic pipeline with pandas & lambda / apply for transformation
  2. Using numpy vectorize for transformation
  3. Open and read file faster:
     - Separate decompressing from reading
     - Optimize reading by selecting only the columns needed.
  4. Open and read file faster using pyarrow











