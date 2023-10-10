from datetime import datetime
import gzip
from logging import basicConfig, DEBUG, getLogger, Logger
import numpy as np
from pandas import DataFrame
from pyarrow import csv
import pyarrow
from pyarrow.csv import ConvertOptions, ParseOptions, ReadOptions
import traceback


# Data files: input and output
INPUT_FILENAME:str = "./data/data.csv.tar.gz"
OUTPUT_FILENAME:str = "./output/processed.parquet"


COLUMN_NAMES:list = ["DR_NO","Date Rptd","DATE OCC","TIME OCC","AREA","AREA NAME","Rpt Dist No","Part 1-2","Crm Cd","Crm Cd Desc","Mocodes","Vict Age","Vict Sex","Vict Descent","Premis Cd","Premis Desc","Weapon Used Cd","Weapon Desc","Status","Status Desc","Crm Cd 1","Crm Cd 2","Crm Cd 3","Crm Cd 4","LOCATION","Cross Street","LAT","LON"]
COLUMNS_TO_READ:list = ["DATE OCC","TIME OCC","AREA NAME","Vict Age","Vict Sex","Vict Descent"]
COLUMNS_TO_RENAME:dict = {
    "DATE OCC": "date",
    "TIME OCC": "time",
    "AREA NAME": "area",
    "Vict Age": "age",
    "Vict Sex": "sex",
    "Vict Descent": "descent",
}






# Initialize logging
LOGGING_FORMAT:str = "%(asctime)s %(message)s"
basicConfig(format=LOGGING_FORMAT)

logger:Logger = getLogger(__name__)
logger.setLevel(DEBUG)

def skip_bad_row(row):
    return 'skip'


def read_data() -> DataFrame:
    """
    """

    start_time:datetime = datetime.utcnow()

    file:gzip.GzipFile = gzip.open(INPUT_FILENAME, "r")

    decompression_time:datetime = datetime.utcnow()
    reading_sec:float = (decompression_time - start_time).total_seconds()
    logger.debug(f"Decompression time: {reading_sec} sec.")

    table:pyarrow.Table = csv.read_csv(
        file,
        read_options=ReadOptions(
            column_names=COLUMN_NAMES,
            use_threads=True,
            skip_rows=1
        ),
        parse_options=ParseOptions(
            delimiter=",",
            invalid_row_handler=skip_bad_row,
        ),
        convert_options=ConvertOptions(
            include_columns=COLUMNS_TO_READ,
        )
    )
    data:DataFrame = table.to_pandas()

    reading_time:datetime = datetime.utcnow()
    reading_sec:float = (reading_time - decompression_time).total_seconds()
    logger.debug(f"Reading into Pandas time: {reading_sec} sec.")

    return data





def write_data(data: DataFrame) -> None:
    """
    """
    data.to_parquet(OUTPUT_FILENAME)



def calculation(date:str) -> float:
    """
    Make some funky calculation!
    """
    return (datetime.utcnow() - datetime.strptime(date, "%d/%m/%Y %H:%M:%S %p")).seconds * 123456789.545




def etl_transformation(data:DataFrame) -> DataFrame:
    """
    ETL Transformation explanation here.

    Parameters:
        data (DataFrame): Input data

     Returns:
        (DataFrame): Transformed data
    """


    data.rename(columns={
        "DATE OCC": "date",
        "TIME OCC": "time",
        "AREA NAME": "area",
        "Vict Age": "age",
        "Vict Sex": "sex",
        "Vict Descent": "descent",
        },
        inplace=True
    )


    # selecting rows based on condition
    output:DataFrame = data[data["date"] < "01/01/2022 12:00:00 AM"]
    output = output[output["area"].str.startswith("P")]

    #after than lambda apply in pandas
    vect_calculation = np.vectorize(calculation)

    output["date_math"] = vect_calculation(output["date"])


    # select columns
    output = output[["date", "time", "area", "age", "sex", "descent","date_math",]]


    logger.debug("Transformed output data:")
    print(output.head(n=10))

    return output


def handler() -> None:
    """
    Pipeline handler function
    """

    start_time:datetime = datetime.utcnow()

    logger.info("Pipeline 4 - Start")

    try:
        # 1. Reading the data file
        data:DataFrame = read_data()

        reading_time:datetime = datetime.utcnow()
        reading_sec:float = (reading_time - start_time).total_seconds()
        logger.debug(f"Reading time: {reading_sec} sec.")

        number_of_rows = len(data.index)
        logger.debug(f"Input has {number_of_rows} rows and {len(data.columns)} columns.")

        logger.debug("Original input data:")
        print(f"Data has: {len(data.columns)}")
        print(data.head(n=10))


        # 2. Executing the transformation
        results:DataFrame = etl_transformation(data)

        transformation_time:datetime = datetime.utcnow()
        transformation_sec:float = (transformation_time - reading_time).total_seconds()
        logger.debug(f"Transformation time: {transformation_sec} sec.")

        number_of_rows = len(results.index)
        logger.debug(f"Output has {number_of_rows} rows and {len(results.columns)} columns.")

        # 3. Writing to output file
        write_data(results)

        writing_sec:float = (datetime.utcnow() - transformation_time).total_seconds()
        logger.debug(f"Writing time: {writing_sec} sec.")

    except BaseException as ex:
        logger.error(f"Something bad happened: {ex}")
        traceback.print_exc()

    runtime:float = (datetime.utcnow() - start_time).total_seconds()
    logger.debug(f"Total processing time: {runtime} sec.")

    logger.info("Pipeline 4 - Done")








if __name__ == "__main__":

    handler()


