from pyspark.sql import SparkSession
import glob   # For reading all files in a directory
import ntpath # For reading file name from path
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

def convert_datetime(x):
    """Convert SAS date into DateType."""
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())


# Function to restructure the dataframe
def restructure_i94(df):
    """Restructure the dataframe for i94_immigration dataset."""
    # Used for converting matflag to boolean
    @udf("boolean")
    def isMatch(x):
        return (True if x == 'M' else False)
    
# Used for Unifying null and 0 values to be 9 instead for i94mode 
    @udf("integer")
    def isModeNull(x):
        return (9 if (x == None) or (x == 0) else x)
    
    # Drop not needed columns - Noticed some new columns in one of the months, [validres,delete_days,delete_mexl,delete_dup,delete_visa,delete_recdup]
    cols_to_drop = ['count','dtadfile','visapost','occup','entdepa','entdepd','entdepu','insnum','validres','delete_days','delete_mexl','delete_dup','delete_visa','delete_recdup']
    df = df.drop(*cols_to_drop)
    # Alter some columns types:
    # cicid type to long
    df = df.withColumn("cicid", df["cicid"].cast(LongType()))
    # i94yr, i94mon, i94cit, i94res, arrdate, i94mode, depdate, i94bir, i94visa to int 
    df = df.withColumn("i94yr", df["i94yr"].cast(IntegerType()))
    df = df.withColumn("i94mon", df["i94mon"].cast(IntegerType()))
    df = df.withColumn("i94cit", df["i94cit"].cast(IntegerType()))
    df = df.withColumn("i94res", df["i94res"].cast(IntegerType()))
    
    df = df.withColumn("arrdate", udf_datetime_from_sas("arrdate"))
    
    df = df.withColumn("i94mode", df["i94mode"].cast(IntegerType()))
    # Use 8 instead of null for missing values
    df = df.withColumn("i94mode", isModeNull(df["i94mode"]))
    
    df = df.withColumn("depdate", udf_datetime_from_sas("depdate"))
    
    df = df.withColumn("i94bir", df["i94bir"].cast(IntegerType()))
    df = df.withColumn("i94bir", df["i94bir"].cast(IntegerType()))
    df = df.withColumn("i94visa", df["i94visa"].cast(IntegerType()))
    # matflag has 2 values, (null, 'M'), to boolean
    df = df.withColumn("match_flag",isMatch(df["matflag"]))
    df = df.drop("matflag")
    df = df.withColumnRenamed("match_flag","matflag")
    # biryear to int
    df = df.withColumn("biryear", df["biryear"].cast(IntegerType()))                                                
    # admnum type to long
    df = df.withColumn("admnum", df["admnum"].cast(LongType()))
    return df

def convert_sas_to_parquet(spark):
    """Convert SAS files to Parquet files and store them localy."""
    i94SASFiles = glob.glob("../../../data/18-83510-I94-Data-2016/*") # Gets all SAS files
    for i94SASFile in i94SASFiles:
        fileName = ntpath.basename(i94SASFile).split(".")[0]
        df_spark = spark.read.format('com.github.saurfang.sas.spark').load(i94SASFile)
        df_spark = restructure_i94(df_spark)
        df_spark.coalesce(1).write.parquet("../i94_immigration/"+fileName)
        
def wrangle_i94_immigration(spark):
    """Wrangle i94_immigration data into the required parquet files."""
    convert_sas_to_parquet(spark)
    
def read_code_country_data(spark):
    """Read code_country dataset."""
    code_country_schema = StructType([
        StructField('code', IntegerType(), True),
        StructField('country', StringType(), True)
    ])
    code_country_df = spark.read.option("quote","\"").csv("../lookups/code_country.csv",schema = code_country_schema,header = True)
    return code_country_df
 
    
def wrangle_world_temp(spark):
    """Wrangle world_temp data into the required parquet files."""
    worldTemp_schema = StructType([
        StructField('dt', DateType(), True),
        StructField('AverageTemperature', DoubleType(), True),
        StructField('AverageTemperatureUncertainty', DoubleType(), True),
        StructField('City', StringType(), True),
        StructField('Country', StringType(), True),
        StructField('Latitude', StringType(), True),
        StructField('Longitude', StringType(), True)
    ])
    worldTempDF = spark.read.csv("../../../data2/",schema = worldTemp_schema,header = True)
    worldTempDF = worldTempDF.drop("AverageTemperatureUncertainty","Latitude","Longitude")
    worldAverageTempDF = worldTempDF.where(worldTempDF.AverageTemperature.isNotNull()).groupBy("City","Country").agg(avg("AverageTemperature").alias("AvgTemp"))
    
    code_country_df = read_code_country_data(spark)
    worldAverageTempDF = worldAverageTempDF.join(code_country_df,lower(worldAverageTempDF.Country) == lower(code_country_df.country), 'left')\
                            .select(worldAverageTempDF.City,\
                                    worldAverageTempDF.Country,\
                                    worldAverageTempDF.AvgTemp,\
                                    code_country_df.code.alias("Country_Code"))
    
    worldAverageTempDF.coalesce(1).write.parquet("../world_temp/")

def wrangle_us_cities_demographics(spark):
    """Wrangle us_cities_demographics data into the required parquet files."""
    usCitiesDemographics_schema = StructType([
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('median_age', DoubleType(), True),
        StructField('male_population', IntegerType(), True),
        StructField('female_population', IntegerType(), True),
        StructField('total_population', IntegerType(), True),
        StructField('number_of_veterans', IntegerType(), True),
        StructField('foreign_born', IntegerType(), True),
        StructField('average_household_size', DoubleType(), True),
        StructField('state_code', StringType(), True),
        StructField('race', StringType(), True),
        StructField('count', IntegerType(), True)
    ])
    
    usCitiesDemographicsDF = spark.read.csv("../us-cities-demographics.csv",schema = usCitiesDemographics_schema,header = True,sep = ";")
    
    cols_to_drop = ['number_of_veterans', 'average_household_size', 'race', 'count']
    usCitiesDemographicsDF = usCitiesDemographicsDF.drop(*cols_to_drop)
    # This will result in duplicated fields, because the data is partitioned by race, so I will remove duplicates by city and state
    usCitiesDemographicsDF = usCitiesDemographicsDF.dropDuplicates()
    
    usCitiesDemographicsDF.coalesce(1).write.parquet("../us_cities_demographics/")
    
    
def main():
    """
    - Initiate a Spark session
    - Wrangle data for i94_immigration, world_temp and us_cities_demographics
    """
    spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").getOrCreate()
    
    wrangle_i94_immigration(spark)
    wrangle_world_temp(spark)
    wrangle_us_cities_demographics(spark)

    print("- All done")


if __name__ == "__main__":
    main()