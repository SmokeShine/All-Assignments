import pandas as pd
import numpy as np
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from itertools import chain
import os
import tqdm
from pprint import pprint


def create_spark_session():
    '''
    Spark Session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang/spark-sas7bdat:2.1.0-s_2.11") \
        .getOrCreate()
    return spark

def sasfile_to_parquet(spark,filepath,dataset,ExclusionList,InclusionList):
    '''
    Function to read SAS Dataset
    '''
    df=spark\
        .read\
        .format('com.github.saurfang.sas.spark')\
        .load(filepath+dataset)\
        .select('cicid','i94yr','i94mon','i94cit','i94res','i94port',
                'arrdate','i94mode','i94addr','depdate','i94bir','i94visa',
                'count','dtadfile','visapost','occup','entdepa','entdepu','matflag',
                'biryear','dtaddto','gender','insnum','airline','admnum','fltno','visatype')
    
    # Filtering only air mode, and tourist visas
    
    df=df.filter((df.i94mode==1.0)&(df.visatype=='B2')&(df.i94cit.isin(*ExclusionList) == False)&(df.i94res.isin(*ExclusionList) == False)&(df.i94addr.isin(*InclusionList)))
    ## Aggregating - Optimization
    df.createOrReplaceTempView("TempTable")
    sqlDF = spark.sql("""SELECT 
                i94cit,i94res,i94port,
                arrdate,i94mode,i94addr,depdate,i94bir,i94visa,
                matflag,
                biryear,gender,airline,fltno,visatype,
                count(cicid) as TotalPassengers
                FROM TempTable
                group by 
                i94cit,i94res,i94port,
                arrdate,i94mode,i94addr,depdate,i94bir,i94visa,
                matflag,
                biryear,gender,airline,fltno,visatype""")   
    #write to parquet
    sqlDF.write.mode('overwrite').parquet("ParquetFiles/{}.parquet".format(dataset))
    
def read_parquet(spark,immigration_files):   
    '''
    Function to read Parquet Files saved for individual SAS datasets
    '''
    for i,file in enumerate(immigration_files):
        if i==0:
            df=spark.read.parquet("ParquetFiles/{}.parquet".format(file))
        else:
            new_df=spark.read.parquet("ParquetFiles/{}.parquet".format(file))
            df=df.union(new_df)
    return df

def process_immigration_data(spark, immigration_input_data,ExclusionList,InclusionList):
    '''
    Consolidate all Immigration Data
    Create Time Table as another subset table
    '''
    # get filepath to data file
    immigration_files = os.listdir(immigration_input_data)
    print("Immigration Files Found:")
    pprint(immigration_files)
    
    # read song data file
    for file in tqdm.tqdm(immigration_files):
        sasfile_to_parquet(spark,immigration_input_data,file,ExclusionList,InclusionList)
    
    print("SAS Files converted to Paraquet Files")
    
    # Combine All Parquest Files
    immigration_table = read_parquet(spark,immigration_files)
    print("SAS Files Combined. Converting Date Format")
    
    def extract_date(sasnumericdate):
        return pd.to_timedelta(sasnumericdate, unit='D') + pd.Timestamp('1960-1-1')

    get_date = udf(lambda x: extract_date(x),DateType())
    immigration_table = immigration_table.withColumn("arrdate", get_date(immigration_table.arrdate))
    
    print(immigration_table.show(2))
    print(immigration_table.count())

    
    # write combined table to parquet files
    immigration_table.write.mode('overwrite').parquet("spark-warehouse/TourismDataByAirChannel.parquet")
    
    print("Extracting Time Table")
      # extract columns to create time table
    time_table = immigration_table.select('arrdate',
                           year(immigration_table.arrdate).alias('year'),
                           month(immigration_table.arrdate).alias('month')
                          ).dropDuplicates()
    print(time_table.show(2))
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet("spark-warehouse/Time.parquet")
    print("Time Table Created")
    
    print("Processing Complete For Immigration Data")
    

def process_temperature_data(spark):
    '''
    Read Temperature Dataset
    '''
    GlobalLandTemperaturesByCity=spark.read.csv(r'GlobalLandTemperaturesByCity.csv',header=True)
    GlobalLandTemperaturesByCity = GlobalLandTemperaturesByCity.withColumn('dt', 
                   to_date(unix_timestamp(col('dt'), 'yyyy-MM-dd').cast("timestamp")))
    GlobalLandTemperaturesByCity = GlobalLandTemperaturesByCity.withColumn("month", month(GlobalLandTemperaturesByCity.dt).alias('month')) 
    GlobalLandTemperaturesByCity = GlobalLandTemperaturesByCity.withColumn("year", year(GlobalLandTemperaturesByCity.dt).alias('year')) 
    GlobalLandTemperaturesByCity = GlobalLandTemperaturesByCity.withColumn("dayofmonth", dayofmonth(GlobalLandTemperaturesByCity.dt).alias('dayofmonth')) 
    def extract_season(month):
        if month >= 3 and month <= 5:
            return 'spring'
        elif month >= 6 and month <= 8:
            return 'summer'
        elif month >= 9 and month <= 11:
            return 'autumn'
        else:
            return 'winter'

    get_season = udf(lambda x: extract_season(x),StringType())
    GlobalLandTemperaturesByCity = GlobalLandTemperaturesByCity.withColumn("season", get_season(GlobalLandTemperaturesByCity.month))
    print(GlobalLandTemperaturesByCity.printSchema())
    print(GlobalLandTemperaturesByCity.show(2))
    print(GlobalLandTemperaturesByCity.count())
    print("Temperature Data Processed")
    GlobalLandTemperaturesByCity.write.mode('overwrite').parquet("spark-warehouse/GlobalLandTemperaturesByCity.parquet")
    
def process_demographics_data(spark):
    us_cities_demographics=spark.read.csv(r'us-cities-demographics.csv',sep=';',header=True)
    us_cities_demographics = us_cities_demographics.withColumnRenamed("Median Age", "Median_Age")
    us_cities_demographics = us_cities_demographics.withColumnRenamed("Male Population", "Male_Population")
    us_cities_demographics = us_cities_demographics.withColumnRenamed("Female Population", "Female_Population")
    us_cities_demographics = us_cities_demographics.withColumnRenamed("Total Population", "Total_Population")
    us_cities_demographics = us_cities_demographics.withColumnRenamed("Number of Veterans", "Number_of_Veterans")
    us_cities_demographics = us_cities_demographics.withColumnRenamed("Foreign-born", "Foreign_born")
    us_cities_demographics = us_cities_demographics.withColumnRenamed("Average Household Size", "Average_Household_Size")
    us_cities_demographics = us_cities_demographics.withColumnRenamed("State Code", "State_Code")
    print(us_cities_demographics.printSchema())
    print(us_cities_demographics.show(2))
    print(us_cities_demographics.count())
    us_cities_demographics.write.mode('overwrite').parquet("spark-warehouse/us_cities_demographics.parquet")
    
def process_airport_data(spark):
    airport_codes=spark.read.csv(r'airport-codes_csv.csv',header=True)
    airport_codes=airport_codes.filter(airport_codes.iso_country=='US')
    print(airport_codes.printSchema())
    print(airport_codes.show(2))
    print(airport_codes.count())
    airport_codes.write.mode('overwrite').parquet("spark-warehouse/airport_codes.parquet")

def process_support_data(spark):
    '''
    Process Other files provided
    Argument: Active Spark Session
    Output: None
    '''
    process_temperature_data(spark)    
    process_demographics_data(spark)
    process_airport_data(spark)        
    print("Supporting Files Processed")

def joincheck1(spark,TourismDataByAirChannel_temp,airport_codes_temp):
    print("Join Check between Immigration Data and Airport Data")
    TourismDataByAirChannel_temp.createOrReplaceTempView("df1")
    airport_codes_temp.createOrReplaceTempView("df2")    
    sqlDF = spark.sql("""SELECT 
                i94port
                FROM df1 a
                inner join df2 b
                on a.i94port=b.iata_code limit 1
                """)    
    sqlDF.show()
    print("Check 1 Complete")
def joincheck2(spark,GlobalLandTemperaturesByCity_temp,us_cities_demographics_temp):
    print("Join Check between Weather Data and Demographics Data")
    GlobalLandTemperaturesByCity_temp.createOrReplaceTempView("df1")
    us_cities_demographics_temp.createOrReplaceTempView("df2")    
    sqlDF = spark.sql("""SELECT 
                a.city
                FROM df1 a
                inner join df2 b
                on a.city=b.city limit 1
                """)    
    sqlDF.show()
    
    print("Check 2 Complete")
def joincheck3(spark,TourismDataByAirChannel_temp,GlobalLandTemperaturesByCity_temp):
    print("Join Check between Immigration Data and Weather Data")    
    TourismDataByAirChannel_temp.createOrReplaceTempView("df1")
    GlobalLandTemperaturesByCity_temp.createOrReplaceTempView("df2")    
    print("Assuming 8 years seasonality in Temperature Fluctuations")
    sqlDF = spark.sql("""SELECT 
                a.arrdate
                FROM df1 a
                inner join df2 b
                on a.arrdate=add_months(b.dt,12*8) limit 1
                """)    
    sqlDF.show()
    print("Check 3 Complete")
def joincheck4(spark,TourismDataByAirChannel_temp,Time_temp):
    print("Join Check between Immigration Data and Time Data")
    TourismDataByAirChannel_temp.createOrReplaceTempView("df1")
    Time_temp.createOrReplaceTempView("df2")    
    sqlDF = spark.sql("""SELECT 
                a.arrdate
                FROM df1 a
                inner join df2 b
                on a.arrdate=b.arrdate limit 1
                """)    
    sqlDF.show() 
    
    print("Check 4 Complete")
def datacheck(spark):
    '''
    Argument: List of Files to be Checked
    Output: None
    '''
    print("Data Load Check")

    file="airport_codes"
    airport_codes_temp=spark.read.parquet("spark-warehouse/{}.parquet".format(file))
    print(airport_codes_temp.printSchema())
    print(airport_codes_temp.show(2))
    print(airport_codes_temp.count())

    file="us_cities_demographics"
    us_cities_demographics_temp=spark.read.parquet("spark-warehouse/{}.parquet".format(file))
    print(us_cities_demographics_temp.printSchema())
    print(us_cities_demographics_temp.show(2))
    print(us_cities_demographics_temp.count())
    
    file="GlobalLandTemperaturesByCity"
    GlobalLandTemperaturesByCity_temp=spark.read.parquet("spark-warehouse/{}.parquet".format(file))
    print(GlobalLandTemperaturesByCity_temp.printSchema())
    print(GlobalLandTemperaturesByCity_temp.show(2))
    print(GlobalLandTemperaturesByCity_temp.count())

    file="TourismDataByAirChannel"
    TourismDataByAirChannel_temp=spark.read.parquet("spark-warehouse/{}.parquet".format(file))
    print(TourismDataByAirChannel_temp.printSchema())
    print(TourismDataByAirChannel_temp.show(2))
    print(TourismDataByAirChannel_temp.count())

    file="Time"
    Time_temp=spark.read.parquet("spark-warehouse/{}.parquet".format(file))
    print(Time_temp.printSchema())
    print(Time_temp.show(2))
    print(Time_temp.count())
    
    print("Data Sample Check Complete")
        
    print("Checking Joins Now")
    joincheck1(spark,TourismDataByAirChannel_temp,airport_codes_temp)
    joincheck2(spark,GlobalLandTemperaturesByCity_temp,us_cities_demographics_temp)
    joincheck3(spark,TourismDataByAirChannel_temp,GlobalLandTemperaturesByCity_temp)
    joincheck4(spark,TourismDataByAirChannel_temp,Time_temp)    
    print("QC Complete")
    airport_codes_temp=None
    us_cities_demographics_temp=None
    GlobalLandTemperaturesByCity_temp=None
    TourismDataByAirChannel_temp=None
    Time_temp=None
    
def main():
    spark = create_spark_session()    
    immigration_input_data = '../../data/18-83510-I94-Data-2016/'
    print("Data Processing Started")
    print("Creating Staging Tables")
    ExclusionList=[403.0,
        712.0,
        700.0,
        719.0,
        574.0,
        720.0,
        106.0,
        739.0,
        394.0,
        501.0,
        404.0,
        730.0,
        731.0,
        471.0,
        737.0,
        753.0,
        740.0,
        710.0,
        505.0,
        0.0,
        705.0,
        583.0,
        407.0,
        999.0,
        239.0,
        134.0,
        506.0,
        755.0,
        311.0,
        741.0,
        54.0,
        100.0,
        187.0,
        190.0,
        200.0,
        219.0,
        238.0,
        277.0,
        293.0,
        300.0,
        319.0,
        365.0,
        395.0,
        400.0,
        485.0,
        503.0,
        589.0,
        592.0,
        791.0,
        849.0,
        914.0,
        944.0,
        996.0,
        ]
    InclusionList=['AL',
        'AK',
        'AZ',
        'AR',
        'CA',
        'CO',
        'CT',
        'DE',
        'DC',
        'FL',
        'GA',
        'GU',
        'HI',
        'ID',
        'IL',
        'IN',
        'IA',
        'KS',
        'KY',
        'LA',
        'ME',
        'MD',
        'MA',
        'MI',
        'MN',
        'MS',
        'MO',
        'MT',
        'NC',
        'ND',
        'NE',
        'NV',
        'NH',
        'NJ',
        'NM',
        'NY',
        'OH',
        'OK',
        'OR',
        'PA',
        'PR',
        'RI',
        'SC',
        'SD',
        'TN',
        'TX',
        'UT',
        'VT',
        'VI',
        'VA',
        'WV',
        'WA',
        'WI',
        'WY']
    process_immigration_data(spark, immigration_input_data,ExclusionList,InclusionList)  
    print("Processing Supporting Datasets")
    process_support_data(spark)   
    print("Data Check")        
    datacheck(spark)
    print("+++++++++++++++++++++++Code Run Completed+++++++++++++++++++++++")
        
if __name__ == "__main__":
    main()