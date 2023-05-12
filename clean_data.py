import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


def load_data_from_s3(folder_path, output):
    geonames_schema = types.StructType([
        types.StructField('geonameid', types.IntegerType(), False),
        types.StructField('name', types.StringType(), False),
        types.StructField('ascii_name', types.StringType(), False),  # not required
        types.StructField('alternate_names', types.StringType(), False),  # not required
        types.StructField('latitude', types.FloatType(), False),
        types.StructField('longitude', types.FloatType(), False),
        types.StructField('feature_class', types.StringType(), False),
        types.StructField('feature_code', types.StringType(), False),
        types.StructField('country_code', types.StringType(), False),
        types.StructField('cc2', types.StringType(), False),  # not required
        types.StructField('admin1_code', types.StringType(), False),  # not required
        types.StructField('admin2_code', types.StringType(), False),  # not required
        types.StructField('admin3_code', types.StringType(), False),  # not required
        types.StructField('admin4_code', types.StringType(), False),  # not required
        types.StructField('population', types.LongType(), False),
        types.StructField('elevation', types.IntegerType(), False),
        types.StructField('dem', types.StringType(), False),  # not required
        types.StructField('timezone', types.StringType(), False),
        types.StructField('modification_date', types.DateType(), False),
    ])

    geospatial_data = spark.read.format("csv")\
                                .option("delimiter", "\t")\
                                .schema(geonames_schema)\
                                .options(compression='gzip', )\
                                .load(folder_path)
    # drop the unnecessary columns
    geospatial_data_filtered = geospatial_data.drop("ascii_name", "alternate_names", "cc2", "admin1_code",
                                                    "admin2_code", "admin3_code", "admin4_code", "dem")
    # replacing NaN will null values
    geospatial_data_filtered = geospatial_data_filtered.replace(float('nan'), None)
    # replace null with 0 for integer values
    geospatial_data_filtered = geospatial_data_filtered.na.fill(value=0, subset=["latitude", "longitude", "population"])
    # replace null with "0" for string values
    geospatial_data_filtered = geospatial_data_filtered.na.fill(
        value="", subset=["name", "feature_class", "feature_code", "country_code", "elevation", "timezone"])
    # print(geospatial_data_filtered.show(1080))
    print(geospatial_data_filtered.show(1080))
    # geospatial_data_filtered.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('read geo spatial data from s3').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    load_data_from_s3(inputs, output)

