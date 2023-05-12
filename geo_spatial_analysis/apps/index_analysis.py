import streamlit as st
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import plotly.express as px
import os
import pyspark
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from shapely.geometry import Point, Polygon

def app():
    st.title("Visual Analysis by Index")
    geonames_schema = types.StructType([
        types.StructField('geonameid', types.IntegerType(), False),
        types.StructField('name', types.StringType(), False),
        types.StructField('latitude', types.FloatType(), False),
        types.StructField('longitude', types.FloatType(), False),
        types.StructField('feature_class', types.StringType(), False),
        types.StructField('feature_code', types.StringType(), False),
        types.StructField('country_code', types.StringType(), False),
        types.StructField('population', types.LongType(), False),
        types.StructField('elevation', types.IntegerType(), False),
        types.StructField('timezone', types.StringType(), False),
        types.StructField('modification_date', types.DateType(), False),
    ])
    
    feature_description_schema = types.StructType([
        types.StructField('feature_class', types.StringType()),
        types.StructField('feature_code', types.StringType()),
        types.StructField('short_description', types.StringType()),
        types.StructField('long_description', types.StringType()),
    ])
    
    countries_info_schema = types.StructType([
        types.StructField('country', types.StringType()),
        types.StructField('capital', types.StringType()),
        types.StructField('area(in sq km)', types.LongType()),
        types.StructField('population', types.LongType()),
        types.StructField('continent', types.StringType()),
        types.StructField('geonameid', types.LongType()),
        types.StructField('neighbours', types.StringType()),
    ])
    
    country_mapping_schema = types.StructType([
        types.StructField('country_code', types.StringType()),
        types.StructField('country_name', types.StringType()),
    ])
    
    plt.rcParams['figure.figsize'] = (20, 10)

    ountries_data = "clean-data"
    feature_description_input = "features"
    country_mapping_input = "country_mapping"
    countries_info_input = "country_info"
    spark = SparkSession.builder.appName('read geo spatial data from s3').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    geospatial_data = spark.read.format("csv") \
        .option("delimiter", ",") \
        .schema(geonames_schema) \
        .options(compression='gzip') \
        .load('countries_data').repartition(8).cache()

    feature_description_df = spark.read.format("csv") \
        .option("delimiter", ",") \
        .schema(feature_description_schema) \
        .load('features')
        
    country_mapping_df = spark.read.format("csv") \
        .option("delimiter", ",") \
        .schema(country_mapping_schema) \
        .load('country_mapping')
    
    countries_info_df = spark.read.format("csv") \
        .option("delimiter", ",") \
        .schema(countries_info_schema) \
        .load('country_info')

    country_mapping = spark.read.format("csv").schema(country_mapping_schema).load("country_mapping")
    country_mapping_list = country_mapping.collect()

    def keyword_mapping(keyword):
        if keyword == "health":
            return {"short_description": ["hospital", "medical"], "long_description": ["hospital", "medical"]}
        if keyword == "school":
            return {"short_description": ["school", "university"]}


    def analysis_by_world_data(analysis_based_on="health"):
        countries_name_with_code = countries_info_df.join(geospatial_data, ["geonameid"]).cache()

        country_wise_features_df = geospatial_data.groupby(geospatial_data["country_code"],
                                                           geospatial_data["feature_code"]).count()
        keywords = keyword_mapping(analysis_based_on)
        feature_based_on_filtered_df = spark.createDataFrame([], feature_description_schema)
        
        for column, filters in keywords.items():
            feature_based_on_filtered_df = feature_based_on_filtered_df.union(feature_description_df.where(
            feature_description_df[column].rlike("|".join(["(" + pat + ")" for pat in filters]))))

        feature_based_on_filtered_df = feature_based_on_filtered_df.dropDuplicates().cache()

        countries_based_on_analysis_df = country_wise_features_df.join(feature_based_on_filtered_df, ["feature_code"])
    
        countries_based_on_analysis_with_population_df = countries_based_on_analysis_df.join(
        countries_name_with_code, ["country_code"]).select(
        countries_based_on_analysis_df["feature_code"],
        countries_based_on_analysis_df["country_code"],
        countries_based_on_analysis_df["count"].alias("hospitals_count"),
        countries_based_on_analysis_df["feature_class"],
        countries_info_df["population"],
        countries_info_df["area(in sq km)"],
        countries_info_df["geonameid"],
        countries_info_df["neighbours"],
        ).cache()

    # calculating feature index by feature_class
        countries_based_on_analysis_with_population_df_by_feature_class = countries_based_on_analysis_with_population_df. \
            groupby(countries_based_on_analysis_with_population_df["country_code"]).sum("hospitals_count")

        countries_based_on_analysis_with_population_df_by_feature_class = \
            countries_based_on_analysis_with_population_df_by_feature_class.join(
                countries_based_on_analysis_with_population_df, ["country_code"])
            
        countries_based_on_analysis_with_population_df_by_feature_class = \
            countries_based_on_analysis_with_population_df_by_feature_class.select(
                countries_based_on_analysis_with_population_df_by_feature_class["feature_class"],
                countries_based_on_analysis_with_population_df_by_feature_class["country_code"],
                countries_based_on_analysis_with_population_df_by_feature_class["sum(hospitals_count)"].alias(
                "hospitals_count"),
                countries_based_on_analysis_with_population_df_by_feature_class["population"],
                countries_based_on_analysis_with_population_df_by_feature_class["area(in sq km)"],
                countries_based_on_analysis_with_population_df_by_feature_class["geonameid"],
                countries_based_on_analysis_with_population_df_by_feature_class["neighbours"],
                )
            
        countries_based_on_analysis_with_population_df_by_feature_class = \
            countries_based_on_analysis_with_population_df_by_feature_class.distinct()
        
        countries_based_on_analysis_with_population_df_by_feature_class = \
            countries_based_on_analysis_with_population_df_by_feature_class. \
                withColumn("analysis_index_by_feature_class",
                           countries_based_on_analysis_with_population_df_by_feature_class["population"] /
                           countries_based_on_analysis_with_population_df_by_feature_class["hospitals_count"]
                           )
        max_value = float(
            countries_based_on_analysis_with_population_df_by_feature_class.agg({"analysis_index_by_feature_class": "max"})
            .collect()[0]["max(analysis_index_by_feature_class)"])
        
        min_value = float(
            countries_based_on_analysis_with_population_df_by_feature_class.agg({"analysis_index_by_feature_class": "min"})
            .collect()[0]["min(analysis_index_by_feature_class)"])
    
        countries_based_on_analysis_with_population_spark_df_by_feature_class = countries_based_on_analysis_with_population_df_by_feature_class
    
        countries_based_on_analysis_with_population_pd_df_by_feature_class = countries_based_on_analysis_with_population_df_by_feature_class.\
        toPandas()
        
        countries_based_on_analysis_with_population_pd_df_by_feature_class['new_analysis_index_by_feature_class'] = \
        (countries_based_on_analysis_with_population_pd_df_by_feature_class['analysis_index_by_feature_class'] - min_value) / (max_value-min_value)
        
    
        # calculating analysis index by feature_code
        countries_based_on_analysis_with_population_df_by_feature_code = countries_based_on_analysis_with_population_df.withColumn(
            "analysis_index_by_feature_code",
            countries_based_on_analysis_with_population_df["population"] /
            countries_based_on_analysis_with_population_df["hospitals_count"]
        )
        
        plot_by_feature_class(analysis_based_on, countries_based_on_analysis_with_population_pd_df_by_feature_class, countries_based_on_analysis_with_population_df_by_feature_code, countries_based_on_analysis_with_population_spark_df_by_feature_class)

    def plot_by_feature_class(analysis_based_on, countries_based_on_analysis_with_population_pd_df_by_feature_class, countries_based_on_analysis_with_population_df_by_feature_code, countries_based_on_analysis_with_population_spark_df_by_feature_class):
        st.title(f"Data Visualisation by {analysis_based_on} Index")
        world_map = gpd.read_file('shapes_simplified_low.json')
        world_map = world_map.rename(columns={'geoNameId': 'geonameid'})
        world_map["geonameid"] = world_map['geonameid'].apply(lambda x: np.int64(x))
        countries_based_on_analysis_with_shapes = world_map.merge(
            countries_based_on_analysis_with_population_pd_df_by_feature_class, on='geonameid')
        data_proj = countries_based_on_analysis_with_shapes.to_crs(epsg=4326)
        data_proj['analysis_index_by_feature_class'] = countries_based_on_analysis_with_shapes['new_analysis_index_by_feature_class']
        # increase map size
        plt.rcParams['figure.figsize'] = (20, 8)
        data_proj.plot('analysis_index_by_feature_class', norm=matplotlib.colors.LogNorm())
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()
    
        plot_by_feature_code(analysis_based_on, countries_based_on_analysis_with_population_spark_df_by_feature_class, countries_based_on_analysis_with_population_df_by_feature_code)
    
    def plot_by_feature_code(analysis_based_on, countries_based_on_analysis_with_population_spark_df_by_feature_class,countries_based_on_analysis_with_population_df_by_feature_code):
        st.title("Data Visualisation using Feature Code by Country")
        country_name_list=[]
        for c in country_mapping_list:
            country_name_list.append(c['country_name'])
    
        country_name_selected = st.selectbox('',country_name_list)
        #print(country_name_selected)
        country_code_selected = country_mapping[country_mapping['country_name'] == country_name_selected].collect()[0]['country_code']
        #print(country_code_selected)
        feature_code_analysis_based_on_feature_code = countries_based_on_analysis_with_population_df_by_feature_code[countries_based_on_analysis_with_population_df_by_feature_code['country_code']== country_code_selected]
        feature_code_analysis_based_on_feature_code = feature_code_analysis_based_on_feature_code.join(feature_description_df,["feature_code"])
        #feature_code_analysis_based_on_feature_code.show()
        feature_code_analysis_based_on_feature_code = feature_code_analysis_based_on_feature_code.toPandas()
        fig = px.bar(feature_code_analysis_based_on_feature_code,x=feature_code_analysis_based_on_feature_code['short_description'],y=feature_code_analysis_based_on_feature_code['hospitals_count'])
        fig.update_layout(title = 'Visualisation by feature code', xaxis_title = 'Short Description', yaxis_title = analysis_based_on + ' Count')
        st.write(fig)
        compare_with_neighbours(analysis_based_on, country_code_selected, countries_based_on_analysis_with_population_spark_df_by_feature_class, country_name_selected) 
    
    def compare_with_neighbours(analysis_based_on, country_code_selected, countries_based_on_analysis_with_population_spark_df_by_feature_class, country_name_selected):
        st.title("Comparison of "+ country_name_selected +" with its neighbours")
        neighbours = countries_based_on_analysis_with_population_spark_df_by_feature_class.filter(countries_based_on_analysis_with_population_spark_df_by_feature_class["country_code"]== country_code_selected).collect()[0]['neighbours']
        if not neighbours:
            st.subheader("This country has no neigbhours.")
            return
        neighbours = neighbours.split(",")
        neighbours.append(country_code_selected)
        neighbours_feature_based_on_filtered_df = countries_based_on_analysis_with_population_spark_df_by_feature_class.filter(countries_based_on_analysis_with_population_spark_df_by_feature_class['country_code'].isin(neighbours)) 
        neighbours_feature_based_on_filtered_df = neighbours_feature_based_on_filtered_df.toPandas()
        #neighbours_feature_based_on_filtered_df.show()
        fig = px.bar(neighbours_feature_based_on_filtered_df, x = neighbours_feature_based_on_filtered_df['country_code'], y = neighbours_feature_based_on_filtered_df['analysis_index_by_feature_class'])
        fig.update_layout(title = 'Comparison of ' + country_code_selected + " with its neighbours", xaxis_title = 'Country Code', yaxis_title = analysis_based_on + ' Index by Feature Class')
        st.write(fig)
        
    features_list = ['health', 'school']
    feature_selected = st.selectbox('Choose the feature to display analysis',features_list)
    analysis_by_world_data(analysis_based_on=feature_selected)
app()