import streamlit as st
import geopandas as gpd
import matplotlib.pyplot as plt
import geoplot
import plotly.express as px
import pyspark
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
import pandas as pd
import os
from shapely.geometry import Point, Polygon
from descartes import PolygonPatch

def app():
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
    
    country_mapping_schema= types.StructType([
    	    types.StructField('country_code', types.StringType(), False),
            types.StructField('country', types.StringType(), False),
    ])
    
    plt.rcParams['figure.figsize'] = (20, 10)
    
    countries_data = "clean-data"
    feature_description_input = "features"
    spark = SparkSession.builder.appName('read geo spatial data from s3').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    geospatial_data = spark.read.format("csv") \
                            .option("delimiter", ",") \
                            .schema(geonames_schema) \
                            .options(compression='gzip') \
                            .load('clean-data').repartition(8).cache()

    feature_description_df = spark.read.format("csv") \
                            .option("delimiter", ",") \
                            .schema(feature_description_schema) \
                            .load('features')
                            
    country_mapping_df = spark.read.format("csv") \
                            .option("delimiter", ",") \
                            .schema(country_mapping_schema) \
                            .load('country_mapping')

    countries_info_df = spark.read.format("csv")\
                                    .option("delimiter", ",")\
                                    .schema(countries_info_schema)\
                                    .load('country_info')

    country_mapping_list = country_mapping_df.collect()

    country_name_list=[]
    for c in country_mapping_list:
        country_name_list.append(c['country'])
    country_name_selected = st.selectbox('',country_name_list)
    country_code = country_mapping_df[country_mapping_df['country'] == country_name_selected].collect()[0]['country_code']
        
    def keyword_mapping(keyword):
        if keyword == "health":
            return {"short_description": ["hospital", "medical"], "long_description": ["hospital", "medical"]}
        if keyword == "school":
            return {"short_description": ["school", "university"]}

    def analyse(country_code, feature_analysis="health", based_on="school"):
        country_df = geospatial_data.filter(geospatial_data['country_code'] == country_code)
        keywords = keyword_mapping(based_on)
        
        feature_based_on_filtered_df = spark.createDataFrame([], feature_description_schema)
        for column, filters in keywords.items():
            feature_based_on_filtered_df = feature_based_on_filtered_df.union(feature_description_df.where(
            feature_description_df[column].rlike("|".join(["(" + pat + ")" for pat in filters]))))
        feature_based_on_filtered_df = feature_based_on_filtered_df.dropDuplicates().cache()

        keywords = keyword_mapping(feature_analysis)
        feature_analysis_filtered_df = spark.createDataFrame([], feature_description_schema)
        for column, filters in keywords.items():
            feature_analysis_filtered_df = feature_analysis_filtered_df.union(feature_description_df.where(
            feature_description_df[column].rlike("|".join(["(" + pat + ")" for pat in filters]))))
        feature_analysis_filtered_df = feature_analysis_filtered_df.dropDuplicates().cache()

        country_data_feature_based_on_filtered_df = country_df.join(feature_based_on_filtered_df, ["feature_code"])
        country_feature_analysis_filtered_df = country_df.join(feature_analysis_filtered_df, ["feature_code"])

        country_data_feature_based_on_filtered_pd_df = country_data_feature_based_on_filtered_df.toPandas()
        countries_feature_analysis_filtered_pd_df = country_feature_analysis_filtered_df.toPandas()

        country_data_feature_based_on_points = [Point(xy) for xy in
                                                zip(country_data_feature_based_on_filtered_pd_df['longitude'],
                                                    country_data_feature_based_on_filtered_pd_df['latitude'])]
        
        countries_feature_analysis_points = [Point(xy) for xy in
                                             zip(countries_feature_analysis_filtered_pd_df['longitude'],
                                                 countries_feature_analysis_filtered_pd_df['latitude'])]
        crs = {'init': 'epsg:4326'}
        country_data_feature_based_on_gdf = gpd.GeoDataFrame(country_data_feature_based_on_filtered_pd_df,
                                                             crs=crs,
                                                             geometry=country_data_feature_based_on_points)
        
        countries_feature_analysis_gdf = gpd.GeoDataFrame(countries_feature_analysis_filtered_pd_df,
                                                          crs=crs,
                                                          geometry=countries_feature_analysis_points)

        geonameid = get_geonameid(country_code)
        geonameid = str(geonameid)
        calculate_buffer_and_plot(geonameid, country_data_feature_based_on_gdf, countries_feature_analysis_gdf)

    def get_geonameid(country_code):
        country_name = country_mapping_df[country_mapping_df['country_code'] == country_code].collect()[0]['country']
        geonameid = countries_info_df[countries_info_df['country'] == country_name].collect()[0]['geonameid']
        return geonameid

    def plot_heatmap(country_map, country_data_feature_based_on_gdf):
        country_map['geometry'] = country_map.buffer(0)
        kwargs = {'Label': 'hospitals_in_2km'}
        ax = geoplot.kdeplot(
            country_data_feature_based_on_gdf, clip=country_map.geometry,
            shade=True, cmap='Reds', figsize=(20, 10),
            projection=geoplot.crs.AlbersEqualArea(), **kwargs)
        
        geoplot.polyplot(country_map, ax=ax, zorder=1)
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()

    def calculate_buffer_and_plot(geonameid, country_data_feature_based_on_gdf, countries_feature_analysis_gdf):
        BLUE = '#6699cc'
        GREEN = '#c1d6de'
        fig = plt.figure(figsize=(20, 10))
        world_map = gpd.read_file('shapes_simplified_low.json')
        data_proj = world_map.to_crs(epsg=4326)
        country_map = data_proj.loc[world_map['geoNameId'] == geonameid]
        ax = country_map.plot("area", color="red")

        for index, row in country_data_feature_based_on_gdf.iterrows():
            buffer = row.geometry.buffer(2)
            hospitals_inside_buffer = countries_feature_analysis_gdf[countries_feature_analysis_gdf.geometry.within(buffer)]
            country_data_feature_based_on_gdf.at[index, "hospitals_in_2km"] = hospitals_inside_buffer.shape[0]
            
        ax.add_patch(PolygonPatch(buffer, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2))
        ax.axis('scaled')
    
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()

        country_map['geometry'] = country_map.buffer(0)
        kwargs = {'Label': 'hospitals_in_2km'}
        ax = geoplot.kdeplot(
        country_data_feature_based_on_gdf, clip=country_map.geometry,
        shade=True, cmap='Reds', figsize=(20, 10),
        projection=geoplot.crs.AlbersEqualArea(), **kwargs)
        geoplot.polyplot(country_map, ax=ax, zorder=1)
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot() 
    analyse(country_code)
app()