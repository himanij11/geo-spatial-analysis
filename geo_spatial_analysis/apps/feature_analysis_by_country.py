import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('feature_analysis_by_country').getOrCreate()
sc = spark.sparkContext

def app():
    st.title("Data Visualisation")

    countries_data = types.StructType([
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
        types.StructField('country', types.StringType(), False),

    ])
    
    geospatial_data = spark.read.format("csv")\
                            .schema(countries_data)\
                            .options(compression='gzip', )\
                            .load('countries_data').repartition(8).cache()
                     
    geospatial_data.createOrReplaceTempView("countries_data")

    feature_class_disribution_schema = types.StructType([
        types.StructField('country_code', types.StringType(), False),
        types.StructField('feature_class', types.StringType(), False),
        types.StructField('feature_description', types.StringType(), False),
        types.StructField('no_of_features', types.IntegerType(), False),
        types.StructField('country', types.StringType(), False),
    ])

    country_mapping_schema = types.StructType([
    	        types.StructField('country_code', types.StringType(), False),
                types.StructField('country', types.StringType(), False),
    ])
    
    features_schema = types.StructType([
    types.StructField('feature_class', types.StringType()),
    types.StructField('feature_code', types.StringType()),
    types.StructField('short_description', types.StringType()),
    types.StructField('long_description', types.StringType()),
    ])
    
    countries_data = spark.read.format("csv").schema(feature_class_disribution_schema).load("feature_class_distribution")
    feature_class_distribution_df = spark.read.format("csv").schema(feature_class_disribution_schema).load("feature_class_distribution").cache()
    country_mapping = spark.read.format("csv").schema(country_mapping_schema).load("country_mapping").collect()
    
    feature_code_mpping = spark.createDataFrame([('A','country,state,region'),('H', "stream, lake"),('L', "park, area"),('P', "city, village"),('R', "road, railroad"),('S', "spot, building, farm"),('T', "mountain,hill,rock"),('U', "undersea"),('V', "forest,heath")],["feature_class","feature_class_description"])


    country_name_list=[]
    for c in country_mapping:
        country_name_list.append(c['country'])
    
    country_name_selected = st.selectbox('',country_name_list)
    country_features = feature_class_distribution_df[feature_class_distribution_df['country'] == country_name_selected].toPandas()
    fig = px.pie(country_features,values = country_features['no_of_features'],names = country_features['feature_description'])
    st.write(fig)
    
    diff_features = country_features['feature_description'].unique()
    
    feature_desc_selected = st.radio('',diff_features)
    
    feature_selected_df = feature_code_mpping[feature_code_mpping['feature_class_description'] == feature_desc_selected].collect()
    if(len(feature_selected_df)==0):
        st.subheader("Unfortunately we do not have data for this feature")
        return
    feature_selected = feature_selected_df[0]['feature_class']
    
    max_country_feature =country_features[country_features['no_of_features']==country_features['no_of_features'].max()]['feature_class'] 
    min_country_feature = country_features[country_features['no_of_features']==country_features['no_of_features'].min()]['feature_class']

    feature_code_df = spark.sql("select * from countries_data where country='{}' and feature_class='{}'".format(country_name_selected,feature_selected))
    feature_code_df.createOrReplaceTempView('features_class_code_df')
    
    feature_code_count=spark.sql('select count(feature_code) as no_of_feature_code,feature_class,feature_code from features_class_code_df group by feature_class,feature_code')
    feature_code_count.createOrReplaceTempView('feature_code_count')

    feature_class_code_mapping = spark.read.format("csv").schema(features_schema).load("features").cache()
    feature_class_code_mapping.createOrReplaceTempView('feature_class_code_mapping')
    
    feature_count_spark_df = spark.sql('select fc.*,f.long_description,f.short_description from feature_code_count fc join feature_class_code_mapping f on f.feature_class=fc.feature_class and f.feature_code= fc.feature_code')
    feature_count_spark_df.createOrReplaceTempView('feature_count_spark_sql')
    
    total_features_in_a_class = spark.sql('select sum(no_of_feature_code) as total_count,feature_class from feature_count_spark_sql group by feature_class')
    total_features_in_a_class.createOrReplaceTempView('total_features_in_a_class')
    
    features_class_table = spark.sql('select f.* ,t.total_count from feature_count_spark_sql f join total_features_in_a_class t on f.feature_class=t.feature_class')
    features_class_table.createOrReplaceTempView('features_class_table')
    features_class_table = spark.sql('select no_of_feature_code/total_count as percentage_of_feature,feature_code,feature_class,short_description from features_class_table')
    
    feature_count_pandas_df = features_class_table.toPandas()
    
    fig = px.bar(feature_count_pandas_df,x = feature_count_pandas_df['short_description'],y = feature_count_pandas_df['percentage_of_feature'])
    st.write(fig)
    
    st.title("Suggestions for improving countries economy")
   
    max_feature = max_country_feature.iloc[0]
    if max_feature =='A' :
        st.subheader("This country has many states which implies this country has many state governments.If this country could focus on improving (state/population) density than this could help the central government in managing its citizens more efficiently")
      
    if max_feature =='H':
        st.subheader('This country has a good potential for generating hydroelectricity.The government should work on that and try to explore more on this field')

    if max_feature =='L':
        st.subheader("This county has large number of parks which in turn increases the property value.The country should work on this field.Also environmental condition of this country is better which makes this country one of the best place to live")

    if max_feature =='P':
        st.subheader("This country has a good potential to invest on their infrastructure.The country should focus more on this field")    


    if max_feature =='R':
       st.subheader("This country has a very good transportation sector .Transportaion , import export business within the country will help to increase the economy of the country.")  

    if max_feature =='S':
        st.subheader("This country has a good potential for agriculture and farming .The government should work on that and try to invest more on agriculture field .Exporting it to other countries will also be a good option for this country") 

    if max_feature =='T' :
        st.subheader("This country is good for tourism as it contains many hill stations.If government invest on their tourism sector this can become one of the greatest revenue generating sector for the country")
    
    if max_feature =='U':
        st.subheader("Focussing on aquatic wildlife,aquatic sports can prove to be a major boom for this county")

    min_feature = min_country_feature.iloc[0]
    if min_feature =='A' :
        st.subheader("This country population density is not scattered properly, which hinders the growth of all the regions in the country.Government should try to invest more on low developed areas of the country")
      
    if min_feature =='H':
        st.subheader('This country should work on building dams and storing water.This country lacks lakes and river which are the major source of water supply for irrigation.Improving on this sector can improve economy of the country')

    if min_feature =='L':
        st.subheader("This county should work on building recreational features like parks, tourist places.These things help in building tourism ")

    if min_feature =='P':
        st.subheader("This country has a should work on their infrastructure")    

    if min_feature =='R':
       st.subheader("This country has a poor transportation system.They should focus more on building roads,railway etc")  

    if min_feature =='S':
        st.subheader("This country has a good potential for agriculture and farming .The government should work on that and try to invest more on agriculture field .Exporting it to other countries will also be a good option for this country") 

    if min_feature =='T' :
        st.subheader("This country should focus more on tourism as it contains many hill stations.If government invest on their tourism sector this can become one of the greatest revenue generating sector for the country")
    
    if min_feature =='U':
        st.subheader("Focussing on aquatic wildlife,aquatic sports can prove to be a major boom for this county")    
app()
