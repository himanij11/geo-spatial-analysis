# Geo Spatial Analysis
Why we used both S3 and Redshift
We are working with geospatial data i.e every data point we have is represented in latitude & longitude. The dataset for which we have done the analysis and designed the solution is the minimalist dataset which is a replica of the original dataset will contain every point in the globe.We are using the replica and still the size of the file is around 1.3 GB. If we consider the whole dataset(dataset containing every point in the globe) the file size might be in TBs or even PetaBytes. We cannot directly query on such big data. We need a data lake to store this data and a data lake like S3 seems the best option for it.
The real world problems will not require the whole dataset to be used.This is one of the many problems(features) which the model focuses on. For instance - If the Indian government has to look into expanding their military power, the first thing they will do is to compare it with their neighbours to analyze how well they are prepared for adverse situations(This info can be easily generated from our dataset).
Also the best way to analyze your power is to compare it with super powers like (USA,CHINA,RUSSIA,CANADA).The whole dataset for this use case will contain a maximum of 10 countries whereas our S3 has a dataset of 253 countries(There are 253 countries in the world). So, it is better to transfer that small chunk of data into redshift.The dataset in redshift is cleaned data which can be used for analysis.


Why we used AWS Glue
This dataset keeps changing everyday so it's not feasible to store the dataset locally or in the cloud and download it everyday as all the analysis depends on the dataset. Also even if somehow we write a script to download dataset everyday it will be a cumbersome task to extract clean data everyday and load it to the cluster .This is where Aws Glue comes in to picture.AWS Glue is a ETL tool (not a data lake)in which we define a S3 crawler which when given S3 bucket link, takes data from S3, infer and store the schema and store it in order to perform ETL operations.In AWS Glue we have also defined redshift crawler which dumps data from S3 to Redshift.This whole process is automated by Glue only and we have the choice to run this whole process after a particular interval of time(In our case each day)


Summary of what we are trying to do:
Our dataset contains every feature which one can think of.Be it a school,lake,city,hospital etc. Although this information can be used by anyone, we have implemented all our solutions keeping in mind our target audience to be the government (be it a state government or central government). We have generalized all our code i.e if we address a particular problem for a particular feature, the same code with different features can be used by the government of any country to analyze their data.
Some of the use cases which we did was:
Analyze different features of a particular country and suggest which sector they should improve on.For example- This is what we inferred from our analysis- USA has the best health index. Although we go deeper into the analysis part we will see that there are very few clinic ,eye care centres in the country.Its because the number of hospitals in comparison with population in the USA is way more than other countries, it has a high health index.
This information is useful for the government to improve the country's economy and look into the people's welfare. Same to the health index,a country can look into the education index to find where they need to make a new school which will improve the country's literacy rate .
Establishing a school just by looking at a place where the education index is low is not sufficient.Its advisable that each educational institute should have a hospital nearby. This type of analysis is only possible with the help of geospatial analysis. What we did was calculate all schools of a particular country. We had the latitude and longitude of the school. We draw a 5km radius from that point and find out how many hospitals are there nearby a school .If no hospital is nearby the government should establish a hospital near the school.Also school should not be made where there is no hospital nearby.

Similiary like this government can infer many thinks for different features.



