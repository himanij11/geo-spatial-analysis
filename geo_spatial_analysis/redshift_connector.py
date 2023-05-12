#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text


# In[ ]:


sql = """
with feature_keyword as(select * from features where short_description like '%hospital' or  long_description like '%hospital' or 
short_description like '%medical' or  long_description like '%medical' or  short_description like '%pharmacy' or  long_description like '%pharmacy') select * from countries join feature_keyword  on countries.feature_class= feature_keyword.feature_class and  countries.feature_code= feature_keyword.feature_code
;
""" 


# In[ ]:


redshift_endpoint1 = "project-redshift-cluster.cnlip4ab5z0u.us-west-2.redshift.amazonaws.com"
redshift_user1 = "awsuser"
redshift_pass1 = "Admin11#"
port1 = 5439 #whaterver your Redshift portnumber is
dbname1 = "dev"


# In[ ]:


from sqlalchemy import create_engine
from sqlalchemy import text
engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" % (redshift_user1, redshift_pass1, redshift_endpoint1, port1, dbname1)
engine1 = create_engine(engine_string)


# In[ ]:


df1 = pd.read_sql_query(text(sql), engine1)

