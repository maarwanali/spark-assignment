#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[28]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col,desc


# ### Postgres Params

# In[3]:


import os
from dotenv import load_dotenv


# In[4]:


load_dotenv()

JAR_PATH=os.getenv("JAR_PATH")

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")


# In[5]:


jdbc_url =f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


# In[6]:


spark = SparkSession.builder\
    .appName('pagial Analysis').config('spark.jars',JAR_PATH).getOrCreate()


# In[8]:


def read_table(table_name:str):
    
    try:


        df = (spark.read
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", jdbc_url)
            .option("dbtable", f"public.\"{table_name}\"")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .load()
             )
        # df = (spark.read.format('jdbc')
        #     .option("driver","org.postgresql.Driver")
        #     .option("url", jdbc_url)
        #     .option("dbtable","public.\"film\"")
        #     .option("user",DB_USER)
        #     .option("password", DB_PASSWORD).load()
        # )
        # df = spark.read.format('jdbc').options(
        #      url=jdbc_url,
        #     driver="org.postgresql.Driver",
        #     dbtable=table_name,
        #     user=DB_USER,
        #     password=DB_PASSWORD
        # ).load()

        df = df.withColumn('_source_table', F.lit(table_name))

        return df

    except Exception as e:
        print(f"Error reading table {table_name}: {str(e)}")
        return None


# In[9]:


tables = [
    "actor",
    "address",
    "category",
    "city",
    "country",
    "customer",
    "film",
    "film_actor",
    "film_category",
    "inventory",
    "language",
    "payment",
    "rental",
    "staff",
    "store"
]

dataframes = {}
for table in tables:
    dataframes[table] = read_table(table)


# #### - Output the number of movies in each category, sorted in descending order. 

# In[20]:


film_category_df = dataframes['film_category']\
    .join(dataframes['category'], dataframes['category']['category_id'] == dataframes['film_category']['category_id'], 'inner')


# In[29]:


number_of_movies = film_category_df.groupBy('name').count().orderBy(desc('count'))


# In[30]:


number_of_movies.show()


# #### - Output the 10 actors whose movies rented the most, sorted in descending order. 

# In[41]:


rental_actor_df = dataframes['rental']\
    .join(dataframes['inventory'], dataframes['rental']['inventory_id'] == dataframes['inventory']['inventory_id'], 'inner')\
    .join(dataframes['film_actor'], dataframes['inventory']['film_id'] == dataframes['film_actor']['film_id'], 'inner')\
    .join(dataframes['actor'], dataframes['film_actor']['actor_id'] == dataframes['actor']['actor_id'])

actories_df = rental_actor_df.groupBy('last_name','first_name').count().orderBy(desc('count')).limit(10)


# In[43]:


actories_df.show()


# #### - Output the category of movies on which the most money was spent. 

# In[ ]:





# #### - Output the names of movies that are not in the inventory. 

# In[ ]:





# #### - Output the top 3 actors who have appeared most in movies in the “Children” category. If several actors have the same number of movies, output all of them. 

# In[ ]:





# #### - Output cities with the number of active and inactive customers (active - customer.active = 1). Sort by the number of inactive customers in descending order. 

# In[ ]:





# #### - Output the category of movies that have the highest number of total rental hours in the cities (customer.address_id in this city), and that start with the letter “a”. Do the same for cities with a “-” symbol.

# In[ ]:




