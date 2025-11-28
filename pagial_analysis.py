#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


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


# In[7]:


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

        df = df.withColumn('_source_table', F.lit(table_name))

        return df

    except Exception as e:
        print(f"Error reading table {table_name}: {str(e)}")
        return None


# In[8]:


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

# In[9]:


film_category_df = dataframes['film_category']\
    .join(dataframes['category'], 'category_id', 'inner')


# In[11]:


number_of_movies = film_category_df.groupBy('name').agg(F.count('*').alias('movies_count')).orderBy(F.desc('movies_count'))


# In[12]:


number_of_movies.show()


# #### - Output the 10 actors whose movies rented the most, sorted in descending order. 

# In[14]:


rental_actor_df = dataframes['rental']\
    .join(dataframes['inventory'], 'inventory_id', 'inner')\
    .join(dataframes['film_actor'], 'film_id', 'inner')\
    .join(dataframes['actor'], 'actor_id', 'inner')

actories_df = rental_actor_df.groupBy('last_name','first_name')\
    .agg(F.count('*').alias('movies_count')).orderBy(F.desc('movies_count')).limit(10)


# In[15]:


actories_df.show()


# #### - Output the category of movies on which the most money was spent. 

# In[16]:


payment_category = dataframes['payment'].join(dataframes['rental'], 'rental_id', 'inner')\
                    .join(dataframes['inventory'], 'inventory_id', 'inner')\
                    .join(dataframes['film_category'], 'film_id', 'inner')\
                    .join(dataframes['category'], 'category_id', 'inner').select('name', 'amount')

most_spent_categories = payment_category.groupBy('name')\
    .agg(F.sum('amount').alias('total_money_spent')).orderBy(F.desc('total_money_spent')).limit(1)

most_spent_categories.show()


# #### - Output the names of movies that are not in the inventory. 

# In[17]:


uni_movies_df = dataframes['film'].join(dataframes['inventory'], 'film_id', 'left_anti')\
    .select('film_id', 'title')
uni_movies_df.show()


# #### - Output the top 3 actors who have appeared most in movies in the “Children” category. If several actors have the same number of movies, output all of them. 

# In[19]:


movies_category_df = dataframes['category'].join(dataframes['film_category'],'category_id','inner')\
                    .join(dataframes['film_actor'], 'film_id','inner')\
                    .join(dataframes['actor'], 'actor_id','inner').filter(F.col('name') == 'Children')\
                    .select('actor_id','first_name', 'last_name')

film_count_df = movies_category_df.groupBy('actor_id','first_name','last_name')\
    .agg(F.count("*").alias("film_count"))

windowSpec = Window.orderBy(F.desc('film_count'))

rn_actors_df = film_count_df.withColumn('rn', F.dense_rank().over(windowSpec))\
    .filter(F.col('rn') <=3)
rn_actors_df.show()


# #### - Output cities with the number of active and inactive customers (active - customer.active = 1). Sort by the number of inactive customers in descending order. 

# In[20]:


customer_status_df = dataframes['customer'].join(dataframes['address'], 'address_id', 'inner')\
                    .join(dataframes['city'], 'city_id', 'inner').select('city_id','city','active')
                    
fin_cus_status_df = customer_status_df.groupBy("city_id","city").agg(
                F.sum(F.when(F.col('active') == 1, 1).otherwise(0)).alias('active_count'),
                F.sum(F.when(F.col('active') == 0, 1).otherwise(0)).alias('inactive_count')
            ).orderBy(F.desc('inactive_count'))

fin_cus_status_df.show()


# #### - Output the category of movies that have the highest number of total rental hours in the cities (customer.address_id in this city), and that start with the letter “a”. Do the same for cities with a “-” symbol.

# In[21]:


windowSpec_cities = Window.partitionBy('city').orderBy(F.desc('total_hours'))


# In[22]:


category_rental_df = dataframes['rental'].withColumn(
     "hours_rented",
    F.timestamp_diff("HOUR", F.col("rental_date"), F.col("return_date")))\
                    .join(dataframes['customer'], 'customer_id', 'inner')\
                    .join(dataframes['address'], 'address_id', 'inner')\
                    .join(dataframes['city'], 'city_id', 'inner')\
                    .join(dataframes['inventory'],'inventory_id','inner')\
                    .join(dataframes['film_category'], 'film_id','inner')\
                    .join(dataframes['category'], 'category_id','inner')\
                    .select('rental_id','city', 'name','hours_rented')

total_rent_category_df = category_rental_df.groupBy('city','name').agg(F.sum('hours_rented').alias('total_hours'))

cities_satrt_a = total_rent_category_df.filter(F.col('city').ilike('a%'))
cities_have_dash = total_rent_category_df.filter(F.col('city').ilike("%-%"))

rank_cities_a_df = cities_satrt_a.withColumn('rn', F.rank().over(windowSpec_cities)).filter(F.col('rn') == 1)\
    .orderBy(F.desc('total_hours'))
rank_cities_dash_df = cities_have_dash.withColumn('rn', F.rank().over(windowSpec_cities)).filter(F.col('rn')== 1)\
    .orderBy(F.desc('total_hours'))

fnal_ranked_df = rank_cities_a_df.withColumn('source', F.lit('starts_with_a_cities')) \
    .unionAll(rank_cities_dash_df.withColumn('source', F.lit('contains_dash_cities')))


# In[23]:


fnal_ranked_df.show()

