from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def business_count(x):
    if(x >= 70):
        return 'High'
    elif (x >= 40):
        return 'Medium'
    else:
        return 'low'

endpoint = 'msds694.cmxsootjz10m.us-west-2.rds.amazonaws.com'
database = 'postgres'
table = 'business'
properties = {'user': 'students', 'password': 'msdsstudents'}
url = 'jdbc:postgresql://%s/%s' % (endpoint, database)
    
ss = SparkSession.builder.getOrCreate()
sc = ss.sparkContext

business_df = ss.read.jdbc(url=url, table=table, properties=properties).repartition('zip', 'street').cache()

# Q1.
business_count_group = business_df.groupBy('zip','street')\
                                  .count()\
                                  .orderBy('count', 'zip',ascending=[False,True])
business_count_group.show(truncate=False)

# Q2.
business_count_udf = udf(business_count)
business_count_group.withColumn('business_count', business_count_udf('count')).show()

ss.stop()
