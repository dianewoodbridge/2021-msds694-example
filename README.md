# 2021-msds694-example
This repository includes weekly examples for USF MSDS694 (Distributed Computing) Class.

More details about the each exercise are included in the course slides (uploaded in Canvas).

All codes require python 3.7 or higher and Spark 2.4 or higher.

### Week 1
1. Spark DataFrame creation exercises - using .toDF() and ss.createDataFrame()
2. DataFrame Basic APIs 
    - select()
    - drop()
    - filter(),where()
    - withColumnRenamed(), withColumn() (Renaming and adding columns)
    - orderBy(), sort()

### Week 2
1. DataFrame APIs
    - Scalar functions
    - Aggregate functions
    - Window functions
    - UDF (User Defined functions)
2. Registering DataFrame in the table catalog
3. Loaindg/Writing DataFrame in various sources/types
    - JSON, CSV, Parquet
    - S3, RDBMS, MongoDB

### Week 3
1. Main components of Spark MLlib
2. Create a feature and apply ML algorithms
3. Evaluate the model using cross validation and param grid builder.