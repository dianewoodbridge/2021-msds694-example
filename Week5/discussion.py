from pyspark import SparkContext
from pyspark.sql import SparkSession
from pysparkling import *
from h2o.estimators.deeplearning import H2ODeepLearningEstimator
import h2o

from user_definition import *


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    ss = SparkSession.builder\
        .config('spark.ext.h2o.log.level', 'FATAL')\
        .getOrCreate()
    hc = H2OContext.getOrCreate()
    #ss.sparkContext.setLogLevel('OFF')
    #log4j = sc._jvm.org.apache.log4j
    #log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)


    train = h2o.import_file(training_data)
    test = h2o.import_file(test_data)

    print()
    train.summary()
    print()
    test.summary()
    print()

    predictors = train.names[:]
    response = "C" + str(test.dim[1])
    predictors.remove(response)


    model_dl = H2ODeepLearningEstimator(variable_importances=True,
                                        loss="Automatic", seed = 1, epochs=30)

    model_dl.train(x=predictors,
                   y= "C" + str(test.dim[1]),
                   training_frame=train,
                   validation_frame=test)

    print(model_dl)
