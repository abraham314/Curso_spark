"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
PRACTICE Exercises : Spark SQL
-----------------------------------------------------------------------------
"""
#Do the usual setup. run pysetup for spark first and then the following
import os
os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
os.curdir

#Create a SQL Context from Spark context
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

"""
-----------------------------------------------------------------------------
1. Your course resource has a CSV file "iris.csv". 
Load that file into a Spark SQL Data Frame called "irisDF".

Hint: You need to use RDDs and remove the header line.
-----------------------------------------------------------------------------
"""
# Note : Spark SQL does not provide a direct way to load a csv into
# a DF. You need to first create an RDD and then load into a DF
 
irisRDD = sc.textFile("iris.csv")
#Remove the header line
irisData = irisRDD.filter(lambda x: "Sepal" not in x)
irisData.count()

#Split the columns
cols = irisData.map(lambda l : l.split(","))
#Make row objects
from pyspark.sql import Row
irisMap = cols.map( lambda p: Row ( SepalLengh = p[0], \
                                   SepalWidth = p[1], \
                                   PetalLength = p[2], \
                                   PetalWidth = p[3], \
                                   Species = p[4] ))
irisMap.collect()
#Create a data frame from the Row objects
irisDF = sqlContext.createDataFrame(irisMap)
irisDF.select("*").show()

"""
-----------------------------------------------------------------------------
2. In the irisDF, filter for rows whose PetalWidth is greater than 0.4
and count them.
Hint: Check for Spark documentation on how to count rows : 
https://spark.apache.org/docs/latest/api/python/pyspark.sql.html
-----------------------------------------------------------------------------
"""
irisDF.filter( irisDF["PetalWidth"] > 0.4).count()    
    
"""
-----------------------------------------------------------------------------
3. Register a temp table called "iris" using irisDF. Then find average
Petal Width by Species using that table.
-----------------------------------------------------------------------------
"""
irisDF.registerTempTable("iris")
sqlContext.sql("select Species,avg(PetalWidth) from iris group by Species")\
.show()

"""
-----------------------------------------------------------------------------
Hope you had some good practice !! Recommend trying out your own use cases
-----------------------------------------------------------------------------
"""
    



