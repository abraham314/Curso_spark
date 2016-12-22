# -*- coding: utf-8 -*-
"""
Make sure you give execute privileges
-----------------------------------------------------------------------------

           Spark with Python: Setup Spyder IDE for Spark

             Copyright : V2 Maestros @2016
                    
Execute this script once when Spyder is started on Windows
-----------------------------------------------------------------------------
"""

import os
import sys
os.chdir("C:\Users\mb45296\data_spark_udemy\RB-Python")
os.curdir

# Configure the environment. Set this up to the directory where
# Spark is installed
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = 'C:\Users\mb45296\spark-1.3.1-bin-hadoop2.4'

# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

#Add the following paths to the system path. Please check your installation
#to make sure that these zip files actually exist. The names might change
#as versions change.
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.8.2.1-src.zip"))

#Initiate Spark context. Once this is done all other applications can run
from pyspark import SparkContext
from pyspark import SparkConf

# Optionally configure Spark Settings
conf=SparkConf()
conf.set("spark.executor.memory", "1g")
conf.set("spark.cores.max", "2")

conf.setAppName("V2 Maestros")

## Initialize SparkContext. Run only once. Otherwise you get multiple 
#Context Error.
sc = SparkContext('local', conf=conf)

#Test to make sure everything works.
lines=sc.textFile("auto-data.csv")
lines.count()
