from hdfs.client import Client
from kubernetes import client, config
config.load_kube_config()
v1 = client.CoreV1Api()

hdfs = Client("http://192.168.10.75:9870", root='root')
namespace = "default"
for e in client.open('/spark_user/password.csv'):
    print(e)
#
# print(hdfs.list('/spark_user'))

from pyspark.sql import SparkSession
spark = SparkSession\
.builder\
.appName("PythonWordCount")\
.getOrCreate()
spark.read.csv()
