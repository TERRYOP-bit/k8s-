import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("PythonWordCount") \
    .getOrCreate()


def _map_to_pandas(rdds):
    return [pd.DataFrame(list(rdds))]


def to_pandas(df_s, n_partitions=None):
    if n_partitions is not None:
        df_s = df_s.repartition(n_partitions)
    df_pandas = df_s.rdd.mapPartitions(_map_to_pandas).collect()
    df_pandas = pd.concat(df_pandas)
    df_pandas.columns = df_s.columns
    return df_pandas


file_name = 'hdfs://192.168.10.75:9000/spark_user/11701080221/data/behavior_log.csv'
group_file = 'hdfs://192.168.10.75:9000/spark_user/11701080221/result/behavior_log_group.csv'
group_double_file = 'hdfs://192.168.10.75:9000/spark_user/11701080221/result/behavior_log_group_double.csv'
# data = pd.read_table(file_name, sep=',', encoding='gbk')
# print(data.head())
src_data = spark.read.csv(file_name, header=True, encoding='gbk')
# data = spark_df.toPandas()
data = src_data.distinct()
data.show()

group_data = src_data.groupby(['user', 'cate']).agg()
group_data.show()
# df = pd.DataFrame(columns=['user', 'cate', 'buy_time', 'buy_count', 'score'])
# df = df.astype({'user': int, 'cate': int, 'buy_count': int, 'score': int})
# btag_score = {'pv': 1, 'cart': 5, 'fav': 9, 'buy': 15}
#
# for (user, cate), group_df in src_data.groupby(['user', 'cate']):
#     insert_data = {'user': user, 'cate': cate, 'buy_count': 0, 'score': 0, 'buy_time': None}
#     for index in group_df.index:
#         if group_df['btag'][index] == 'buy':
#             insert_data['buy_count'] += 1
#             insert_data['buy_time'] = group_df['time_stamp'][index]
#         insert_data['score'] += btag_score[group_df['btag'][index]]
#     df.loc[df.shape[0]] = insert_data
# # df.to_csv(group_file, index=False)
# df = spark.createDataFrame(df)
# df.show()
# df.write.format("csv").save(group_file)
#
# # df = pd.read_table(group_file, sep=',', encoding='gbk')
# df1 = pd.DataFrame(columns=['user', 'buy_time', 'buy_count'])
# df1 = df1.astype(int)
# df.dropna(axis=0, how='any', inplace=True)
# for user, group_df in df.groupby(['user']):
#     insert_data = {'user': user, 'buy_count': 0, 'buy_time': None}
#     for index in group_df.index:
#         if group_df['buy_time'][index] is not None:
#             if insert_data['buy_time'] is not None:
#                 insert_data['buy_time'] = min(group_df['buy_time'][index], insert_data['buy_time'])
#             else:
#                 insert_data['buy_time'] = group_df['buy_time'][index]
#         insert_data['buy_count'] = group_df['buy_count'].sum()
#     df1.loc[df1.shape[0]] = insert_data
# df1 = spark.createDataFrame(df1)
# df1.show()
# df1.write.format("csv").save(group_double_file)
# # df1.to_csv(group_double_file, index=False)
