from kubernetes import client, config

config.load_kube_config()
v1 = client.CoreV1Api()
# print("Listing pods with their IPs:")
# ret = v1.list_namespaced_pod('default')
# for e in ret.items:
#     print(e.metadata.name)
# print('--------------delete after----------------------------')
# v1.delete_namespaced_pod(name="spark-test", namespace="default")
#
# ret = v1.list_namespaced_pod('default')
# for e in ret.items:
#     print(e.metadata.name)
# print('------------------------------------------')


# ret = v1.list_namespaced_pod("default")
# for e in ret.items:
#     print(e)
# print('------------------------------------------')
#
# print(v1.read_namespaced_pod_log(name="spark-test", namespace="default"))


# from hdfs.client import Client
#
# hdfs = Client("http://192.168.10.22:9870", root='root')
#
# with open("hdfs://192.168.10.22:9000/spark_user/1.logs", 'w') as fp:
#     fp.write('fsdfasdfa')
# for e in v1.list_node():
#     print(v1.read_node(e))

# print(v1.read_node('cyf', exact=True, export=True))
# print(v1.get_api_resources())
print(v1.list_node())
