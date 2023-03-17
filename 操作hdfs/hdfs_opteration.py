from hdfs.client import Client
from kubernetes import client, config
from datetime import datetime, timezone
from datetime import *
config.load_kube_config()
v1 = client.CoreV1Api()

hdfs = Client("http://192.168.10.75:9870", root='root')
namespace = "default"
# print(hdfs.list())

# 显示目录下的文件名
def list_dir_file(user_name, file_flag):
    # 用户代码文件夹路径
    list_path = '/spark_user/'+user_name+file_flag
    try:
        files = hdfs.list(list_path)
        return files
    except Exception as e:
        print(e)
        return e

# 将文件上传到hdfs
def uploading_file(local_path, user_name, file_flag):
    # 文件上传到指定目录下
    target_path = '/spark_user/' + user_name + file_flag
    try:
        hdfs.upload(target_path, local_path)
        return 1
    except Exception as e:
        print(e)
        return e

# 查看文件的内容
def see_file(file_name, user_name, file_flag):

    file_path = "/spark_user/" + user_name + file_flag + file_name

    try:
        data = []
        with hdfs.read(file_path, encoding='utf-8') as fp:
            for line in fp:
                data.append(line.strip())
        print(data)
        return 1, data
    except Exception as e:
        print(e)
        return -1, e

# 删除文件
def delete_file(file_name, user_name, file_flag):
    target_file = "/spark_user/" + user_name + file_flag + file_name

    try:
        hdfs.delete(target_file)
        return 1
    except Exception as e:
        print(e)
        return e

# 重命名文件
def rename_file(file_name, rename, user_name, file_flag):
    path = "/spark_user/" + user_name + file_flag
    print(path)
    try:
        hdfs.rename(path+file_name, path+rename)
        return 1
    except Exception as e:
        return e

# 下载文件到本地
def load_local(file_name, local_path, user_name, file_flag):
    path = "/spark_user/" + user_name + file_flag + file_name

    try:
        hdfs.download(path, local_path)
        return 1
    except Exception as e:
        return e

# 运行代码
def run_code(user_name, code_name, data_name, memory_size=8, cores_num=1, instance=1):
    from subprocess import Popen
    code_path = "hdfs://192.168.10.75:8020/spark_user/" + user_name + '/code/' + code_name
    data_path = "hdfs://192.168.10.75:8020/spark_user/" + user_name + '/data/' + data_name
    print(code_name, data_name, memory_size, cores_num, instance)
    try:
        pods = []
        for e in v1.list_namespaced_pod(namespace).items:
            pods.append(e.metadata.name)
        if user_name in pods:
            v1.delete_namespaced_pod(name=user_name, namespace=namespace)
        Process = Popen(r'./shell/run.sh %s %s %d %d %s %d' % (code_path, data_path, memory_size, cores_num, user_name, instance), shell=True)
        p = Process.wait()
        return p
    except Exception as e:
        return e

# 获取pod中的日志
def get_result(user_name, code_name):

    logs_tmp = './tmp/' + user_name + '-' + code_name.split('.')[0] + '-result.logs'
    save_path = "/spark_user/" + user_name + '/result/'

    try:
        logs = v1.read_namespaced_pod_log(name=user_name, namespace=namespace)
        v1.delete_namespaced_pod(user_name, namespace)
        with open(logs_tmp, 'w') as fp:
            fp.write(logs)
        hdfs.upload(save_path, logs_tmp, overwrite=True)
        return 1, list(logs.split('\n'))
    except Exception as e:
        return -1, e
def top(user_name):
    import json

    from kubernetes import client, config, watch
    from kubernetes.client.rest import ApiException
    from kubernetes.client.api_client import ApiClient

    config.load_kube_config()
    v1 = client.CoreV1Api()
    api_client = ApiClient()

    podname = user_name
    node_metrics = "/apis/metrics.k8s.io/v1beta1/namespaces/default/pods/" + podname
    response = api_client.call_api(node_metrics,
                                   'GET', auth_settings=['BearerToken'],
                                   response_type='json', _preload_content=False)
    r1 = json.loads(response[0].data.decode('utf-8'))
    mem = r1['containers'][0]['usage']
    with open('t.txt', 'w+') as f:
        f.write(podname + '     cpu:' + mem['cpu'] + "      " + 'memory:' + mem['memory'])
def get_pod(user_name):

    import os
    '''
    ApiToken = "eyJhbGciOiJSUzI1NiIsImtpZCI6Inc1UkM3Q0owV09DbVVjdUIzVEI4Qk15X21nakNxR2pDSDFDS1BuVGtPWTAifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJhZG1pbi11c2VyLXRva2VuLTI5ajZ2Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImFkbWluLXVzZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiJlNDAzNTE1Yi05OWMxLTRjMGYtYWZhMS03MmNhNTRjMTRhZmUiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZS1zeXN0ZW06YWRtaW4tdXNlciJ9.MdMX6kRPOgI2ztLtu_x3pvcwXRzCaQ_w76FeJ0pP9h-h-zfWrQzRxagRK8EIUU83fxRnx5uyTXz9FLZ5AF7lBvLx2o0QUXSozoNNdDE195HeddPbPlNnKk43GBuY0QNxbY0dg3brILzYqanjLFcLWGUx8gaF5riq5W8832bOaTFo08ayP0pVRR2uGFLwSKYhOAbX8ICpKB7DrKYV6NTjM-dcEOUxL2wp5pRxVwqna96gUebo55m0hOM4s9fcT9HdP9OsseVDkHa3hJHmCBO96NWJHzpl2bla4sMYTfZ0kF305LDLRQfXpf0jdNKwZ74H50_KnFRHtFb1C2Gk4Cd__Q"
    configuration = client.Configuration()
    setattr(configuration, 'verify_ssl', False)
    client.Configuration.set_default(configuration)
    configuration.host = "https://192.168.10.75:6443"    #ApiHost
    configuration.verify_ssl = False
    configuration.debug = True
    configuration.api_key = {"authorization": "Bearer " + ApiToken}
    client.Configuration.set_default(configuration)
    v1 = client.CoreV1Api()
    '''

    now2 = datetime.now(timezone.utc)
    config.load_kube_config()
    v1 = client.CoreV1Api()
    namespaces = 'default'
    pod_name = user_name
    ret = v1.read_namespaced_pod(namespace=namespaces, name=pod_name)
    i = ret
    with open('g.txt', 'w+') as f:
        f.write("Pod Start-time Duration\n")
        f.write("%-49s%s\t%sdays\n" % (
        i.metadata.name, i.status.start_time.strftime('%Y-%m-%d'), (now2 - i.status.start_time).days))
    os.popen('hadoop fs -put -f g.txt /spark_user/11923010236/pod')
