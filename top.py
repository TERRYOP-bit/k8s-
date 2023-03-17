
import re
import json

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from kubernetes.client.api_client import ApiClient


config.load_kube_config()
v1 = client.CoreV1Api()
api_client = ApiClient()

podname='mysql-bck7r'
node_metrics = "/apis/metrics.k8s.io/v1beta1/namespaces/default/pods/" +podname
response = api_client.call_api(node_metrics,
                                   'GET', auth_settings=['BearerToken'],
                                   response_type='json', _preload_content=False)
r1 = json.loads(response[0].data.decode('utf-8'))
mem=r1['containers'][0]['usage']   
with open ('t.txt','w+') as f:
    f.write(podname+'     cpu:'+mem['cpu']+"      "+'memory:'+mem['memory'])                               