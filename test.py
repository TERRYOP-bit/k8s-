from datetime import datetime, timezone
from datetime import *
from kubernetes import client, config
import os
now2= datetime.now(timezone.utc)
config.load_kube_config()
v1 = client.CoreV1Api()
ret = v1.list_namespaced_pod('default',label_selector='mysql')
print(ret)
