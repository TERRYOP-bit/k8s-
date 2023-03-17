from kubernetes import client, config
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
from datetime import datetime, timezone
from datetime import *
now2= datetime.now(timezone.utc)
config.load_kube_config()
v1 = client.CoreV1Api()
namespaces='default'
pod_name='mysql-bck7r'  
ret = v1.read_namespaced_pod(namespace=namespaces,name=pod_name)
i=ret
with open ('g.txt','w+') as f:
    f.write("Pod Start-time Duration\n")
    f.write("%-49s%s\t%sdays\n" % (i.metadata.name,i.status.start_time.strftime('%Y-%m-%d'),(now2-i.status.start_time).days))
os.popen('hadoop fs -put -f g.txt /spark_user/11923010236/pod')
