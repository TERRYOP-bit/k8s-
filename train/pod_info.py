from kubernetes import *
from config_update import update_config
from pprint import pprint
from kubernetes.client.rest import ApiException


class pods_info(update_config):
    def __init__(self):
        self.error_flag = -1
        self.success_flag = 1

    def delete_user_app_pods(self, app_name, userID, pod_namespace = "default"):
        try:
            config.load_kube_config()
            v1 = client.CoreV1Api()
            print("Listing pods with their IPs:")
            ret = v1.list_namespaced_pod(pod_namespace)
        except:
            print("collecting pod info fails")
            return self.error_flag

        for pod in ret.items:
            pod_name = pod.metadata.name
            if userID in pod_name and app_name in pod_name:
                try:
                    v1.delete_namespaced_pod(namespace=pod_namespace, name=pod_name)
                except:
                    print("pod has been deleted")
        return self.error_flag


    def delete_user_pod(self, pod_name):
        try:
            config.load_kube_config()
            k8s_apps_v1 = client.CoreV1Api()
            k8s_apps_v1.delete_namespaced_pod(namespace="default", name=pod_name)
        except:
            print("pod has been deleted")
            return self.error_flag
        return self.success_flag

    def get_app_driver_logs(self, app_name, userID, pod_namespace = "default"):
        try:
            config.load_kube_config()
            v1 = client.CoreV1Api()
            print("Listing pods with their IPs:")
            ret = v1.list_namespaced_pod(pod_namespace)
        except:
            print("collecting pod info fails")
            return self.error_flag

        # a spark app has one driver pod
        for pod in ret.items:
            pod_name = pod.metadata.name
            if userID in pod_name and app_name in pod_name and 'driver' in pod_name:
                try:
                    api_response = v1.read_namespaced_pod_log(pod_name, pod_namespace)
                    pprint(api_response)
                    return (pod_name,api_response)
                except ApiException as e:
                    print("Exception when calling CoreV1Api->read_namespaced_pod_log: %s\n" % e)
                    return (pod_name, "Exception when calling CoreV1Api->read_namespaced_pod_log: %s\n" % e)

    def collect_pods_info(self, userID):
        ret = self.collect_allpods_info()
        if ret == self.error_flag:
            print("could not get pods' info")
            return ret
        else:
            (pod_ip_dict, pod_status_dict, pod_host_ip_dict) = ret
            if userID in pod_ip_dict.keys():
                return (pod_ip_dict[userID], pod_status_dict[userID], pod_host_ip_dict[userID])
            else:
                print("pods or service for %s don't exist." % (userID))
                return self.error_flag

    def collect_allpods_info(self):
        try:
            config.load_kube_config()
            v1 = client.CoreV1Api()
            print("Listing pods with their IPs:")
            ret = v1.list_namespaced_pod("default")
        except:
            print("collecting pod info fails")
            return self.error_flag

        pod_ip_dict = {}
        pod_status_dict = {}
        pod_host_ip_dict = {}
        pod_name_dict = {}
        for pod in ret.items:
            #print(pod)
            # print(pod.metadata)
            # print(pod.status)
            # pod.status.conditions should save to logs
            #print("%s\t%s\t%s\t%s\t%s\t%s" %(pod.status.pod_ip, pod.metadata.namespace, pod.metadata.name, pod.status.phase, pod.status.host_ip, pod.status.conditions))
            print("pod name,conditions:%s\t%s", pod.metadata.name, pod.status.conditions)
            for condition in pod.status.conditions:
                print(condition)
                print(condition.type)
                print(condition.last_transition_time)
            pod_ip = pod.status.pod_ip
            pod_status = pod.status.phase
            pod_name = pod.metadata.name
            pod_host_ip = pod.status.host_ip
            if pod_status != "Running":
                print(pod_name, "doesn't work yet")

            parts = pod_name.split('-')
            userID = parts[0]
            if userID not in pod_ip_dict.keys():
                pod_ip_dict[userID] = {}
                pod_status_dict[userID] = {}
                pod_host_ip_dict[userID] = {}
                pod_name_dict[userID] = []
                # remove userID in the pod name
            hostname_userID = pod_name.replace('-' + userID, '')
            pod_ip_dict[userID][hostname_userID] = pod_ip
            pod_status_dict[userID][hostname_userID] = pod_status
            pod_host_ip_dict[userID][hostname_userID] = pod_host_ip
            pod_name_dict[userID].append(pod_name)

        for userID in pod_status_dict.keys():
            flag = True
            for hostname in pod_status_dict[userID].keys():
                status = pod_status_dict[userID][hostname]
                if status == 'Running':
                    continue
                else:
                    flag = False

            if flag is True:
                print("ok")
        # print(pod_ip_dict)

        return (pod_ip_dict, pod_status_dict, pod_host_ip_dict, pod_name_dict)


def get_pods_info(userID):
    m = pods_info()
    (pod_ip_dict, pod_status_dict, pod_host_ip_dict) = m.collect_pods_info(userID)
    print("pod ip, pod status, host ip")
    for userID in pod_ip_dict.keys():
        print(pod_ip_dict[userID], pod_status_dict[userID], pod_host_ip_dict[userID])


if __name__ == '__main__':
    userID = "abc"
    m = pods_info()
    #m.delete_user_app_pods("abc", "wordcounthdfs")
    #m.get_app_driver_logs(userID, "wordcount","default")
    m.collect_allpods_info()
    # get_pods_info(userID)
    # create_user_hadoop_pods(dest_dir,userID,service_kind)
    # time.sleep(60)
    # delete_user_service("hadoop-master-0111")
    # delete_user_pod("hadoop-master-0111-0101")
    # delete_user_pod("hadoop-master-0111-0101-0101")
    # delete_user_pod("hadoop-slave1-0111-0101")
    # delete_user_pod("hadoop-slave1-0111-0101-0101")
    # delete_user_resources("0101")
    # delete_user_resources(dest_dir, "0105")
