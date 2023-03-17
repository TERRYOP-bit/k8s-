import os



def run_spark_app_kube(userID, code_name=None, user_data=None, memory_size=1, cores_num=1):
    from subprocess import Popen
    from hdfs.client import Client
    client = Client("http://192.168.10.22:9870")
    print(client.list('/spark_user/11701080204/code'))

    print("code_path", code_name)
    print("user_data", user_data)


    code_path = "hdfs://192.168.10.22:9000/spark_user/"+ userID + '/code/' + code_name
    data_path = "hdfs://192.168.10.22:9000/spark_user/"+ userID + '/data/' + code_name

    if code_path != -1 and data_path != -1:
        try:
            Process = Popen(r'/home/cyf/Desktop/a.sh %s %s' % (code_path, data_path), shell=True)
            print('success')
            if Process.wait() != 0:
                return -1
        except:
            return -1
    else:
        print("Could not upload user data to hdfs")

    return


# run_spark_app_kube(userID='11701080204', code_name='wordcount.py', user_data='wordcount.py')

