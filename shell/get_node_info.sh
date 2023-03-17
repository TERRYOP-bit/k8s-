#!/bin/bash

dir_path=/root/PycharmProjects/cyf/flask_spark/shell/

kubectl get nodes | grep -v NAME | awk '{print $1}' >> ${dir_path}node.txt




