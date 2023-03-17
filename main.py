import pyhdfs
from datetime import timedelta
from flask import *
from hdfs_opteration import list_dir_file, uploading_file, see_file, delete_file, rename_file, load_local, run_code, get_result,top,get_pod


app = Flask(__name__)
client = pyhdfs.HdfsClient(hosts='192.168.10.75, 8020', user_name='root')



@app.route('/', methods=['GET', 'POST'])
def login():

    return render_template('login.html')  

@app.route('/operation', methods=['GET', 'POST'])
def operation():
    if request.method == 'POST':
        result = request.form

        print('操作界面的信息：', result)
        if 'back_operation' in request.form:
            return render_template('operation.html', user_name=session['user_name'])
        else:
            user_name = result['user_name']
            session["user_name"] = user_name
            print('用户名：', user_name)
            return render_template('operation.html', user_name=user_name)


@app.route('/operation/code', methods=['GET', 'POST'])
def code():
    if request.method == 'POST':
        print('代码文件界面框的信息：', request.form)
        file_flag = '/code/'
        if 'code' in request.form:
            files = list_dir_file(session['user_name'], file_flag)
            print('代码界面的文件：',files)
            return render_template("code.html", files=files)
        elif 'uploading' in request.form:
            flag = uploading_file(request.form['file_name'], session['user_name'], file_flag)
            if flag == 1:
                files = list_dir_file(session['user_name'], file_flag)
                return render_template("code.html", files=files)
            else:
                return render_template("error_show.html", data=flag)
        elif 'see' in request.form:
            flag, data = see_file(request.form['file_name'], session['user_name'], file_flag)
            if flag == 1:
                return render_template("show.html", data=data)
            else:
                return render_template("error_show.html", data=data)
        elif 'delete' in request.form:
            flag = delete_file(request.form['file_name'], session['user_name'], file_flag)
            if flag == 1:
                files = list_dir_file(session['user_name'], file_flag)
                return render_template("code.html", files=files)
            else:
                return render_template("error_show.html", data=flag)

        elif 'rname' in request.form:
            flag = rename_file(request.form['file_name'], request.form['file_rname'], session['user_name'], file_flag)
            if flag == 1:
                files = list_dir_file(session['user_name'], file_flag)
                return render_template("code.html", files=files)
            else:
                return render_template("error_show.html", data=flag)



@app.route('/operation/data', methods=['GET', 'POST'])
def data():
    if request.method == 'POST':
        file_flag = '/data/'
        print('数据界面框的信息：', request.form)
        if 'data' in request.form:
            datas = list_dir_file(session['user_name'], file_flag)
            print('数据界面的文件：', datas)
            return render_template("data.html", datas=datas)
        elif 'uploading' in request.form:
            print('需要上传的数据文件：', request.form['data_name'])
            flag = uploading_file(request.form['data_name'], session['user_name'], file_flag)
            if flag == 1:
                datas = list_dir_file(session['user_name'], file_flag)
                return render_template("data.html", datas=datas)
            else:
                return render_template("error_show.html", data=flag)

        elif 'see' in request.form:
            flag, datas = see_file(request.form['data_name'], session['user_name'], file_flag)
            if flag == 1:
                return render_template("show.html", data=datas)
            else:
                return render_template("error_show.html", data=data)

        elif 'delete' in request.form:
            flag = delete_file(request.form['data_name'], session['user_name'], file_flag)
            if flag == 1:
                files = list_dir_file(session['user_name'], file_flag)
                return render_template("data.html", datas=files)
            else:
                return render_template("error_show.html", data=flag)

        elif 'rname' in request.form:
            flag = rename_file(request.form['data_name'], request.form['data_rname'], session['user_name'], file_flag)
            if flag == 1:
                files = list_dir_file(session['user_name'], file_flag)
                return render_template("data.html", datas=files)
            else:
                return render_template("error_show.html", data=flag)


@app.route('/operation/result', methods=['GET', 'POST'])
def result():
    if request.method == 'POST':
        print('结果界面框的信息：', request.form)
        file_flag = '/result/'
        if 'result' in request.form:
            results = list_dir_file(session['user_name'], file_flag)
            print(results)
            return render_template("result.html", results=results)
        elif 'loading' in request.form:
            flag = load_local(request.form["result_name"], request.form["local_path"], session['user_name'], file_flag)
            if flag == 1:
                results = list_dir_file(session['user_name'], file_flag)
                return render_template("result.html", results=results)
            else:
                return render_template("error_show.html", data=flag)

        elif 'see' in request.form:
            flag, datas = see_file(request.form['result_name'], session['user_name'], file_flag)
            if flag == 1:
                return render_template("show.html", data=datas)
            else:
                return render_template("error_show.html", data=data)

        elif 'delete' in request.form:
            flag = delete_file(request.form['result_name'], session['user_name'], file_flag)
            if flag == 1:
                files = list_dir_file(session['user_name'], file_flag)
                return render_template("result.html", results=files)
            else:
                return render_template("error_show.html", data=flag)

        elif 'rname' in request.form:
            flag = rename_file(request.form['result_name'], request.form['result_rname'], session['user_name'], file_flag)
            if flag == 1:
                files = list_dir_file(session['user_name'], file_flag)
                return render_template("result.html", results=files)
            else:
                return render_template("error_show.html", data=flag)


@app.route('/operation/run', methods=['GET', 'POST'])
def run():
    if request.method == 'POST':
        result = request.form
        print('运行代码界面框的信息：', result)
        if '运行' in request.form:
            return render_template('run.html')
        elif 'run_code' in request.form:
            code_name = result['code_name']
            data_name = result['data_name']
            memory = result['memory']
            core = result['core']
            instance = result['instance']
            run_code(session['user_name'], code_name, data_name, int(memory), int(core), int(instance))
            flag, result = get_result(session['user_name'], code_name,)
            print(flag)
            print('日志文件：', result)
            if flag == 1:
                print(result)
                return render_template("show.html", data=result)
            else:
                return render_template("error_show.html", data=result)
@app.route('/operation/kubernetes', methods=['GET', 'POST'])
def kubernetes():
    import  os
    top(session['user_name'])
    os.popen('/root/spark/flask_spark/top.sh')
    get_pod(session['user_name'])
    file_flag='/pod/'
    if request.method == 'POST':
        result = request.form
        print('操作界面的信息：', result)
        if 'kubernetes' in request.form:
            return render_template('kubernetes.html')
        elif 'top' in request.form:

            flag, data = see_file('t.txt', '11923010236', '/pod/')
            if flag == 1:
                return render_template("show.html", data=data)
            else:
                return render_template("error_show.html", data=data)
        elif 'get' in request.form:
            flag, data = see_file('g.txt', '11923010236', '/pod/')
            if flag == 1:
                return render_template("show.html", data=data)
            else:
                return render_template("error_show.html", data=data)

class Config(object):
    SECRET_KEY = "DJFAJLAJAFKLJQ"


if __name__ =="__main__":
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(seconds=1)
    app.config.from_object(Config())
    app.run(debug=True, port=30000, host='192.168.10.75')



