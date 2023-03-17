var files = ['a.py', 'b.py', 'c.py', 'd.py','e.py', 'f.py', 'cg.py', 'dd.py'];


// 首页
function show_home_page() {
    document.getElementById('right_home_page').style.display = 'block';
    document.getElementById('right_code').style.display = 'none';
    document.getElementById('right_data').style.display = 'none';
    document.getElementById('right_result').style.display = 'none';

}

// 代码操作页面
function show_code_file() {
    document.getElementById('right_home_page').style.display = 'none';
    document.getElementById('right_code').style.display = 'block';
    document.getElementById('right_data').style.display = 'none';
    document.getElementById('right_result').style.display = 'none';
    tab = document.getElementById("table_code");
    tab.innerHTML = '';
    if(files.length !==0){
        var tbody='';
        for(var row =0; row < files.length; row++){
            tbody += "<tr>" +
                "<td>"+ files[row] +"</td>"+
                `<td><input type="submit" value="查看"/></td>`+
                `<td><button onclick='modify(${row})'>删除</button></td>`+
                `<td><button onclick='rename(${row})'>重命名</button></td>`+
                "</tr>"
        }
        tab.innerHTML+=tbody;
    }
}

// 数据操作页面
function show_data_file() {
    document.getElementById('right_home_page').style.display = 'none';
    document.getElementById('right_code').style.display = 'none';
    document.getElementById('right_data').style.display = 'block';
    document.getElementById('right_result').style.display = 'none';
    tab = document.getElementById("table_data");
    tab.innerHTML = '';
    if(files.length !==0){
        var tbody='';
        for(var row =0; row < files.length; row++){
            tbody += "<tr>" +
                "<td>"+ files[row] +"</td>"+
                `<td><button class='operation_button'>查看</button></td>`+
                `<td><button class='operation_button'>删除</button></td>`+
                `<td><button class='operation_button'>重命名</button></td>`+
                "</tr>"
        }
        tab.innerHTML+=tbody;
    }
}

// 结果操作页面
function show_result_file() {
    document.getElementById('right_home_page').style.display = 'none';
    document.getElementById('right_code').style.display = 'none';
    document.getElementById('right_data').style.display = 'none';
    document.getElementById('right_result').style.display = 'block';
    tab = document.getElementById("table_result");
    tab.innerHTML = '';
    if(files.length !==0){
        var tbody='';
        for(var row =0; row < files.length; row++){
            tbody += "<tr>" +
                "<td>"+ files[row] +"</td>"+
                `<td><button class='operation_button' onclick='see(${row})'>查看</button></td>`+
                `<td><button class='operation_button' onclick='modify(${row})'>删除</button></td>`+
                `<td><button class='operation_button' onclick='rename(${row})'>重命名</button></td>`+
                `<td><button class='operation_button' onclick='DownLoad(${row})'>下载</button></td>`+
                "</tr>"
        }
        tab.innerHTML+=tbody;
    }
}

