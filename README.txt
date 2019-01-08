1.先进入到项目的目录
cd (项目的目录)


2.将要统计单词的文件上传到hadoop集群(安装好hadoop集群, 可以jps命令查看)
hdfs dfs -put 本地目标文件 集群目录


3.确保安装好maven(没装好自行百度)
  进行打包
mvn package


3.利用打包好的jar包运行程序, 注意, jar包在target目录下, 注意看上一步的提示信息.
yarn jar jar包的路径 主类的位置(用'.'分隔) 集群的目标输入文件 集群的输出文件
