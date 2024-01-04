## Personal Info
何芷倩 112598401 資訊所碩一

## Environment and Spec
1) Virtual Box VM Ubuntu20.04LTS
2) 2 nodes in total: 
    a) 8 processors for master node, 4 processors for worker node
    b) Memory: both are 20GB

## Versions:
1) Spark: 3.2.4
2) Scala: 2.12.11
3) sbt: 1.9.6
4) Java: 1.8.0_382

> To set up the spark scala environment using sbt build tool, please see [here](https://dboyliao.medium.com/spark-%E9%96%8B%E7%99%BC-vscode-%E8%88%87-sbt-a9a453d85d51).

To run this project with sbt build tool:
`$sbt run`

To run this project in cluster mode:
1) Start hadoop: `/opt/hadoop/sbin/start-all.sh`
2) Start spark (for web ui only): `/opt/spark/sbin/start-all.sh`
3) Get jar file from current project: `sbt package`
4) Submit file to hadoop hdfs: 
    a) Create the directory in HDFS if not existed: HDFS dfs -mkdir -p hdfs://<spark-master-ip>:9000/user/username/
    b) Copy file to HDFS: hdfs dfs -copyFromLocal /absolute/path/to/input/file hdfs://<spark-master-ip>:9000/user/username
    c) To see whether file exists: hdfs dfs -ls hdfs://<spark-master-ip>:9000/user/username
5) Submit to spark: `/path/to/spark/bin/spark-submit --master spark://<spark-master-ip>:7077 /path/to/your/project.jar  --deploy-mode cluster --driver-memory 8G --executor-memory 8G`

To view the project status in spark web ui please visit *http://<spark-master-ip>:8080*

Please place the input file at the outermost layer of the project's directory.
All csv files should be placed in a folder named "Data".
After running the project with *sbt run*, 2 folders named "target" will be produced.
```
├─project
│  └─build.properties
│  └─*target*
├─src
│  └─main
│      └─scala
├─*target*
├─.sbtopts
├─build.sbt
├─*Data*
├─output.txt
```

## Generated Output
1) (Q1) : ![(output1)](Q1.png)
2) (Q2) : ![(output2a)](Q2a.png)![(output2b)](Q2b.png)![(output2c)](Q2c.png)
3) (Q3) : ![(output3a)](Q3a.png)![(output3b)](Q3b.png)
4) (Q4) : ![(output4)](Q4.png)
5) (Q5) : ![(output5)](Q5.png)
6) Cluster mode execution time:  ![BDMA_HW4](BDMA_HW4.png)

## Homework information
1) input file: [MovieLens 1M Dataset](http://grouplens.org/datasets/movielens/1m/) from grouplens.