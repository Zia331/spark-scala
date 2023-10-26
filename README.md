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

To run this project in cluster moded:
`$/path/to/spark/bin/spark-submit --master spark://<spark-master-ip>:7077 /path/to/project.jar --deploy-mode cluster`

Please place the input file at the outermost layer of the project's directory.
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
├─*input_file.txt*
├─output.txt
```

## Generated Output
Folders according to question number will be generated (Q1, Q2, ...).

## Homework information
1) input file: [Space News Dataset](https://www.kaggle.com/datasets/patrickfleith/space-news-dataset) from Kaggle
