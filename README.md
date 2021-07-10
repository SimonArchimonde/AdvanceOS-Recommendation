# 高级操作系统大作业赛题一

### 1.小组成员

SA20221053 李林枫

SA20221901 朱池苇

### 2.项目环境

spark-3.1.2-bin-hadoop3.2

openjdk 1.8.0_292

### 3.项目结构

​	本项目用scala语言编写，用maven组织。代码结构如下。
- 项目src/main/AR目录下存放源代码文件。
- main文件夹中存放频繁项集挖掘与关联规则生成与关联规则匹配与推荐分值计算这两个模块的代码。
- util包里FPTree、AssociationRules是频繁项集挖掘所必须的数据结构，FPNewDef是基于mllib的FP-Growth算法的优化版本。
- conf文件夹包含一个Conf类用于程序运行参数配置。

~~~shell
|____src
| |____main
| | |____resources
| | |____scala
| | | |____AR
| | | | |____Main.scala                 # main函数
| | | | |____main
| | | | | |____FPGrowth.scala           # 频繁项集挖掘与关联规则生成
| | | | | |____RecPartUserRdd.scala     # 关联规则匹配与推荐分值计算
| | | | |____conf
| | | | | |____Conf.scala                  # 配置参数类
| | | | |____util
| | | | | |____AssociationRules.scala     # 关联规则实体类 
| | | | | |____FPTree.scala          
| | | | | |____FPNewDef.scala             # PFP基于spark源码优化算法
~~~

### 4.实验说明

#### 4.1.并行化设计思路和方法 

#### 4.2.详细的算法设计与实现 

#### 4.3.实验结果与分析

##### 4.3.1.正确性

##### 4.3.2.性能

#### 4.4.程序代码说明 

##### 4.4.1.编译方法

使用maven编译打包本项目。编译打包命令为：

```shell
mvn clean install
```

##### 4.4.2.运行命令

　　运行脚本如下所示，由于赛题中只给出了一个输入路径参数和一个输出路径参数，因此代码中读取输入路径为`input+'/D.dat'`和`input+'/U.dat'`，所以在输入路径文件夹中需要包含D.dat和U.dat文件。代码中输出路径为`output+'/Frep'`和`output+'/Rec'`。临时工作目录用于存储关联规则。（若如下脚本不能运行，请将参数配置部分的"."改为“-”，如“executor-memory”改为“executor.memory”）

```shell
${SPARK_HOME/bin}/spark-submit \
--master <test spark cluster master uri> \
--class AR.Main\
--executor-memory 20G \
--driver-memory 20G \
<your jar file path> \
hdfs://<输入文件夹路径> \
hdfs://<结果输出文件(夹)路径> \
hdfs://<临时工作目录路径>
```

