package AR

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object Main {

  def main(args:Array[String]){

    // 设置默认spark参数
    var spark_memory_fraction = "0.7"
    var spark_memory_storage_Fraction = "0.3"
    var spark_shuffle_spill_compress = "true"
    var spark_memory_offHeap_enable = "true"
    var spark_memory_offHeap_size = "5g"

    var spark_executor_cores_AB = "2"
    var spark_cores_max_AB = "42"

    var spark_executor_cores_CD = "8"
    var spark_cores_max_CD = "168"
    var spark_parallelism_CD = "672"

    var spark_executor_instances = "21"
    var spark_driver_cores = "24"
    var spark_driver_memory = "20g"
    var spark_executor_memory_AB = "20g"
    var spark_executor_memory_CD = "20g"

    // 实例化conf对象
    val conf = new SparkConf().setAppName("FPGrowth")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.memory.fraction", spark_memory_fraction)
    conf.set("spark.memory.storageFraction", spark_memory_storage_Fraction)
    conf.set("spark.shuffle.spill.compress", spark_shuffle_spill_compress)
    conf.set("spark.memory.offHeap.enable", spark_memory_offHeap_enable)
    conf.set("spark.memory.offHeap.size", spark_memory_offHeap_size)
    conf.set("spark.executor.memory", spark_executor_memory_AB)
    conf.set("spark.driver.cores", spark_driver_cores)
    conf.set("spark.driver.memory", spark_driver_memory)
    conf.set("spark.executor.instances", spark_executor_instances)
    conf.set("spark.cores.max", spark_cores_max_AB)
    conf.set("spark.executor.cores", spark_executor_cores_AB)

    // 将参数传入sparkcontext
    val sc = new SparkContext(conf)

    // 命令行参数
    val input_path=args(0)
    val output_path=args(1)
    val tmp_path = args(2)

    // 最小支持度
    val minSupport=0.092
    // 最小置信度
    val minConfidence=0.8
    // 数据分区
    val numPartitions=336


    // 取出数据
    val data_D = sc.textFile(input_path + "/D.dat", numPartitions)
    val data_U = sc.textFile(input_path + "/U.dat", numPartitions)
    // 把数据通过空格分割
    val purchase = data_D.map(x=>x.split(" "))
    val users = data_U.map(x=>x.split(" "))

    // 创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()
    // 设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)

    // 把数据带入算法中
    val model = fpg.run(purchase)

    // 查看所有的频繁项集，并且列出它出现的次数
    model.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK_SER)
    model.freqItemsets.saveAsTextFile(output_path + "/Freq")
    val freqItems = model.freqItemsets.collect()

    // 通过置信度筛选出推荐规则则
    model.generateAssociationRules(minConfidence).saveAsTextFile(output_path + "/Rules")

    // 根据用户数据推荐商品
    val userList = users.collect()
    val recList = ListBuffer[String]()
    for(user <- userList){
      // 对每个用户，获取包含其购买的产品项集的频度
      var goodFreq = 0L
      for(goods <- freqItems){
        if(goods.items.mkString == user.mkString){
          goodFreq = goods.freq
        }
      }
      var preConf = 0D
      // 推荐结果初始化为 0
      var rec = "0"
      // 对频繁项集中的每个集合计算推荐分值
      for(f <- freqItems){
        // 若项集包含用户购买的商品，则推荐结果从其中产生
        if(f.items.mkString.contains(user.mkString) && f.items.size > user.size){
          var conf:Double = f.freq.toDouble / goodFreq.toDouble
          // 若推荐分值大于此前的最大推荐分值，则更新最大推荐分值和推荐结果
          if(conf >= preConf) {
            preConf = conf
            var item = f.items
            // 过滤用户购买过的商品
            for(i <- 0 until user.size){
              item = item.filter(_ != user(i)) 
            }
            // 将推荐商品列表转化为字符串
            rec = item.mkString(" ")
          }
        }
      }
      // 将推荐结果存入列表中
      recList += rec
    }
    // 将列表转化为RDD对象并保存至文件
    sc.parallelize(recList).saveAsTextFile(output_path + "/Rec")
  }

}
