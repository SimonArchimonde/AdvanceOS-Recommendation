package AR.main

import AR.conf.Conf
import AR.util.AssociationRules.RuleNewDef
import AR.util.{FPModel, FPNewDef}
import AR.conf.Conf
import AR.util.AssociationRules.RuleNewDef
//import pasa.nju.ARMining.util.AssociationRules.ConSeq
import FPNewDef.FreqItemset
import FPNewDef.WrapArray

//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
//import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel, FPModel, FPNewDef}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks

object FP_Growth {

  def total(myConf: Conf, conf: SparkConf): Unit = {
    val partitionNum = myConf.numPartitionAB //336
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.memory.fraction", myConf.spark_memory_fraction)
    conf.set(
      "spark.memory.storageFraction",
      myConf.spark_memory_storage_Fraction
    )
    conf.set(
      "spark.shuffle.spill.compress",
      myConf.spark_shuffle_spill_compress
    )
    conf.set("spark.memory.offHeap.enable", myConf.spark_memory_offHeap_enable)
    conf.set("spark.memory.offHeap.size", myConf.spark_memory_offHeap_size)
    conf.set("spark.executor.memory", myConf.spark_executor_memory_AB)
    conf.set("spark.driver.cores", myConf.spark_driver_cores)
    conf.set("spark.driver.memory", myConf.spark_driver_memory)
    conf.set("spark.executor.instances", myConf.spark_executor_instances)
    conf.set("spark.cores.max", myConf.spark_cores_max_AB)
    conf.set("spark.executor.cores", myConf.spark_executor_cores_AB)
    conf.registerKryoClasses(Array(classOf[FreqItemset], classOf[RuleNewDef]))
    val sc = new SparkContext(conf)

    val data = sc.textFile(myConf.inputFilePath + "/D.dat", partitionNum)
    val transactions = data.map(s => s.trim.split(' ').map(f => f.toInt))

    val fp = new FPNewDef() //FPGrowth()
      .setMinSupport(0.092) // 0.092
      .setNumPartitions(partitionNum)
    val fpgrowth = fp.run(transactions)
    fpgrowth.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK_SER)
    genFreSortBy(myConf.outputFilePath + "/Freq", fpgrowth)
    sc.stop()
  }

  /* Sort frequentItemSet by RDD.SortBy
    * @param outPath  save frequentItemSet file path
    * @param model generated FPGrowthModel
    * test pass...
    */
  def genFreSortBy(outPath: String, model: FPModel) = {
    val freqUnsort =
      model.freqItemsets //.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val freqSort = freqUnsort
      .map(itemset => s"${itemset.items.mkString(" ")}: ${itemset.freq}")
      .sortBy(f => f)
    println("frequentItemSet count:", freqSort.count())
    //save
    freqSort.saveAsTextFile(outPath)
  }

  /* generate rules sorted and save the result
    * @param outPath   save rule as objectfile file path
    * @param model  generated FPGrowthModel
    * test pass...
    */
  def genRules(outPath: String, model: FPModel) = {
    val assRules = model.generateAssociationRules(
      0.8
    )
    println("Associated Rules count", assRules.count())
    //save
    assRules.saveAsObjectFile(outPath)
  }

}
