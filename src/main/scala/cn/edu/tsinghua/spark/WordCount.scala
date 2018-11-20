package cn.edu.tsinghua.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // init spark context
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)

    // read file
    val textRdd = sc.textFile("pom.xml")

    // map reduce
    val result = textRdd.map(word => (word, 1)).reduceByKey(_+_)

    // save result
    result.saveAsTextFile("output")
  }

}
