package com

import org.apache.spark.sql.SparkSession


/**
  * @Author: hongbing.li
  * @Date: 29/9/2018 2:15 PM
  *        自定义生成sparkplan的strategy
  */
object SparkPi {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("Spark Pi")
      .getOrCreate()

    spark.sparkContext
      .textFile("/Users/bing/Programs/sbt/conf/sbtconfig.txt")
      .flatMap { line =>
        line.split(" ")
      }.filter(_.length >= 2)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(p => p)
      .reduceByKey(_ + _)
      .map(p => p)
      .reduceByKey(_ + _)
      .map(p => p)
      .saveAsTextFile("/Users/bing/Desktop/output")
    spark.stop()


  }
}
