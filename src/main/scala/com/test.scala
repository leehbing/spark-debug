package com

import org.apache.spark.sql.SparkSession


/**
  */
object test {
  def main(args: Array[String]) {
    val it = Iterator("Baidu", "Google", "Runoob", "Taobao")

    while (it.hasNext){
      println(it.next())
    }


  }
}
