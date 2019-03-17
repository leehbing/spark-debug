package com.leehbing;

import com.cobub.analytics.jobs.testutil.{SparkLocal, UnitTestSuite}
import org.apache.spark.HashPartitioner



/**
 * @Author: hongbing.li
 * @Date: 26/11/2018 10:25 AM
 */
class Test extends UnitTestSuite with SparkLocal{

    before {
        startSparkSession()
    }

    after {
        stopSparkSession()
    }

    test("partitionBy") {
        val rdd = spark.sparkContext.parallelize(Array(
            ("user1",1),("user2",1),("user3",1),("user4",1),("user5",1)
            ,("user1",1),("user2",1),("user3",1),("user4",1),("user5",1)
            ,("user1",1),("user2",1),("user3",1),("user4",1),("user5",1)
            ,("user1",1),("user2",1),("user3",1),("user4",1),("user5",1)))
        val res1 = rdd.repartition(4).mapPartitionsWithIndex{
            (partIdx,iter) => {
                val part_map = scala.collection.mutable.Map[String, List[(String,Int)]]()
                while(iter.hasNext){
                    val part_name = "part_" + partIdx
                    var elem = iter.next()
                    if(part_map.contains(part_name)) {
                        var elems = part_map(part_name)
                        elems ::= elem
                        part_map(part_name) = elems
                    } else {
                        part_map(part_name) = List[(String,Int)]{elem}
                    }
                }
                part_map.iterator
            }
        }.collect
        res1.foreach(x=>{
            println("::" + x._1)
            println(x._2)
        })


        rdd.partitionBy(new HashPartitioner(4)).glom().collect().foreach(x=>{
            println(x.toList)

        })
    }

    test("distinct") {
        val rdd = spark.sparkContext.parallelize(Array(
            "10012" -> ("d1","u1"),
            "10012" -> ("d1","u1"),
            "10012" -> ("d1","u1"),
            "10012" -> ("d1","u2"),
            "10012" -> ("d1","u2"),
            "10012" -> ("d1","u3"),
            "10013" -> ("d1","u3"),
            "10012" -> ("d1",""),
            "10014" -> ("d1",""),
            "10014" -> ("d1","u1"),
            "10014" -> ("d1","u1")))
          rdd.distinct().collect().foreach(println(_))




    }
}