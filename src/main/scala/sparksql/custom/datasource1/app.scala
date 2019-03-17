package sparksql.custom.datasource1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anicolaspp on 1/8/16.
  * https://mapr.com/blog/spark-data-source-api-extending-our-spark-sql-query-engine/
  *
  *
  * Spark already has some standard structures built in such as Avro and Parquet,
  * yet third parties have created new readers for CSV, JSON and others by extending this API. Today we are going to create our own.
  *
  *
  * 可以读取自己的格式，并将当前的数据源变成可以方便使用的数据源
  * datasource： 百万个文件，每个文件描述了一个用户的信息，文件中每行就是一个字段，比如某个文件内容：
  *   pepe		//用户名
  *   20			//age
  *   Miami		//live place
  *   Cube		//born place
  * 利用sparksql来查询这些文件内容
  *
  *
  *
  *
  * 取名为DefaultSource的原因可以看RelationProvider源码的注释，针对每一种不同的格式，比如jdbc，json，parquet，在org.apache.spark.sql.execution.datasources.jdbc/json/parquet下面都有DefaultSource类，1.5.1版本的spark是这样
  *
  *
  * The Data Source API   reading data / write it in a custom format
  *
  *
  */
object app {




  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("testing provider").setMaster("local[2]")

    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)

    println("Here")

    val df = sqlContext.read
      .format("sparksql.custom.datasource") //会找这个包下面的DeaultSource类
      .load("/Users/bing/Documents/projects/helloworld/HelloWord2/data/input") //.load("/Users/nperez/person.dat")

    df.printSchema() //不管有没有数据，这里都可以打印自定义的schema
    //root
    // |-- name: string (nullable = true)
    // |-- age: integer (nullable = true)
    df.show()

    val join = df.join(df, "name")

    join.show()

    df.write.format("sparksql.custom.datasource").save("/Users/bing/Documents/projects/helloworld/HelloWord2/data/output")
    //pepe,20
    //lihongbing,28
    //yangsiwei,27
  }
}
