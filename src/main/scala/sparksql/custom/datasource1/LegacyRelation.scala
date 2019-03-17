package sparksql.custom.datasource1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._

/**
  * Created by anicolaspp on 1/8/16.
  * BaseRelation代表一个已知的schema
  * 混合TableScan，为了可以读取我们的数据源
  */
class LegacyRelation(location: String, userSchema: StructType)(@transient val sqlContext: SQLContext)
  extends BaseRelation
      with TableScan with Serializable {

  override def schema: StructType = { //继承BaseRelation的schema方法，建立我们数据的schema，
    if (this.userSchema != null) {
      return this.userSchema
    }
    else { //比如如果这里文件是csv格式，可以根据文件的headers推断出schema
      return StructType(Seq(StructField("name", StringType, nullable = true), StructField("age", IntegerType, nullable = true)))
    }
  }

  private def castValue(value: String, toType: DataType) = toType match {
    case _: StringType      => value
    case _: IntegerType     => value.toInt
  }

  override def buildScan(): RDD[Row] = { //继承TableScan的该方法，来读取我们的数据源，返回所有的rows，这个例子中每个row就是一个文件
    val schemaFields = schema.fields
    val rdd = sqlContext.sparkContext.wholeTextFiles(location).map(x => x._2) //wholeTextFiles，一整个文件一行

    val rows = rdd.map(file => {
      val lines = file.split("\n")

      val typedValues = lines.zipWithIndex.map{
        case (value, index) => {
          val dataType = schemaFields(index).dataType
          castValue(value, dataType)
        }
      }

      Row.fromSeq(typedValues)
    })

    rows
  }
}

object LegacyRelation {
  def apply(location: String, userSchema: StructType)(sqlContext: SQLContext): LegacyRelation =
    return new LegacyRelation(location, userSchema)(sqlContext)
}
