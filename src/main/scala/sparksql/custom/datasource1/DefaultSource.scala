package sparksql.custom.datasource1

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType


/**
  * Created by anicolaspp on 1/8/16.
  * RelationProvider produce relations for a specific kind of data source
  * SchemaRelationProvider allows us to create the schema that we want
  *
  * create a class named DefaultSource and Spark will look for it in a given package.
  * The DefaultSource class will extend RelationProvider and mix SchemaRelationProvider
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {//RelationProvider的方法
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))

    return LegacyRelation(parameters.get("path").get, schema)(sqlContext)
  }

  def saveAsCsvFile(data: DataFrame, path: String) = {
    val dataCustomIterator = data.rdd.map(row => {
      val values = row.toSeq.map(value => value.toString)

      values.mkString(",")
    })

    dataCustomIterator.saveAsTextFile(path)
  }

  //CreatableRelationProvider的方法，保存数据时用的的schema
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    saveAsCsvFile(data, parameters.get("path").get)

//
//    val canSaveData = mode match {
//      case SaveMode.Append => sys.error("Append mode is not supported")
//      case SaveMode.Overwrite =>
//    }

    createRelation(sqlContext, parameters, data.schema)
  }
}
