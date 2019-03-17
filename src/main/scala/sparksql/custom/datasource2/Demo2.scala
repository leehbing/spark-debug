package sparksql.custom.datasource2

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SQLContext, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Range}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{ProjectExec, RangeExec, SparkPlan}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  *
  * https://gist.github.com/marmbrus/f3d121a1bc5b6d6b57b9
  * a simple artificial data source that just returns ranges of consecutive integers
  *
  * scala> spark.sql("SELECT * FROM smallTable s JOIN bigTable b ON s.a = b.a").explain(true)
  *            == Optimized Logical Plan ==
  *            Join Inner, (a#0 = a#4)
  *            :- Filter isnotnull(a#0)
  *            :  +- Relation[a#0] SimpleRelation(1,1)
  *            +- Filter isnotnull(a#4)
  *            +- Relation[a#4] SimpleRelation(1,10000000)
  *
  *            == Physical Plan ==
  *            *SortMergeJoin [a#0], [a#4], Inner
  *            :- *Sort [a#0 ASC NULLS FIRST], false, 0
  *            :  +- Exchange hashpartitioning(a#0, 200)
  *            :     +- *Filter isnotnull(a#0)
  *            :        +- *Scan SimpleRelation(1,1) [a#0]
  *            +- *Sort [a#4 ASC NULLS FIRST], false, 0
  *            +- Exchange hashpartitioning(a#4, 200)
  *            +- *Filter isnotnull(a#4)
  *            +- *Scan SimpleRelation(1,10000000) [a#4]
  *
  * 这里添加了sparkplan的自定义strategy，但是explain发现没用到，因为新版的spark 在optimize的时候加了Filter，所有case匹配的时候不匹配，怎么改？我还不会？
  */
/** A data source that returns ranges of consecutive integers in a column named `a`. */
case class SimpleRelation(start: Int,
                          end: Int)(
                           @transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  //  val schema = StructType('a.int :: Nil)
  val schema = StructType(new StructField("a", DataTypes.IntegerType) :: Nil)

  def buildScan() = sparkSession.sparkContext.parallelize(start to end).map(Row(_))
}


object demo2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("appName")
      .getOrCreate()

    import spark.implicits._
    //create tables
    spark.baseRelationToDataFrame(SimpleRelation(1, 1)(spark)).registerTempTable("smallTable")
    spark.baseRelationToDataFrame(SimpleRelation(1, 10000000)(spark)).registerTempTable("bigTable")
    //However, doing a join is pretty slow since we need to shuffle the big table around for no reason
    spark.sql("SELECT * FROM smallTable s JOIN bigTable b ON s.a = b.a").show()


    // Add the strategy to the query planner.
    spark.experimental.extraStrategies = SmartSimpleJoin :: Nil

    spark.sql("SELECT * FROM smallTable s JOIN bigTable b ON s.a = b.a").collect()

  }
}

//let's define special physical operators for the case when we are inner joining two of these relations using equality.
// One will handle the case when there is no overlap and the other when there is.
// Physical operators must extend SparkPlan and must return an RDD[Row] containing the answer when execute() is called.

import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan

/** A join that just returns the pre-calculated overlap of two ranges of consecutive integers. */
case class OverlappingRangeJoin(leftOutput: Attribute, rightOutput: Attribute, start: Int, end: Int) extends SparkPlan {
  def output: Seq[Attribute] = leftOutput :: rightOutput :: Nil

  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(start to end).map(i => InternalRow(i, i))
  }

  def children: Seq[SparkPlan] = Nil
}

/** Used when a join is known to produce no results. */
case class EmptyJoin(output: Seq[Attribute]) extends SparkPlan {
  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    sqlContext.sparkContext.emptyRDD
  }

  def children: Seq[SparkPlan] = Nil
}

/** Finds cases where two sets of consecutive integer ranges are inner joined on equality. */
object SmartSimpleJoin extends Strategy with Serializable {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // Find inner joins between two SimpleRelations where the condition is equality.
    case Join(l@LogicalRelation(left: SimpleRelation, _, _), r@LogicalRelation(right: SimpleRelation, _, _), Inner, Some(EqualTo(a, b))) =>
      // Check if the join condition is comparing `a` from each relation.
      if (a == l.output.head && b == r.output.head || a == r.output.head && b == l.output.head) {
        if ((left.start <= right.end) && (left.end >= right.start)) {
          OverlappingRangeJoin(
            l.output.head,
            r.output.head,
            math.max(left.start, right.start),
            math.min(left.end, right.end)) :: Nil
        } else {
          // Ranges don't overlap, join will be empty
          EmptyJoin(l.output.head :: r.output.head :: Nil) :: Nil
        }
      } else {
        // Join isn't between the the columns output...
        // Let's just let the query planner handle this.
        Nil
      }
    case _ => Nil // Return an empty list if we don't know how to handle this plan.
  }
}