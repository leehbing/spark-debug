package sparksql.custom.sparkplan.strategy

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Range}
import org.apache.spark.sql.execution.{ProjectExec, RangeExec, SparkPlan}

/**
  * @Author: hongbing.li
  * @Date: 29/9/2018 2:15 PM
  *       自定义生成sparkplan的strategy
  */
object Test {
  val conf = new SparkConf().setMaster("master").setAppName("appName")
  val spark = SparkSession.builder
    .master("master")
    .appName("appName")
    .getOrCreate()

  import spark.implicits._

  val tableA1 = spark.range(100000000).as('a)
  val tableB1 = spark.range(100000000).as('b)
  val result1 = tableA1.join(tableB1, $"a.id" === $"b.id").groupBy().count()

  result1.explain(true)
  result1.count()
//////////////////-------------------------
  val tableA2 = spark.range(100000000).as('a)
  val tableB2 = spark.range(100000000).as('b)
  val result2 = tableA2.join(tableB2, $"a.id" === $"b.id").groupBy().count()

//  tableB2.select(org.apache.spark.sql.functions.mean($"a.id"))

  spark.experimental.extraStrategies = IntervalJoin :: Nil


  result2.explain(true)
  result2.count()

}


case object IntervalJoin extends Strategy with Serializable {
  def apply(myplan: LogicalPlan): Seq[SparkPlan] = myplan match {
    case Join(
    Range(start1, end1, 1, part1, Seq(o1)),
    Range(start2, end2, 1, part2, Seq(o2)),
    Inner,
    Some(EqualTo(e1, e2)))
      if ((o1 semanticEquals e1) && (o2 semanticEquals e2)) ||
        ((o1 semanticEquals e2) && (o2 semanticEquals e1)) => {
      if ((start1 <= end2) && (end1 >= end2)) {
        val start: Long = math.max(start1, start2)
        val end = math.min(end1, end2)
        val part = math.max(part1.getOrElse(200), part2.getOrElse(200))
        val result = RangeExec(Range(start, end, 1, Some(part), o1 :: Nil))
        val twoColumns = ProjectExec(
          Alias(o1, o1.name)(exprId = o1.exprId) :: Nil,
          result)
        twoColumns :: Nil
      }else{
        Nil
      }
    }
    case _ => Nil
  }
}