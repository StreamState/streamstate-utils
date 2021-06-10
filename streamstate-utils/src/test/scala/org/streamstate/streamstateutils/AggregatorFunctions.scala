package org.streamstate.streamstateutils

import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.functions.{col}
import org.apache.spark.{SparkContext}
//import org.scalatest.{FunSuite}
import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}

object CreateDataSet {
  def create_dataset(sc: SparkContext, sqlCtx: SQLContext): DataFrame = {
    import sqlCtx.implicits._
    return sc
      .parallelize(
        Seq(
          ("val1", 1.0, 0.0),
          ("val1", 1.0, 0.0),
          ("val1", 1.0, 1.0),
          ("val1", 2.0, 1.0),
          ("val1", 2.0, 1.0),
          ("val1", 2.0, 1.0),
          ("val1", 3.0, 0.0),
          ("val1", 3.0, 0.0),
          ("val1", 3.0, 0.0),
          ("val1", 3.0, 0.0),
          ("val2", 1.0, 0.0),
          ("val2", 1.0, 0.0),
          ("val2", 1.0, 0.0),
          ("val2", 1.0, 1.0),
          ("val2", 1.0, 1.0)
        )
      )
      .toDF("group", "label", "prediction")
  }
}

class GeometricMeanTest extends AnyFunSuite with DataFrameSuiteBase {
  test("it returns GeometricMean") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val dataset = CreateDataSet.create_dataset(sc, sqlCtx)
    val gm = new GeometricMean

    val result =
      dataset
        .groupBy("group")
        .agg(gm(col("label")).as("GeometricMean"))
        .collect()

    val expected = Seq(("val1", 1.9105460086999304), ("val2", 1.0))
    val zippedResultExpected: Seq[(Row, (String, Double))] =
      result zip expected
    println(result)
    zippedResultExpected.foreach({
      case (row, (e1, e2)) => {
        val resultGroup = row.getString(0)
        val resultValue = row.getDouble(1)
        assert(resultGroup == e1)
        assert(resultValue == e2)
      }
    })

  }
}

class StandardDeviationTest extends AnyFunSuite with DataFrameSuiteBase {
  test("it returns StandardDeviation") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val dataset = CreateDataSet.create_dataset(sc, sqlCtx)
    val sd = new StandardDeviation

    val result =
      dataset
        .groupBy("group")
        .agg(sd(col("label")).as("StandardDeviation"))
        .collect()

    val expected = Seq(("val1", 0.8755950357709131), ("val2", 0.0))
    val zippedResultExpected: Seq[(Row, (String, Double))] =
      result zip expected
    println(result)
    zippedResultExpected.foreach({
      case (row, (e1, e2)) => {
        val resultGroup = row.getString(0)
        val resultValue = row.getDouble(1)
        assert(resultGroup == e1)
        assert(resultValue == e2)
      }
    })

  }
}
