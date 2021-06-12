package org.streamstate.streamstateutils

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.expressions.{
  Aggregator,
  SparkUserDefinedFunction,
  UserDefinedAggregator,
  UserDefinedFunction
}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{Column}
class GeometricMean extends Aggregator[Double, (Int, Double), Double] {
  def zero: (Int, Double) = (0, 1.0)
  def reduce(
      buffer: (Int, Double),
      data: Double
  ): (Int, Double) = {
    (buffer._1 + 1, buffer._2 * data)
  }
  def merge(
      b1: (Int, Double),
      b2: (Int, Double)
  ): (Int, Double) = {
    (b1._1 + b2._1, b1._2 * b2._2)
  }
  def finish(buffer: (Int, Double)): Double = {
    math.pow(buffer._2, 1.toDouble / buffer._1)
  }

  def bufferEncoder: Encoder[(Int, Double)] = Encoders.tuple(
    Encoders.scalaInt,
    Encoders.scalaDouble
  )
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class StandardDeviation
    extends Aggregator[Double, (Int, Double, Double), Double] {
  def zero: (Int, Double, Double) = (0, 0.0, 0.0)
  def reduce(
      buffer: (Int, Double, Double),
      data: Double
  ): (Int, Double, Double) = {
    (buffer._1 + 1, buffer._2 + data * data, buffer._3 + data)
  }
  def merge(
      b1: (Int, Double, Double),
      b2: (Int, Double, Double)
  ): (Int, Double, Double) = {
    (b1._1 + b2._1, b1._2 + b2._2, b1._3 + b2._3)
  }
  def finish(buffer: (Int, Double, Double)): Double = {
    val mean = buffer._3
    val n = buffer._1
    math.sqrt(buffer._2 / (n - 1) - mean * mean / (n * (n - 1)))
  }

  def bufferEncoder: Encoder[(Int, Double, Double)] = Encoders.tuple(
    Encoders.scalaInt,
    Encoders.scalaDouble,
    Encoders.scalaDouble
  )
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object functions {

  def standardDeviation(e: Column): Column = {
    udaf(new StandardDeviation(), Encoders.scalaDouble)(e)
  }

  def geometricMean(e: Column): Column = {
    udaf(new GeometricMean(), Encoders.scalaDouble)(e)
  }
}
