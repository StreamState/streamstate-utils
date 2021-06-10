package org.streamstate.streamstateutils

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class GeometricMean extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("product", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}

class StandardDeviation extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("sumsq", DoubleType) ::
      StructField("sum", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
    buffer(2) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    val v = input.getAs[Double](0)
    buffer(1) = buffer.getAs[Double](1) + v * v
    buffer(2) = buffer.getAs[Double](2) + v
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val mean = buffer.getDouble(2)
    val n = buffer.getLong(0)
    math.sqrt(buffer.getDouble(1) / (n - 1) - mean * mean / (n * (n - 1)))
  }
}

class StandardDeviation extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("sumsq", DoubleType) ::
      StructField("sum", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
    buffer(2) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    val v = input.getAs[Double](0)
    buffer(1) = buffer.getAs[Double](1) + v * v
    buffer(2) = buffer.getAs[Double](2) + v
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val mean = buffer.getDouble(2)
    val n = buffer.getLong(0)
    math.sqrt(buffer.getDouble(1) / (n - 1) - mean * mean / (n * (n - 1)))
  }
}

class WeightedMovingAverage extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("sum", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
    buffer(2) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    val v = input.getAs[Double](0)
    buffer(1) = buffer.getAs[Double](1) + v * v
    buffer(2) = buffer.getAs[Double](2) + v
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val mean = buffer.getDouble(2)
    val n = buffer.getLong(0)
    math.sqrt(buffer.getDouble(1) / (n - 1) - mean * mean / (n * (n - 1)))
  }
}
