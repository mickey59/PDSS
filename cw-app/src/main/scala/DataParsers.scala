package SparseEngine

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Only need the essential data structures
case class MatrixEntry(row: Int, col: Int, value: Double)
case class TensorEntry(coords: Array[Int], value: Double)

// Parsers - now return raw RDDs
object MatrixParser {
  def parseSparseMatrix(spark: SparkSession, filePath: String): RDD[MatrixEntry] = {
    spark.sparkContext.textFile(filePath)
      .zipWithIndex()
      .flatMap { case (line, rowIndex) =>
        val values = line.split("\t").filter(_.nonEmpty)
        values.zipWithIndex.flatMap { case (valueStr, colIndex) =>
          try {
            val value = valueStr.toDouble
            if (value != 0.0) Some(MatrixEntry(rowIndex.toInt, colIndex, value)) 
            else None
          } catch {
            case _: NumberFormatException => None
          }
        }
      }
  }
}

object TensorParser {
  def parseTensor(spark: SparkSession, filePath: String, dims: Int): RDD[TensorEntry] = {
    require(dims >= 3, "Tensor must be 3D or higher")
    
    spark.sparkContext.textFile(filePath)
      .flatMap { line =>
        val parts = line.split("\t").filter(_.nonEmpty)
        if (parts.length == dims + 1) {
          try {
            val coords = parts.take(dims).map(_.toInt)
            val value = parts.last.toDouble
            Some(TensorEntry(coords, value))
          } catch {
            case _: NumberFormatException => None
          }
        } else None
      }
  }
}