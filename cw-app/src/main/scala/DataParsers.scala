package SparseEngine

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Only need the essential data structures
case class MatrixEntry(row: Int, col: Int, value: Double)
case class TensorEntry(coords: Array[Int], value: Double)
case class VectorEntry(index: Int, value: Double)

// object DenseVectorParser {
//   def parseDenseVector(spark: SparkSession, filePath: String): RDD[VectorEntry] = {
//     spark.sparkContext.textFile(filePath)
//       .flatMap { line =>
//         val parts = line.split("\\s+|,|\t").filter(_.nonEmpty)
//         parts.zipWithIndex.flatMap { case (valueStr, idx) =>
//           try {
//             val value = valueStr.toDouble
//             Some(VectorEntry(idx, value))
//           } catch {
//             case _: NumberFormatException => None
//           }
//         }
//       }
//   }
// }

object COO_VectorParser {
  def parseVector(spark: SparkSession, filePath: String): RDD[VectorEntry] = {
    spark.sparkContext.textFile(filePath)
      .flatMap { line =>
        val parts = line.split("\\s+|,|\t").filter(_.nonEmpty)
        parts.zipWithIndex.flatMap { case (valueStr, idx) =>
          try {
            val value = valueStr.toDouble
            // Only keep nonzero values
            if (value != 0.0) Some(VectorEntry(idx, value))
            else None
          } catch {
            case _: NumberFormatException => None
          }
        }
      }
  }
}


// Parsers - now return raw RDDs
object COO_MatrixParser {
  def parseMatrix(spark: SparkSession, filePath: String): RDD[MatrixEntry] = {
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

// object DenseMatrixParser {
//   def parseDenseMatrix(spark: SparkSession, filePath: String): RDD[MatrixEntry] = {
//     spark.sparkContext.textFile(filePath)
//       .zipWithIndex()
//       .flatMap { case (line, rowIndex) =>
//         val values = line.split("\t").filter(_.nonEmpty)
//         values.zipWithIndex.flatMap { case (valueStr, colIndex) =>
//           try {
//             val value = valueStr.toDouble
//             Some(MatrixEntry(rowIndex.toInt, colIndex, value))
//           } catch {
//             case _: NumberFormatException => None
//           }
//         }
//       }
//   }
// }

object COO_TensorParser {
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