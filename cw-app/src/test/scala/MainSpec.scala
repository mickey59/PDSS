import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Basic data structures
case class MatrixEntry(row: Int, col: Int, value: Double)
case class TensorEntry(coords: Array[Int], value: Double)

class SimpleSparseEngine(spark: SparkSession) {
  
  // Basic parsing - no optimizations
  def parseSparseMatrix(filePath: String): RDD[MatrixEntry] = {
    spark.sparkContext
      .textFile(filePath)
      .flatMap { line =>
        val parts = line.split("[,\\s]+").filter(_.nonEmpty)
        if (parts.length == 3) {
          Some(MatrixEntry(parts(0).toInt, parts(1).toInt, parts(2).toDouble))
        } else None
      }
  }
  
  def parseTensor(filePath: String, numDims: Int): RDD[TensorEntry] = {
    spark.sparkContext
      .textFile(filePath)
      .flatMap { line =>
        val parts = line.split("[,\\s]+").filter(_.nonEmpty)
        if (parts.length == numDims + 1) {
          val coords = parts.take(numDims).map(_.toInt)
          Some(TensorEntry(coords, parts.last.toDouble))
        } else None
      }
  }
  
  // Simple Sparse Matrix-Matrix Multiplication
  def simpleSpMM(A: RDD[MatrixEntry], B: RDD[MatrixEntry]): RDD[MatrixEntry] = {
    // Convert A to (col, (row, value)) for joining
    val A_byCol = A.map(entry => (entry.col, (entry.row, entry.value)))
    
    // Convert B to (row, (col, value)) - but we need to join on A's col = B's row
    val B_byRow = B.map(entry => (entry.row, (entry.col, entry.value)))
    
    // Join and multiply
    A_byCol.join(B_byRow)
      .map { case (k, ((rowA, valA), (colB, valB))) =>
        ((rowA, colB), valA * valB)
      }
      .reduceByKey(_ + _)
      .map { case ((row, col), value) => MatrixEntry(row, col, value) }
  }
  
  // Simple Tensor-Tensor Multiplication (element-wise for same dimensions)
  def simpleTensorMult(tensor1: RDD[TensorEntry], tensor2: RDD[TensorEntry]): RDD[TensorEntry] = {
    // Convert to (coordinates, value) for joining
    val t1 = tensor1.map(te => (te.coords.mkString(","), te.value))
    val t2 = tensor2.map(te => (te.coords.mkString(","), te.value))
    
    t1.join(t2)
      .map { case (coordsStr, (v1, v2)) =>
        val coords = coordsStr.split(",").map(_.toInt)
        TensorEntry(coords, v1 * v2)
      }
  }
}