package SparseEngine

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparseEngineBaseline")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    println("=== Sparse Engine Baseline Implementation ===")
    
    // Parse matrices - now we get raw RDDs
    println("\n1. Parsing Sparse Matrices...")
    val matrixA = MatrixParser.parseSparseMatrix(spark, "src/main/data/SM_left.tsv")
    val matrixB = MatrixParser.parseSparseMatrix(spark, "src/main/data/SM_right.tsv")
    
    println(s"Matrix A: ${matrixA.count()} non-zero entries")
    println(s"Matrix B: ${matrixB.count()} non-zero entries")
    
    // Show some entries
    println("Matrix A sample entries:")
    matrixA.take(5).foreach { case MatrixEntry(r, c, v) =>
      println(s"  ($r, $c) -> $v")
    }
    
    // Sparse Matrix × Sparse Matrix Multiplication
    println("\n2. Sparse Matrix Multiplication (A × B)...")
    val matrixResult = Operations.sparseMatrixMultiply(matrixA, matrixB)
    println(s"Result: ${matrixResult.count()} non-zero entries")
    println("First 5 entries:")
    matrixResult.take(5).foreach { case MatrixEntry(r, c, v) =>
      println(s"  ($r, $c) -> $v")
    }
    
    // // Parse tensor
    // println("\n3. Parsing 3D Tensor...")
    // val tensor = TensorParser.parseTensor(spark, "data/tensor3d.tsv", 3)
    // println(s"Tensor: ${tensor.count()} non-zero entries")
    
    // // Show some tensor entries
    // println("Tensor sample entries:")
    // tensor.take(5).foreach { case TensorEntry(coords, v) =>
    //   println(s"  (${coords.mkString(", ")}) -> $v")
    // }
    
    // // Tensor × Tensor Multiplication
    // println("\n4. Tensor Multiplication...")
    // val tensorResult = Operations.tensorMultiply(tensor, tensor, 3)
    // println(s"Result: ${tensorResult.count()} non-zero entries")
    // println("First 5 entries:")
    // tensorResult.take(5).foreach { case TensorEntry(coords, v) =>
    //   println(s"  (${coords.mkString(", ")}) -> $v")
    // }
    
    spark.stop()
  }
}