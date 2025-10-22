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
    val matrixA = COO_MatrixParser.parseMatrix(spark, "src/main/data/baseline_data/SM_1.tsv")
    val matrixB = COO_MatrixParser.parseMatrix(spark, "src/main/data/baseline_data/SM_2.tsv")

    println(s"Matrix A: ${matrixA.count()} non-zero entries")
    println(s"Matrix B: ${matrixB.count()} non-zero entries")
    
    // Show some entries
    println("Matrix A sample entries:")
    matrixA.foreach { case MatrixEntry(r, c, v) =>
      println(s"  ($r, $c) -> $v")
    }
    println("Matrix B sample entries:")
    matrixB.foreach { case MatrixEntry(r, c, v) =>
      println(s"  ($r, $c) -> $v")
    }
    
    // Sparse Matrix × Sparse Matrix Multiplication
    println("\n2. Sparse Matrix Multiplication (A × B)...")
    val matrixResult = Operations.MatrixMultiply(matrixA, matrixB)
    println(s"Result: ${matrixResult.count()} non-zero entries")
    println("First 5 entries:")
    matrixResult.foreach { case MatrixEntry(r, c, v) =>
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
    
  
    // Frontend integration

    // TODO: need to define args to take in user defined file paths and formats from the command line


    // call frontend to validate which parsers and operations to use
    val (leftParserString, rightParserString) = Frontend.main(args)

    // Based on the returned strings from front end can do the relevant operations and parsers
    // the clarifier value "dense" or "sparse" will serve as information later for optimisations in the data formats
    if leftParserString == "matrix" && rightParserString == "denseVector" then
      val matrix_A = COO_MatrixParser.parseMatrix(spark, leftOperandfilePath1)
      val vector_x = COO_VectorParser.parseVector(spark, rightOperandfilePath2)
      val mv_result = Operations.COO_MatrixVectorMultiply(matrix_A, vector_x)

    else if leftParserString == "matrix" && rightParserString == "sparseVector" then
      val matrix_A = COO_MatrixParser.parseMatrix(spark, leftOperandfilePath1)
      val vector_x = COO_VectorParser.parseVector(spark, rightOperandfilePath2)
      val mv_result = Operations.COO_MatrixVectorMultiply(matrix_A, vector_x)

    else if leftParserString == "matrix" && rightParserString == "denseMatrix" then
      val matrix_A = COO_MatrixParser.parseMatrix(spark, leftOperandfilePath1)
      val matrix_B = COO_MatrixParser.parseDenseMatrix(spark, rightOperandfilePath2)
      val mm_result = Operations.COO_MatrixMultiply(matrix_A, matrix_B)

    else if leftParserString == "matrix" && rightParserString == "sparseMatrix" then
      val matrix_A = COO_MatrixParser.parseMatrix(spark, leftOperandfilePath1)
      val matrix_B = COO_MatrixParser.parseMatrix(spark, rightOperandfilePath2)
      val mm_result = Operations.COO_MatrixMultiply(matrix_A, matrix_B)

    spark.stop()
  }
}