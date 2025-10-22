package SparseEngine

import org.apache.spark.rdd.RDD

object Operations {

  def COO_MatrixVectorMultiply(A: RDD[MatrixEntry], x: RDD[VectorEntry]): RDD[(Int, Double)] = {
    // Step 1: join on the column index (A.col == x.index)
    val A_byCol = A.map(e => (e.col, (e.row, e.value)))
    val x_byIndex = x.map(v => (v.index, v.value))

    A_byCol.join(x_byIndex) // (col, ((row, A_val), x_val))
      .map { case (_, ((row, A_val), x_val)) =>
        (row, A_val * x_val) // partial contribution to row
      }
      .reduceByKey(_ + _) // sum contributions per row
  }
  
  // Now takes and returns raw RDDs
  def COO_MatrixMultiply(A: RDD[MatrixEntry], B: RDD[MatrixEntry]): RDD[MatrixEntry] = {
    val A_byCol = A.map(e => (e.col, (e.row, e.value)))
    val B_byRow = B.map(e => (e.row, (e.col, e.value)))
    
    A_byCol.join(B_byRow)
      .map { case (_, ((rowA, valA), (colB, valB))) => 
        ((rowA, colB), valA * valB) 
      }
      .reduceByKey(_ + _)
      .map { case ((row, col), value) => MatrixEntry(row, col, value) }
  }
  
  def COO_tensorMultiply(t1: RDD[TensorEntry], t2: RDD[TensorEntry], dimensions: Int): RDD[TensorEntry] = {
    val t1_keyed = t1.map(te => (te.coords.mkString(","), te.value))
    val t2_keyed = t2.map(te => (te.coords.mkString(","), te.value))
    
    t1_keyed.join(t2_keyed)
      .map { case (coordsStr, (v1, v2)) =>
        val coords = coordsStr.split(",").map(_.toInt)
        TensorEntry(coords, v1 * v2)
      }
  }
}