
// give the data examples and then check for each possible combination and do the relevant stuff
// check if its a matrix of vector, check if its sparse or dense

package Frontend

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD



object Frontend {
    // Main should take in filepaths of the tsv data files
    def main(args: Array[String]): Unit = {

        if args.length < 4 then
            println("Usage: Frontend <filePath1> <filePath2> <dataFormat1> <dataFormat2>")
            println("<dataFormat>: 'sparse' or 'dense'")
            return
            
        val leftOperandfilePath1 = args(0)
        val rightOperandfilePath2 = args(1)
        val leftOperanddataFormat1 = args(2) // "COO" or "CSR" OR "CSC" or "SELL"
        val rightOperanddataFormat2 = args(3) 

        val leftOperand = spark.sparkContext.textFile(leftOperandfilePath1)
        val rightOperand = spark.sparkContext.textFile(rightOperandfilePath2)

        // Check the left leftOperand
        val leftNumRows = leftOperand.count()
        val leftNumCols = leftOperand.first().split("\t").length
        
        // check if the right operand is a vector or matrix by looking at the dimensions
        val rightOperandIsVector = rightOperand.first().split("\t").length == 1
        val rightNumRows = rightOperand.count()
        val rightNumCols = if rightOperandIsVector then 1 else rightOperand.first().split("\t").length

        // Check compatibility for multiplication
        // check if the shapes are compatible- matrix multiplication is (m x n) x (n x p) = (m x p). n must be the same
        // matrices are (r x c) and vectors are (n x 1) for our purposes
        val compatible = (leftNumCols == rightNumRows)
        if !compatible then
            println(s"Incompatible dimensions for multiplication: Left operand is $leftNumRows x $leftNumCols, Right operand is $rightNumRows x $rightNumCols")
            return


        // Check if the leftOperand is a Sparse Matrix
        val zeroCountLeft = zeroCount(leftOperand)
        val zeroRatio = (zeroCountLeft.toDouble / (leftNumRows.toDouble * leftNumCols.toDouble)) * 100.0
        val leftOperandIsSparse = zeroRatio > 50.0 // arbitrary threshold of 50% zeros to consider sparse

        // Check if the rightOperand is a Sparse Vector/Matrix
        val zeroCountRight = zeroCount(rightOperand)
        val rightZeroRatio = (zeroCountRight.toDouble / (rightNumRows.toDouble * rightNumCols.toDouble)) * 100.0
        val rightOperandIsSparse = rightZeroRatio > 50.0 // arbitrary threshold of 50% zeros to consider sparse
        
        // Handle SpM x Dense Vector
        if leftOperandIsSparse && !rightOperandIsSparse && rightOperandIsVector && compatible then
            // return COO_MatrixParser.parseMatrix and COO_VectorParser.parseVector and operation is COO_MatrixVectorMultiply
            return "matrix", "denseVector"

        // Handle SpM x Sparse Vector
        else if leftOperandIsSparse && rightOperandIsSparse && rightOperandIsVector && compatible then
            return "matrix", "sparseVector"
            // Call the relevant function from SparseEngine

        // Handle SpM x Dense Matrix
        else if leftOperandIsSparse && !rightOperandIsSparse && !rightOperandIsVector && compatible then
            return "matrix", "denseMatrix"
            // Call the relevant function from SparseEngine

        // Handle SpM x Sparse Matrix
        else if leftOperandIsSparse && rightOperandIsSparse && !rightOperandIsVector && compatible then
            return "matrix", "sparseMatrix"
            // Call the relevant function from SparseEngine

        else
            // TODO: Check if the left operand is a dense matrix as this shouldnt be allowed
            // TODO: also handle tensors
            println("Currently only Sparse Matrix operations are supported in this version.")










    }

    def zeroCount(rdd: RDD[String]): Long = {
        rdd.flatMap(_.split("\t")) // split each row into columns
            .filter(_.nonEmpty)    // remove blanks
            .map(_.toDouble)       // convert to double
            .filter(_ == 0.0)      // filter zeros   
            .count()
    }
}