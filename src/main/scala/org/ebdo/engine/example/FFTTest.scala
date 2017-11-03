package org.ebdo.engine.example

import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D
import org.apache.spark.sql.SparkSession

/**
  * FFT computation
  * Copyright (C) 2017 Project-EBDO
  * Author: Paul NHD
  *
  * Enables to divide a long arrays in smaller ones.
  * Computes FFT on the small arrays
  *
  * @param spark The SparkSession if needed.
  */
class FFTTest(spark: SparkSession){

  /**
    * Function that computes FFT for an array
    *
    * @param arrayToProcess the data to analyze in an Array[Double]
    * @return the FFT of the input array
    */
  def FFTArrays(arrayToProcess: Array[Double]): Array[Double] = {
    val fftResult = {
      val temp = arrayToProcess ++ Array.fill(arrayToProcess.size)(0.0)
      val fft = new DoubleFFT_1D(arrayToProcess.size)                      // attention!! data2.size == temp.size / 2
      fft.realForwardFull(temp)                         // Side Effect!
      temp
    }
    fftResult
  }

  /**
    * Function dividing the array to process in smaller parts to compute FFT
    *
    * @param nfft Points to consider to compute the FFT
    * @param overlap overlap of the buffers
    * @param arrayToProcess the data to analyze in an Array[Double]
    * @return the FFT of the input array
    */
  // Todo  : Check if array length is a multiple of nfft and check for the overlap
  def smallBuffersForFFT(nfft: Int, overlap: Double,arrayToProcess: Array[Double]): Array[Array[Double]]={
    // Initialisation of variable that will be returned
    var buffersOfNfft = null.asInstanceOf[Array[Array[Double]]]

    // Check if the last buffer contains nfft elements todo : to complete
    if (arrayToProcess.length % nfft == 0){
      // Divide the array in smaller buffers of nfft points with overlap or not
      buffersOfNfft = arrayToProcess.sliding(nfft,(nfft*overlap).toInt).toArray
    }else{
      // Divide the array in smaller buffers of nfft points with overlap or not.
      // To improve if the last buffer does not contains nfft elements
      buffersOfNfft = arrayToProcess.sliding(nfft,(nfft*overlap).toInt).toArray.dropRight(1)
    }
    // Return an array of small buffers of nfft points
    buffersOfNfft
  }

  /**
    * Function computing the division without handling the exception /0.
    *
    */
  // Todo : Exception to handle
  def divi(num: Double, deno: Double): Double = {num/deno}

  /**
    * Function computing the mean of FFTs without overlap
    *
    * @param aaaToProcess Contain all the small buffers of a dataframe.
    * @return contain the mean FFTs of a dataframe
    */
  def meanFFT(aaaToProcess: Array[Array[Array[Double]]]): Array[Array[Double]] = {
    // First map to sum all elements by column.
    // e.g: [ a0 b0 c0 ]   ->  [ a0+a1 b0+b1 c0+c1 ]
    //      [ a1 b1 c1 ]
    // Second map to divide each elements by nfft ( = x.length)
    // e.g: [ (c0+c1)/nfft (c0+c1)/nfft (c0+c1)/nfft]
    aaaToProcess.map(x=>x.transpose.map(_.sum).map(y=>divi(y,x.length)))
  }
}
