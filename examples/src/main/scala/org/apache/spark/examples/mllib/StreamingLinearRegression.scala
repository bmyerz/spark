/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Train a linear regression model on one stream of data and make predictions
 * on another stream, where the data streams arrive as text files
 * into two different directories.
 *
 * The rows of the text files must be labeled data points in the form
 * `(y,[x1,x2,x3,...,xn])`
 * Where n is the number of features. n must be the same for train and test.
 *
 * Usage: StreamingLinearRegression <trainingDir> <testDir> <batchDuration> <numFeatures>
 *
 * To run on your local machine using the two directories `trainingDir` and `testDir`,
 * with updates every 5 seconds, and 2 features per data point, call:
 *    $ bin/run-example \
 *        org.apache.spark.examples.mllib.StreamingLinearRegression trainingDir testDir 5 2
 *
 * As you add text files to `trainingDir` the model will continuously update.
 * Anytime you add text files to `testDir`, you'll see predictions from the current model.
 *
 */
object StreamingLinearRegression {

  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println(
        "Usage: StreamingLinearRegression <trainingDir> <testDir> <batchDuration> <numFeatures>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("StreamingLinearRegression")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

    val trainingData = MLUtils.loadStreamingLabeledPoints(ssc, args(0))
    val testData = MLUtils.loadStreamingLabeledPoints(ssc, args(1))

    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(Array.fill[Double](args(3).toInt)(0)))

    model.trainOn(trainingData)
    model.predictOn(testData).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
