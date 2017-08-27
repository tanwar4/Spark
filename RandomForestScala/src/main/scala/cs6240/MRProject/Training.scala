import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

object Training {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Training").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val csv = sc.textFile(args(0))

    //To find the headers
    val header = csv.first;

    //To remove the header
    val data = csv.filter(line => line != header)

    // create categorical info map
    var baileyEcoRegMap: Map[String, Double] = Map()
    var index: Double = 0.0
    data.map { line =>
      val parts = line.split(',')
      parts(961)
    }.distinct.collect.foreach(x => { baileyEcoRegMap += (x -> index); index += 1.0 })

    var featuresSize = 0
    //To create a RDD of (label, features) pairs
    val parsedData = data.map { line =>
      val parts = line.split(',')
      // extracting the column features from input row
      //val slc = parts.slice(2, 8) ++ parts.slice(955, 961) ++ parts.slice(962, 1016) ++ parts.slice(1019, 1091) ++ parts.slice(961, 962)
      val slc = parts.slice(2, 8) ++ parts.slice(955, 961) ++ parts.slice(962, 1016) ++ parts.slice(1019, 1656) ++ parts.slice(961, 962)
      // create sparse vector of ( featureIndex -> feature.value )
      val indces = slc.zipWithIndex.filter(_._1 != "?").map(_._2)

      val values = indces.map { a =>
        var m = 0.0
        if (a == slc.size - 1) {
          m = baileyEcoRegMap(slc(a)).toDouble
        } else {
          m = slc(a).toDouble
        }
        m
      }

      var out = 0.0
      // if label is X or > 0 : classify as 1.o
      if (parts(26) == "X" || (parts(26).toDouble > 0.0))
        out = 1.0

      LabeledPoint(out.toDouble, Vectors.sparse(slc.length, indces, values))
    }.cache()

    /***************     MODEL ******************************************************/

    val splits = parsedData.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    val numClasses = 2
    //var categoricalFeaturesInfo = Map[Int, Int](138 -> baileyEcoRegMap.size)
    var categoricalFeaturesInfo = Map[Int, Int](703 -> baileyEcoRegMap.size)

    val numTrees = 20
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 25
    val maxBins = baileyEcoRegMap.size + 10

    // train the classifier model
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute ACC
    println("Learned classification forest model:\n" + model.toDebugString)

    val predictionsAndLabels = testData.map {
      point => (model.predict(point.features), point.label)
    }
    val stats = Statss(Statss.confusionMatrix(predictionsAndLabels))
    println("ACC : " + stats.ACC)

    // Save model
    model.save(sc, args(1) + "/RF_DEPTH25_TREES20")
  }
}