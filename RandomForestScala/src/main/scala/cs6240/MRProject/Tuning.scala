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
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object Tuning {

  def indices(v1: Array[String]): Array[Int] = {
    v1.zipWithIndex.filter(_._1 != "?").map(_._2)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Tuning").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val csv = sc.textFile(args(0))

    //To find the headers
    val header = csv.first;

    //To remove the header
    val data = csv.filter(line => line != header)
    //To create a RDD of (label, features) pairs
    val parsedData = data.map { line =>
      val parts = line.split(',')
      // extracting the column features from input row
      val slc = parts.slice(2, 8) ++ parts.slice(955, 961) ++ parts.slice(963, 1016) ++ parts.slice(1019, 1091)
      // create sparse vector of ( featureIndex -> feature.value )
      val indces = slc.zipWithIndex.filter(_._1 != "?").map(_._2)

      val values = indces.map { a => slc(a).toDouble }

      var out = 0.0
      // if label is X or > 0 : classify as 1.o
      if (parts(26) == "X" || (parts(26).toDouble > 0.0))
        out = 1.0

      LabeledPoint(out.toDouble, Vectors.sparse(slc.length, indces, values))
    }.cache()

    //***************     MODEL ******************************************************//

    val splits = parsedData.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    val numClasses = 2
    var categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10 
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 25
    val maxBins = 34

    // run the classifier for each configuration
    val tuning = for (numTrees <- Range(2, 21)) yield {
      val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
      val predictionsAndLabels = testData.map {
        point => (model.predict(point.features), point.label)
      }
      // get the ACC stats
      val stats = Statss(Statss.confusionMatrix(predictionsAndLabels))
      val metrics = new BinaryClassificationMetrics(predictionsAndLabels)
      (numTrees, stats.MCC, stats.ACC, metrics.areaUnderPR, metrics.areaUnderROC)
    }
    //var outRDD = sc.parallelize(Seq("Results:"))
    tuning.sortBy(_._2).reverse.foreach {
      x => println("Num trees: " + x._1 + ", Matthews correlation coefficient: " + x._2 + ", Accuracy: " + x._3 + ", Area under PR: " + x._4 + ", Area under ROC: " + x._5)
      //   outRDD = outRDD ++ sc.parallelize(Seq("Num trees: " + x._1 + ", Matthews correlation coefficient: " + x._2 + ", Accuracy: " + x._3+ ", Area under PR: " + x._4+ ", Area under ROC: " + x._5))
    }

    //outRDD.coalesce(1, true).saveAsTextFile(args(1) + "/tuning_results") 
  }
}