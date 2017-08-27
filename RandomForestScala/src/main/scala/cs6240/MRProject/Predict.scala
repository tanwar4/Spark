import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

object Predict {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Predict").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sameModel = RandomForestModel.load(sc, args(2))

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

    // parse data into labeledPoint and predict the label
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
      val lp = LabeledPoint(0.0, Vectors.sparse(slc.length, indces, values))
      // predict the label of this point
      val prediction = sameModel.predict(lp.features).toInt
      
      (parts(0) + ',' + prediction.toString())
    }.cache()

    // create the header for final output
    val finalHeader = sc.parallelize(Array("SAMPLING_EVENT_ID,SAW_AGELAIUS_PHOENICEUS"))
    // append the header to output
    val finalRDD = finalHeader.union(parsedData).collect
    val output = sc.parallelize(finalRDD, 1)

    // write the output
    output.saveAsTextFile(args(1) + "/predict")

  }
}