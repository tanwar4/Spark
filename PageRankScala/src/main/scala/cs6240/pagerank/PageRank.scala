package cs6240.pagerank
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf,SparkContext}


object PageRank {
    def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("PageRank").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    val filename = args(0)            //filename
    val file = sc.textFile(filename) //create rdd
    val lines = file.map( f => Bz2WikiParser.mainParser(f)) // parse each line and create the adjacency list Rdd
                       
    var links = lines.map(line => line.split("\t"))        //parsing each adjacency list RDD  
                     .filter(fields => (!fields(0).contains("~~") || (fields(0).length>1)))  //filtering out invalid pages
                     .map(fields => (fields(0),fields(1).substring(1,fields(1).length()-1).split(",").distinct)) // cleaned adjacency list
                     .groupByKey()                         //grouping keys
                     .mapValues(f=>f.flatMap(f=>f))        //grouping values into single list for each key
                     .cache()                              //caching RDD
            
     var nodes = links.keys                                //Distinct Keys list
     var newnodes= links.values.flatMap(f=>f).distinct()   //Distinct Values list
          
     //extracting sink nodes in values which doesnt appear in keys list
     var ff = newnodes.subtract(nodes).collect
                    
     //concatinating the sink nodes into original RDD list, the values of all such nodes is empty list
     val linkList = links ++ sc.parallelize(for (i <- ff) yield (i, List.empty))  
        
     val nodeSize = linkList.count()  //count of all nodes in the graph       
  
     var ranks = linkList.mapValues(v => 1.0/nodeSize)  //initializing the page rank with initial values
                   
    // Iterative calculation PR value  
        for (i <- 1 to 10) {  
            val danglingMass = sc.accumulator(0.0)        //dangling nodes mass accumulator
            val contribs = linkList.join(ranks).values.flatMap {   //joining the list with rank list
                case (urls, rank) => {                           //new class is created which stores outlinks and pagerank
                    val size = urls.size                        //total outgoing links
                    if (size == 0) {  
                        danglingMass += rank                    //if url is sink node add the mass in accumulator
                        List()  
                    } else {  
                        urls.map(url => (url, rank / size))     //else propagate the  mass to outgoing links
                    }  
                }  
            }    
            contribs.count()          
            val danglingValue = danglingMass.value   //dangling mass 
            //using the page rank formula to calculate pagerank for each node 
            ranks = contribs.reduceByKey(_ + _).mapValues[Double](p =>  
                                              (0.85 / nodeSize) + 0.15 * (danglingValue / nodeSize + p))  
       }
    
    var tupledRDD = ranks.map(line => (line._1, line._2)).sortBy(_._2, ascending=false)   //sorting the list
                  tupledRDD.take(100).foreach(println) 
                  tupledRDD.saveAsTextFile(args(1))             //saving the file  
   } 
    
}