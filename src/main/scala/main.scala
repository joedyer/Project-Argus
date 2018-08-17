import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

//import the various handlers for each module
import handlers._

object run_this {
	def main(args: Array[String]) = {

        val conf = new SparkConf().set("spark.neo4j.bolt.url","bolt://neo4j:neo4j@localhost")

        val sc = new SparkContext(conf);

        val neo = org.neo4j.spark.Neo4j(sc)

        //all the logs will come in as arguments to the main function
        //I.e. system logs will be an argument at posiion 0
        //order of the logs will be enforced by a flag system and a bash script
        //I.e. when running the program this:
        //$run.sh -l /home/directory/otherdir/file
        //in this case the user is flagging file as a system log

        //cheat sheet for files by arg position:
        //0: System logs
        //1: PCAP logs

		  //create the handlers for the modules
       	var loghandler = new LogHandler()
        var neohandler = new NeoHandler(neo)

       	// check if args are null
       	// if not, send result to handler
        var file = sc.textFile(args(0))
       	// if(file != null){
       	var arr =	loghandler.run(file)
       	// }

        arr.foreach(rdd => rdd.take(20).foreach(x => println(x)));

       	//add resultant DF to neo4j
		    arr.foreach(rdd => neohandler.add(rdd))
	}
}

