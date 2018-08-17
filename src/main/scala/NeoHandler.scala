package handlers

import org.apache.spark.rdd.RDD

class NeoHandler(neo:org.neo4j.spark.Neo4j){

	//input (connections dataframe) already has all the strings necessary to make the graph
	//strings look like: 
	//'(daw:User) -[:SuperAction {time:'Nov  2 14:33:46', command:'/opt/spark-2.0/sbin/start-master.sh' passfail:true}]->(deep1:Machine)'
	//use the cypher function to create neo4j graph

	def add(connections:RDD[String]){
		//adds each line from the dataframe to the neo context
		connections.foreach(line => neo.cypher("CREATE "+line))
	}
	
}