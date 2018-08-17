//module dedicated to the actions of superusers, but not su. really any action that begins 'sudo'

package logData

import org.apache.spark.rdd.RDD

case class SuperAction(username: String, time: String, machine: String, command: String, passfail: Boolean)

class SuperUserAction(){
    

    def createDF(sublog:RDD[String]): RDD[SuperAction] ={
		//this will generate a dataframe from the strings of the log
		//input will be in the form of whole strings that look like this:
		//Nov  2 14:33:46 deep1 sudo: bullards : TTY=pts/5 ; PWD=/home/bullards/1000Genome_safe ; USER=root ; COMMAND=/opt/spark-2.0/sbin/start-master.sh
		//output will correspond with the case class i.e.
		//Su('bullards', 'Nov 2 14:33:46', 'deep1', '/opt/spark-2.0/sbin/start-master.sh', true)
		return sublog.map(line => {
			var time = line.substring(0,15)
			var machine = line.substring(16, line.indexOf("sudo:"))
			var passfail = true
			var user = line.substring((line.indexOf("sudo:")+6), line.indexOf(" : "))
			var command = line.substring((line.indexOf("COMMAND=")+8))
			SuperAction(user, time, machine, command, passfail)
			});
	}

	def createStrings(rdd:RDD[SuperAction]):RDD[String] = {
		//this will create the strings used by cypher 
		//takes in a dataframe of the case class
		//returns a dataframe of strings in the form of this:
		//'(username:User) -[:SuperAction {time:timestamp, command:commandgiven passfail:true/false}]->(hostname:Machine)'
		//username, timestamp, the boolean value, commandgiven, and hostname are take from the case class

		return rdd.map(row => "("+row.username+":User)-[:SuperUserAction {time:"+row.time+", passfail:"+row.passfail.toString()+", command:"+row.command+"}]->("+row.machine+":Machine)") 
	}

	def run( sublog:RDD[String]):RDD[String] = {
		//This will be called by the log handler
		//a subsection of the system log in the form of an RDD of strings
		//call createDF with the subsection, call create strings with the result, return that result

		return createStrings(createDF(sublog));
	}

}

