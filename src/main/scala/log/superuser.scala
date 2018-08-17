//module dedicated to the processing of users becoming superusers, but not the actions of superusers

package logData

import org.apache.spark.rdd.RDD

case class Su(username: String, time: String, machine: String, passfail: Boolean)

class SuperUser() {

	def createDF(sublog:RDD[String]): RDD[Su]={
		//this will generate a dataframe from the strings of the log
		//input will be in the from of whole strings that look like this:
		//Nov  9 09:55:39 deep1 su: pam_unix(su:session): session opened for user root by daw(uid=3001)
		//output will correspond with the case class i.e.
		//Su('daw', 'Nov 9 09:55:39', 'deep1', true)

		return sublog.map(line => {
			var time = line.substring(0,15)
			var machine = line.substring(16, line.indexOf("su"))
			var passfail = true
			var user = line.substring((line.indexOf("by")+3), line.indexOf("(uid="))
			Su(user, time, machine, passfail)
			});
	}

	def createStrings(df:RDD[Su]): RDD[String]={
		//this will create the strings used by cypher 
		//takes in a dataframe of the case class
		//returns a dataframe of strings in the form of this:
		//'(username:User) -[:SU {time:timestamp, passfail:true/false}]->(hostname:Machine)'
		//username, timestamp, the boolean value, and hostname are take from the case class

		return df.map(row => "("+row.username+":User)-[:SU {time:"+row.time+", passfail:"+row.passfail.toString()+"}]->("+row.machine+":Machine)") 
	}

	def run( sublog:RDD[String]): RDD[String]={
		//This will be called by the log handler
		//a subsection of the system log in the form of an RDD of strings
		//call createDF with the subsection, call create strings with the result, return that result

		return createStrings(createDF(sublog))
	}

}


