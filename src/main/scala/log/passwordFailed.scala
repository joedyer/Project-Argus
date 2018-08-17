//this module is responsible for recording all failed login attempts

package logData

import org.apache.spark.rdd.RDD

case class PasswordFailure(username: String, ip: String, time: String, machine: String)

class PasswordFailed(){

	def createDF(sublog:RDD[String]):RDD[PasswordFailure]={
		//this will generate a dataframe from the strings of the log
		//input will be in the from of whole strings that look like this:
		//Sep 30 16:47:47 deep1 sshd[254414]: Failed password for daw from 10.1.10.40 port 38938 ssh2
		//output will correspond with the case class i.e.
		//PasswordFailed('daw', '10.1.10.40', 'Sep 30 16:47:47', 'deep1')

		return sublog.map(line => {
			var time = line.substring(0,15)
			var machine = line.substring(16, line.indexOf(" sshd"))
			var user = line.substring((line.indexOf("password for")+13), line.indexOf(" from"))
			var ip = line.substring((line.indexOf("from")+5), line.indexOf(" port"))
			PasswordFailure(user, time, ip, machine)
			});
	}

	def createStrings(df:RDD[PasswordFailure]): RDD[String]={
		//this will create the strings used by cypher 
		//takes in a dataframe of the case class
		//returns a dataframe of strings in the form of this:
		//'(username:User) -[:PasswordFailed {time:timestamp, ip:ip#}]->(hostname:Machine)'
		//username, timestamp, the ip number, and hostname are take from the case class

		return df.map(row => "("+row.username+":User)-[:PasswordFailed {time:"+row.time+", ip:"+row.ip+"}]->("+row.machine+":Machine)") 

	}

	def run( sublog:RDD[String]): RDD[String]={
		//This will be called by the log handler
		//a subsection of the system log in the form of an RDD of strings
		//call createDF with the subsection, call create strings with the result, return that result

		return createStrings(createDF(sublog));
	}

}
