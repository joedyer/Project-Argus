//responsible for tracking users logging on

package logData

import org.apache.spark.rdd.RDD

case class LogOn(username: String, time: String, machine: String, ip: String, port: String, protocol: String)

class LogOnOff(){

	def createDF(sublog:RDD[String]): RDD[LogOn] ={
		//this will generate a dataframe from the strings of the log
		//input will be in the from of whole strings that look like this:
		//Sep 29 09:19:00 deep1 sshd[282354]: Accepted password for maharjand from 172.17.17.52 port 58659 ssh2
		//output will correspond with the case class i.e.
		//logon('maharjand', 'Sep 29 09:19:00', 'deep1', '172.17.17.52', '58659', 'ssh2')

		return sublog.map(line => {
			var time = line.substring(0,15)
			var machine = line.substring(16, line.indexOf(" sshd"))
			var user = line.substring((line.indexOf("password for")+13), line.indexOf(" from"))
			var ip = line.substring((line.indexOf("from")+5), line.indexOf(" port"))
			var port = line.substring((line.indexOf("port")+5), line.lastIndexOf(" "))
			var protocol = line.substring(line.lastIndexOf(" "))
			LogOn(user, time, machine, ip, port, protocol)
			});
	}

	def createStrings(df:RDD[LogOn]): RDD[String]={
		//this will create the strings used by cypher 
		//takes in a dataframe of the case class
		//returns a dataframe of strings in the form of this:
		//'(username:User) -[:Logon {time:timestamp, ip:ip#, port:port#, protocal:protocal}]->(hostname:Machine)'
		//username, timestamp, the ip number, the port number, the protocal, and hostname are take from the case class

		return df.map(row => "("+row.username+":User)-[:LogOn {time:"+row.time+", ip:"+row.ip+", port: "+row.port+", protocal:"+row.protocol+"}]->("+row.machine+":Machine)") 
	}

	def run( sublog:RDD[String]):RDD[String]={
		//This will be called by the log handler
		//a subsection of the system log in the form of an RDD of strings
		//call createDF with the subsection, call create strings with the result, return that result
		return createStrings(createDF(sublog));
	}

}
