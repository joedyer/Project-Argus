//handler for all the modules related to system logs

package handlers

import logData._ 
import org.apache.spark.rdd.RDD

class LogHandler(){

	def su(log:RDD[String]): RDD[String] ={
		//input will be the entire log
		//strip all lines but those relevant to becoming a super user
		//example line:
		//Nov  9 09:55:39 deep1 su: pam_unix(su:session): session opened for user root by daw(uid=3001)
		//call module with result as argument
		//return result from module
		
		var superuser = new SuperUser();
		var newlog = log.filter(line => line.contains(": session opened for user root by"))
		var result = superuser.run(newlog)
		return result
		
	}

	def superaction(log:RDD[String]): RDD[String]={
		//input will be the entire log
		//strip all lines but those relevant to a super user sending an action to the host
		//example line:
		//Nov  2 14:33:46 deep1 sudo: bullards : TTY=pts/5 ; PWD=/home/bullards/1000Genome_safe ; USER=root ; COMMAND=/opt/spark-2.0/sbin/start-master.sh
		//call module with result as argument
		//return result from module

		var superuseraction = new SuperUserAction();
		var newlog = log.filter(line => line.contains(": TTY="))
		var result = superuseraction.run(newlog)
		return result

	}

	def logon(log:RDD[String]): RDD[String]={
		//input will be the entire log
		//strip all lines but those relevant to a user logging in 
		//example line:
		//Sep 29 09:19:00 deep1 sshd[282354]: Accepted password for maharjand from 172.17.17.52 port 58659 ssh2
		//call module with result as argument
		//return result from module

		var logon = new LogOnOff();
		var newlog = log.filter(line => line.contains("Accepted password for"))
		var result = logon.run(newlog)
		return result
	}
	
	def passFail(log:RDD[String]): RDD[String]={
		//input will be the entire log
		//strip all lines but those relevant to a user (whether the username is valid or not) failing a log in attempt.
		//example line:
		//Sep 30 16:47:47 deep1 sshd[254414]: Failed password for daw from 10.1.10.40 port 38938 ssh2
		//call module with result as argument
		//return result from module

		var passFail = new PasswordFailed();
		var newlog = log.filter(line => line.contains("Failed password for"))
		var result = passFail.run(newlog)
		return result
	}

	def run(log:RDD[String]):Array[RDD[String]] ={
		//run the parsers
		var superuser = su(log);
		var superuseraction = superaction(log);
		var logonrdd = logon(log);
		var pf = passFail(log)

		//place resultant dataframes into the array
		return Array(superuser, superuseraction, logonrdd, pf); 

		//test comment
	}

}