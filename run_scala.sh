#!/bin/bash

pushd `dirname $0` &> /dev/null

#define all variables here
THREADS=12
SYSLOG=""
PCAP=""

#Start While loop for args
while test $# -gt 0; do
	case "$1" in
		-h|--help)
			echo "OPTIONS"
			echo " "
			echo "-h, --help  help page"
			echo "-t, --threads [integer] specifies the number of threads to be used"
			echo "-l, --syslog [String] specifies filepath for a system log to be scanned"
			echo "-p, --pcap [String] specifies filepath for a network traffic log to be scanned"
			exit 0
			;;
		-t|--threads)
			shift
			THREADS=$1
			shift
			;;
		-l|--syslog)
			shift
			echo "syslog set to "
			echo $1
			SYSLOG=$1
			shift
			;;
		-p|--pcap)
			shift
			echo "pcap set to "
			echo $1
			PCAP=$1
			shift
			;;
		*)
			break
			;;
	esac
done



echo -e "\e[91m\e[1mUsing $THREADS Threads\e[0m"

echo -e "\e[91m\e[1mAdjust Java Heap Space...\e[0m"
export _JAVA_OPTIONS="-Xmx100g"

echo -e "\e[91m\e[1mPackaging to Jar...\e[0m"
/opt/sbt/bin/sbt package

echo -e "\e[91m\e[1mRunning via spark-submit\e[0m"
/opt/spark-2.0/bin/spark-submit --class "run_this" --master local[$THREADS] --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --driver-memory 100g --verbose target/scala-2.10/argus_2-0_2.10-2.0.1.jar $SYSLOG $PCAP

popd &> /dev/null

