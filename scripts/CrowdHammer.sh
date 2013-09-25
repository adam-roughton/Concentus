#!/bin/bash

function printUsage {
	echo "Usage :"
	echo "    $0 Coordinator"
	echo " OR"
	echo "    $0 Guardian ZooKeeperAddress"
}

if [ -z "$1" ]
then
    printUsage
    exit
fi

ADDRESS=`curl http://169.254.169.254/latest/meta-data/local-ipv4`

if [[ $1 = "Coordinator" ]]
then
	ZOO_KEEPER_ADDRESS="127.0.0.1"
elif [[ $1 = "Guardian" ]]
then
	# ensure that the zookeeper address is given
	if [ -z "$2" ]
	then
	    echo "error: ZooKeeper address not given"
	    printUsage
	    exit
	fi
	ZOO_KEEPER_ADDRESS=$2
else
	echo "error: Unknown command $1"
	printUsage
	exit
fi

# go to the working directory (where this script is located)
pushd .
cd `dirname $0`

CLASSPATH="concentus-core-1.0-SNAPSHOT.jar:concentus-crowdhammer-1.0-SNAPSHOT.jar:concentus-service-1.0-SNAPSHOT.jar:concentus-sparkstreamingdriver-1.0-SNAPSHOT.jar:concentus-tests-1.0-SNAPSHOT.jar:lib/*"
if [[ $1 = "Coordinator" ]]; then
   # start ZooKeeper
   java -XX:+UseCompressedOops -server -d64 -cp concentus-core-1.0-SNAPSHOT.jar:lib/* com.adamroughton.concentus.cluster.TestZooKeeperProcess 50000 &
   ZPID=$!
   
   java -cp $CLASSPATH -Djava.library.path=/usr/local/lib -Xmx4g -XX:+UseCompressedOops -server -d64 com.adamroughton.concentus.crowdhammer.CrowdHammerCli -zkaddr $ZOO_KEEPER_ADDRESS:50000 -hostaddr $ADDRESS
else
   java -cp $CLASSPATH -Djava.library.path=/usr/local/lib com.adamroughton.concentus.cluster.worker.Guardian -zkaddr $ZOO_KEEPER_ADDRESS:50000 -hostaddr $ADDRESS -svmargs "-Djava.library.path=/usr/local/lib -Xmx2g -XX:+UseCompressedOops -server -d64"
fi

if [[ $1 = "Coordinator" ]]; then
   # kill ZooKeeper
   kill $ZPID
fi

# return to the original directory
popd