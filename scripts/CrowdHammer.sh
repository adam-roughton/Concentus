#!/bin/bash

function printUsage {
	echo "Usage :"
	echo "    $0 Coordinator ZooKeeperAddress"
	echo " OR"
	echo "    $0 Guardian ZooKeeperAddress"
}

if [ -z "$1" ]
then
    printUsage
    exit
fi

ADDRESS=`curl http://169.254.169.254/latest/meta-data/local-ipv4`

if [[ $1 = "Coordinator" ]] || [[ $1 = "Guardian" ]]
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
   java -cp $CLASSPATH -Djava.library.path=/usr/local/lib -Xmx4g -XX:+UseCompressedOops -server -d64 com.adamroughton.concentus.crowdhammer.CrowdHammerCli -zkaddr $ZOO_KEEPER_ADDRESS -hostaddr $ADDRESS
else
   java -cp $CLASSPATH -Djava.library.path=/usr/local/lib com.adamroughton.concentus.cluster.worker.Guardian -zkaddr $ZOO_KEEPER_ADDRESS -hostaddr $ADDRESS -svmargs "-Djava.library.path=/usr/local/lib -Xmx4g -XX:+UseCompressedOops -server -d64"
fi

# return to the original directory
popd