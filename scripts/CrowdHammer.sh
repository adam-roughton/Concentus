#!/bin/bash

function printUsage {
	echo "Usage :"
	echo "    $0 Coordinator"
	echo " OR"
	echo "    $0 InstanceType ZooKeeperAddress [additional flags as per instance]"
    echo "   InstanceTypes: CanonicalState, ClientHandler, MetricListener, Worker"
}

if [ -z "$1" ]
then
    printUsage
    exit
fi

MAIN=""
ADDITIONAL=""
ADDRESS=`curl http://169.254.169.254/latest/meta-data/local-ipv4`

if ! [[ $1 = "Coordinator" ]]
then
	# ensure that the zookeeper address is given
	if [ -z "$2" ]
	then
	    echo "error: ZooKeeper address not given"
	    printUsage
	    exit
	fi
	ZOO_KEEPER_ADDRESS=$2
  	
  	case "$1" in
	  CanonicalState) MAIN="com.adamroughton.concentus.crowdhammer.concentushost.CrowdHammerHostNode"
	                  ADDITIONAL="-c com.adamroughton.concentus.canonicalstate.CanonicalStateNode"
	                  ;;
	                  
	  ClientHandler)  MAIN="com.adamroughton.concentus.crowdhammer.concentushost.CrowdHammerHostNode"
	                  ADDITIONAL="-c com.adamroughton.concentus.clienthandler.ClientHandlerNode"
	                  ;;
	  
	  MetricListener) MAIN="com.adamroughton.concentus.crowdhammer.metriclistener.MetricListenerNode"
	  				  ;;
	  
	  Worker)         MAIN="com.adamroughton.concentus.crowdhammer.worker.WorkerNode"
	  				  if ! [[ "$3" =~ ^[0-9]+$ ]]
					  then
					     echo "Usage : $0 Worker ZooKeeperAddress MaxClientCount"
					     exit
					  fi
					  ADDITIONAL="-n $3"
					  ;;
					  
	  *) echo "Unknown InstanceType '$1'"
	     printUsage
	     exit
	     ;;
	esac
else
	MAIN="com.adamroughton.concentus.crowdhammer.CrowdHammerCoordinatorNode"
	ZOO_KEEPER_ADDRESS="127.0.0.1"
fi

# go to the working directory (where this script is located)
pushd .
cd `dirname $0`

if [[ $1 = "Coordinator" ]]; then
   # start ZooKeeper
   java -cp concentus-core-1.0-SNAPSHOT.jar:lib/* com.adamroughton.concentus.cluster.TestZooKeeperProcess 50000 &
   ZPID=$!
fi
   
# start the node
CLASSPATH="concentus-core-1.0-SNAPSHOT.jar:concentus-crowdhammer-1.0-SNAPSHOT.jar:concentus-service-1.0-SNAPSHOT.jar:lib/*"
java -cp $CLASSPATH -Djava.library.path=/usr/local/lib -Xmx4g $MAIN -z $ZOO_KEEPER_ADDRESS:50000 -p config.yaml -a $ADDRESS $ADDITIONAL

if [[ $1 = "Coordinator" ]]; then
   # kill ZooKeeper
   kill $ZPID
fi

# return to the original directory
popd