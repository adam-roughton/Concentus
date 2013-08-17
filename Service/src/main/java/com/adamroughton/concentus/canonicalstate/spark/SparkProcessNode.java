package com.adamroughton.concentus.canonicalstate.spark;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.cli.Option;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.ConcentusWorkerNode;
import com.adamroughton.concentus.canonicalstate.CanonicalStateService;
import com.adamroughton.concentus.cluster.worker.ClusterListener;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.metric.MetricContext;

public class SparkProcessNode implements ConcentusWorkerNode<Configuration, ConcentusServiceState> {

	public static void main(String[] args) {
		ConcentusExecutableOperations.executeClusterWorker(args, new SparkProcessNode());
	}

	@Override
	public Iterable<Option> getCommandLineOptions() {
		// spark home; type = {master, worker}
		
		
		return Collections.emptyList();
	}

	@Override
	public String getProcessName() {
		return "Canonical State";
	}

	@Override
	public Class<Configuration> getConfigType() {
		return Configuration.class;
	}

	@Override
	public <TBuffer extends ResizingBuffer> ClusterListener<ConcentusServiceState> createService(
			Map<String, String> commandLineOptions,
			ConcentusHandle<? extends Configuration, TBuffer> handle, 
			MetricContext metricContext) {
		return new CanonicalStateService<>(handle, metricContext);
	}

	@Override
	public Class<ConcentusServiceState> getClusterStateClass() {
		return ConcentusServiceState.class;
	}
	
}
