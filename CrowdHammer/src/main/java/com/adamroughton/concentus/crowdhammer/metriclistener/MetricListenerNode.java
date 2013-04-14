package com.adamroughton.concentus.crowdhammer.metriclistener;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusExecutableOperations.FactoryDelegate;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusNode;
import com.adamroughton.concentus.ConcentusProcessFactory;
import com.adamroughton.concentus.NoArgsConcentusProcessFactory;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;

public class MetricListenerNode implements ConcentusNode<MetricListenerService, CrowdHammerConfiguration> {

	public static void main(String[] args) {
		ConcentusExecutableOperations.executeClusterWorker(args, new MetricListenerNode());
	}

	@Override
	public ConcentusProcessFactory<MetricListenerService, CrowdHammerConfiguration> getProcessFactory() {
		return new NoArgsConcentusProcessFactory<>("Metric Listener", 
			new FactoryDelegate<MetricListenerService, CrowdHammerConfiguration>() {

			@Override
			public MetricListenerService create(
					ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle) {
				return new MetricListenerService(concentusHandle);
			}
		}, 
		MetricListenerService.class,
		CrowdHammerConfiguration.class);
	}

}
