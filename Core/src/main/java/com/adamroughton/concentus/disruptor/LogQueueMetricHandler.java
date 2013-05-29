package com.adamroughton.concentus.disruptor;

import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.minlog.Log;

public class LogQueueMetricHandler implements QueueMetricHandler {

	@Override
	public void onMetric(QueueMetric queueMetric) {
		Log.info(String.format("'%s' Queue Stats", queueMetric.getQueueName()));
		Util.statsToString("    queue time", queueMetric.getQueueTimeStats());
		Util.statsToString("    capacity", queueMetric.getQueueCapacityStats());
	}

}
