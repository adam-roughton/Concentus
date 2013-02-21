package com.adamroughton.consentus.crowdhammer.metriclistener;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.EventType;
import com.adamroughton.consentus.messaging.events.StateMetricEvent;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.collections.Histogram;

public class MetricProcessor implements EventHandler<byte[]>, LifecycleAware {

	private final StateMetricEvent _metricEvent = new StateMetricEvent();
	private Histogram _histogram;
	
	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch)
			throws Exception {
		if (!isValid(event)) {
			return;
		}

		if (MessageBytesUtil.readInt(event, 1) == EventType.STATE_METRIC.getId()) {
			_metricEvent.setBackingArray(event, 1);
			long actionsProcessed = _metricEvent.getInputActionsProcessed();
			long duration = _metricEvent.getDurationInMs();
			
			double throughput = 0;
			if (duration > 0) {
				throughput = ((double) actionsProcessed / (double) duration) * 1000;
				//_histogram.addObservation(throughput);
			}
			if (sequence % 100 == 0) {
				Log.info(String.format("Throughput: %f per second", throughput));
				
				
				/*Log.info(String.format("Mean: %s, Max: %d, Min: %d, 99.00%%: %d, 99.99%%: %d", 
						_histogram.getMean().toEngineeringString(),
						_histogram.getMax(),
						_histogram.getMin(),
						_histogram.getTwoNinesUpperBound(),
						_histogram.getFourNinesUpperBound()
						));*/
			}
			_metricEvent.clear();
		}
	}

	@Override
	public void onStart() {
		long[] upperBounds = new long[10000];
		for (int i = 0; i < 10000; i++) {
			upperBounds[i] = (i + 1) * 10;
		}
		_histogram = new Histogram(upperBounds);
	}

	@Override
	public void onShutdown() {
		// TODO Auto-generated method stub
		
	}
	
	private static boolean isValid(byte[] event) {
		return !MessageBytesUtil.readFlagFromByte(event, 0, 0);
	}

}
