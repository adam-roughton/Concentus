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
			long duration = _metricEvent.getTimeDuration();
			
			if (duration > 0) {
				long throughput = actionsProcessed / duration;
				_histogram.addObservation(throughput);
			}
			
			if (sequence % 1000 == 0) {
				StringBuilder sb = new StringBuilder();
				sb.append("Histogram:\n");
				for (int i = 0; i < _histogram.getSize(); i++) {
					sb.append(String.format("%d: %d\n", _histogram.getUpperBoundAt(i), _histogram.getCountAt(i)));
				}
				Log.info(sb.toString());
			}
			_metricEvent.clear();
		}
	}

	@Override
	public void onStart() {
		_histogram = new Histogram(new long[] {10, 100, 1000, 10000});
	}

	@Override
	public void onShutdown() {
		// TODO Auto-generated method stub
		
	}
	
	private static boolean isValid(byte[] event) {
		return !MessageBytesUtil.readFlagFromByte(event, 0, 0);
	}

}
