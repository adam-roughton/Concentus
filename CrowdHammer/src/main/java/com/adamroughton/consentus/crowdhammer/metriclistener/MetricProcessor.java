package com.adamroughton.consentus.crowdhammer.metriclistener;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.StateMetricEvent;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class MetricProcessor implements EventHandler<byte[]>, LifecycleAware {

	private final StateMetricEvent _metricEvent = new StateMetricEvent();
	
	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch)
			throws Exception {
		if (!isValid(event)) {
			return;
		}

		_metricEvent.setBackingArray(event, 1);
		Log.info(String.format("Metric Received: %d", _metricEvent.getTimeDuration()));
		_metricEvent.clear();
	}

	@Override
	public void onStart() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onShutdown() {
		// TODO Auto-generated method stub
		
	}
	
	private static boolean isValid(byte[] event) {
		return !MessageBytesUtil.readFlagFromByte(event, 0, 0);
	}

}
