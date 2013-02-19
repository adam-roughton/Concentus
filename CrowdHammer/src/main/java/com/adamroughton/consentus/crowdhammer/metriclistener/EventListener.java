package com.adamroughton.consentus.crowdhammer.metriclistener;

import com.esotericsoftware.minlog.*;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.EventListenerBase;
import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.crowdhammer.TestConfig;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.EventType;
import com.lmax.disruptor.RingBuffer;

class EventListener extends EventListenerBase {
	
	private final int _metricsPort;
	private final int _testMetricsSubPort;
	
	public EventListener(
			final ZMQ.Context zmqContext, 
			final RingBuffer<byte[]> ringBuffer, 
			final TestConfig conf, 
			final FatalExceptionCallback exCallback) {
		super(zmqContext, ringBuffer, conf, exCallback);
		
		String portString = conf.getCanonicalStatePubPort();
		_metricsPort = Integer.parseInt(portString);
		Util.assertPortValid(_metricsPort);
		
		String testMetricSubPort = conf.getTestMetricSubPort();
		_testMetricsSubPort = Integer.parseInt(testMetricSubPort);
		Util.assertPortValid(_testMetricsSubPort);
	}

	@Override
	protected Socket doConnect(Context zmqContext, Config conf)
			throws Exception {
		ZMQ.Socket input = zmqContext.socket(ZMQ.SUB);
		input.setHWM(100);
		input.bind("tcp://127.0.0.1:" + _testMetricsSubPort);
		input.connect("tcp://127.0.0.1:" + _metricsPort);
		
		byte[] subId = new byte[4];
		MessageBytesUtil.writeInt(subId, 0, EventType.STATE_METRIC.getId());
		input.subscribe(subId);
		
		Log.info(String.format("Connected to port %d", _metricsPort));
		return input;
	}
}