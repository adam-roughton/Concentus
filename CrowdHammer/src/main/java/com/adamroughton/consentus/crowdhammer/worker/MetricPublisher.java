package com.adamroughton.consentus.crowdhammer.worker;

import java.util.Objects;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.crowdhammer.TestConfig;
import com.adamroughton.consentus.crowdhammer.messaging.events.TestEventType;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

class MetricPublisher implements EventHandler<byte[]>, LifecycleAware {

	private final ZMQ.Context _zmqContext;
	private final int _testMetricSubPort;

	private ZMQ.Socket _pub;
	
	public MetricPublisher(ZMQ.Context zmqContext,
			TestConfig conf) {
		_zmqContext = Objects.requireNonNull(zmqContext);
		
		_testMetricSubPort = Integer.parseInt(conf.getTestMetricSubPort());
		Util.assertPortValid(_testMetricSubPort);
	}
	
	@Override
	public void onStart() {
		_pub = _zmqContext.socket(ZMQ.PUB);
		_pub.setHWM(100);
		_pub.connect("tcp://127.0.0.1:" + _testMetricSubPort);
		Log.info(String.format("Connecting to sub port %d", _testMetricSubPort));
	}

	@Override
	public void onShutdown() {
		if (_pub != null) {
			try {
				_pub.close();
			} catch (Exception eClose) {
				Log.warn("Exception thrown when closing ZMQ socket.", eClose);
			}
			_pub = null;
		}
	}

	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch)
			throws Exception {
		try {
			// check if the error flag is raised
			if (!isValid(event)) {
				return;
			}
			
			byte[] subId = new byte[4];
			MessageBytesUtil.writeInt(subId, 0, TestEventType.LOAD_METRIC.getId());
			
			_pub.send(subId, ZMQ.SNDMORE);
			_pub.send(event, 1, 0);
		} catch (ZMQException eZmq) {
			if (eZmq.getErrorCode() != ZMQ.Error.ETERM.getCode()) {
				throw eZmq;
			}
		}
	}
	
	private static boolean isValid(byte[] event) {
		return !MessageBytesUtil.readFlagFromByte(event, 0, 0);
	}

}


