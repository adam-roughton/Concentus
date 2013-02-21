package com.adamroughton.consentus.canonicalstate;

import java.util.Objects;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

class Publisher implements EventHandler<byte[]>, LifecycleAware {

	private final ZMQ.Context _zmqContext;
	private final int _publishPort;

	private ZMQ.Socket _pub;
	
	public Publisher(ZMQ.Context zmqContext,
			Config conf) {
		_zmqContext = Objects.requireNonNull(zmqContext);
		
		_publishPort = Integer.parseInt(conf.getCanonicalStatePubPort());
		Util.assertPortValid(_publishPort);
	}
	
	@Override
	public void onStart() {
		_pub = _zmqContext.socket(ZMQ.PUB);
		_pub.setHWM(100);
		_pub.bind("tcp://*:" + _publishPort);
		Log.info(String.format("Publishing on port %d", _publishPort));
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
			_pub.send(getSubId(event), ZMQ.SNDMORE);
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

	private byte[] getSubId(byte[] event) {
		byte[] subId = new byte[4];
		System.arraycopy(event, 1, subId, 0, 4);
		return subId;
	}
	
}


