package com.adamroughton.consentus.clienthandler;

import com.esotericsoftware.minlog.*;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.EventListenerBase;
import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.messaging.events.EventType;
import com.lmax.disruptor.RingBuffer;

import static com.adamroughton.consentus.Util.*;

class InternalEventListener extends EventListenerBase {
	
	private final int _canonicalStatePubPort;
	
	public InternalEventListener(
			final ZMQ.Context zmqContext, 
			final RingBuffer<byte[]> ringBuffer, 
			final Config conf, 
			final FatalExceptionCallback exCallback) {
		super(zmqContext, ringBuffer, conf, exCallback);
		
		String portString = conf.getCanonicalStatePubPort();
		_canonicalStatePubPort = Integer.parseInt(portString);
		Util.assertPortValid(_canonicalStatePubPort);
	}

	@Override
	protected Socket doConnect(Context zmqContext, Config conf)
			throws Exception {
		ZMQ.Socket input = zmqContext.socket(ZMQ.SUB);
		input.setHWM(100);
		input.connect("tcp://127.0.0.1:" + _canonicalStatePubPort);
		input.subscribe(getSubscriptionBytes(EventType.STATE_UPDATE));
		Log.info(String.format("Subscribed to tcp://127.0.0.1:%d", _canonicalStatePubPort));
		return input;
	}
}