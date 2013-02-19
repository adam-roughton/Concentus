package com.adamroughton.consentus.crowdhammer.worker;

import com.esotericsoftware.minlog.*;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.EventListenerBase;
import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.lmax.disruptor.RingBuffer;

class UpdateListener extends EventListenerBase {
	
	private final int _updatePort;
	
	public UpdateListener(
			final ZMQ.Context zmqContext, 
			final RingBuffer<byte[]> ringBuffer, 
			final Config conf, 
			final FatalExceptionCallback exCallback) {
		super(zmqContext, ringBuffer, conf, exCallback);
		
		String portString = conf.getCanonicalStatePubPort();
		_updatePort = Integer.parseInt(portString);
		Util.assertPortValid(_updatePort);
	}

	@Override
	protected Socket doConnect(Context zmqContext, Config conf)
			throws Exception {
		ZMQ.Socket input = zmqContext.socket(ZMQ.SUB);
		input.setHWM(100);
		input.connect("tcp://127.0.0.1:" + _updatePort);
		input.subscribe(new byte[0]);
		Log.info(String.format("Listening for updates on port %d", _updatePort));
		return input;
	}
}