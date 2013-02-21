package com.adamroughton.consentus.clienthandler;

import com.esotericsoftware.minlog.*;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.EventListenerBase;
import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.lmax.disruptor.RingBuffer;

class ClientEventListener extends EventListenerBase {
	
	private final int _listenPort;
	
	public ClientEventListener(
			final ZMQ.Context zmqContext, 
			final RingBuffer<byte[]> ringBuffer, 
			final Config conf, 
			final FatalExceptionCallback exCallback) {
		super(zmqContext, ringBuffer, conf, exCallback);
		
		String portString = conf.getCanonicalSubPort();
		_listenPort = Integer.parseInt(portString);
		Util.assertPortValid(_listenPort);
	}

	@Override
	protected Socket doConnect(Context zmqContext, Config conf)
			throws Exception {
		ZMQ.Socket input = zmqContext.socket(ZMQ.SUB);
		input.setHWM(100);
		input.bind("tcp://*:" + _listenPort);
		input.subscribe(new byte[0]);
		Log.info(String.format("Listening on port %d", _listenPort));
		return input;
	}
}