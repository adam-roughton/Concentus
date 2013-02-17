package com.adamroughton.consentus.canonicalstate;

import java.util.Objects;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.Util;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

//class Publisher implements Runnable {
//
//	private final RingBuffer<byte[]> _pubDisruptor;
//
//	private final ZMQ.Context _zmqContext;
//	private final int _publishPort;
//
//	private final Sequence _sequence;
//	private final SequenceBarrier _barrier;
//
//	public Publisher(ZMQ.Context zmqContext, 
//			RingBuffer<byte[]> pubDisruptor, 
//			SequenceBarrier barrier,
//			Config conf) {
//		_zmqContext = Objects.requireNonNull(zmqContext);
//		_pubDisruptor = Objects.requireNonNull(pubDisruptor);
//
//		_publishPort = Integer.parseInt(conf.getCanonicalStatePubPort());
//		Util.assertPortValid(_publishPort);
//
//		_barrier = Objects.requireNonNull(barrier);
//		_sequence = new Sequence();
//	}
//
//	@Override
//	public void run() {
//		ZMQ.Socket pub = _zmqContext.socket(ZMQ.PUB);
//		pub.setHWM(100);
//		pub.bind("tcp://localhost:" + _publishPort);
//
//		try {
//			while (!Thread.interrupted()) {
//				publishNext(pub);
//			}
//		} catch (ZMQException eZmq) {
//			// check that the socket hasn't just been closed
//			if (eZmq.getErrorCode() != ZMQ.Error.ETERM.getCode()) {
//				throw eZmq;
//			}
//		} finally {
//			try {
//				pub.close();
//			} catch (Exception eClose) {
//				Log.warn("Exception thrown when closing ZMQ socket.", eClose);
//			}
//		}
//	}
//
//	Sequence getSequence() {
//		return _sequence;
//	}
//
//	private void publishNext(ZMQ.Socket pub) {
//
//	}
//
//
//
//}

class Publisher implements EventHandler<byte[]>, LifecycleAware {

	private final ZMQ.Context _zmqContext;
	private final int _publishPort;
	private final ExceptionHandler _exceptionHandler = new ExceptionHandler() {
		
		@Override
		public void handleOnStartException(Throwable ex) {
			Log.error("Error Starting Publisher", ex);
		}
		
		@Override
		public void handleOnShutdownException(Throwable ex) {
		}
		
		@Override
		public void handleEventException(Throwable ex, long sequence, Object event) {
			// check that the socket hasn't just been closed
			if (ex instanceof ZMQException) {
				if (((ZMQException)ex).getErrorCode() == ZMQ.Error.ETERM.getCode()) {
					return;
				}
			}
			Log.error("Error during publishing", ex);
		}
	};

	private ZMQ.Socket _pub;
	
	public Publisher(ZMQ.Context zmqContext, 
			RingBuffer<byte[]> pubDisruptor, 
			SequenceBarrier barrier,
			Config conf) {
		_zmqContext = Objects.requireNonNull(zmqContext);
		
		_publishPort = Integer.parseInt(conf.getCanonicalStatePubPort());
		Util.assertPortValid(_publishPort);
	}
	
	@Override
	public void onStart() {
		_pub = _zmqContext.socket(ZMQ.PUB);
		_pub.setHWM(100);
		_pub.bind("tcp://localhost:" + _publishPort);
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
		
	}
	
	// replay?
	// open another socket and listen? Then rebroadcast, or just send updates to the requester?
	// what if more than one update was requested? Do we send them all in one?
	
}


