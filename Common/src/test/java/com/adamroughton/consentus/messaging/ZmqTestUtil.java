package com.adamroughton.consentus.messaging;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class ZmqTestUtil {

	public static Answer<Integer> fakeRecv(final ByteBuffer content) {
		return fakeRecv(content.array());
	}
	
	public static Answer<Integer> fakeRecv(final byte[] content) {
		return new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				Object[] args = invocation.getArguments();
				byte[] array = (byte[]) args[0];
				System.arraycopy(content, 0, array, 0, array.length);
				return array.length;
			}
		};
	}
	
	public static Integer[] repeatReturnVal(int val, int times) {
		Integer[] rVals = new Integer[times];
		for (int i = 0; i < rVals.length; i++) {
			rVals[i] = val;
		}
		return rVals;
	}
	
	public static Answer<Integer> fakeBlockingRecv(final BlockingCall blockingCall) {
		return new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				blockingCall.block();
				return -1;
			}
		};
	}
	
	public static class BlockingCall {
		
		private final Lock _lock = new ReentrantLock();
		private final Condition _blockCond = _lock.newCondition();
		private final Condition _callerHasBlockedCond = _lock.newCondition();
		
		private boolean _isBlocked = false;
		private boolean _raiseEterm = false;
		private boolean _hasBlocked = false;
		
		public void waitForBlockingCall(long timeout, TimeUnit unit) throws InterruptedException {
			_lock.lock();
			try {
				while (!_hasBlocked) {
					if (timeout >= 0) {
						_callerHasBlockedCond.await(timeout, unit);
					} else {
						_callerHasBlockedCond.await();
					}
				}
			} finally {
				_lock.unlock();
			}
		}
		
		public void waitForBlockingCall() throws InterruptedException {
			waitForBlockingCall(-1, TimeUnit.NANOSECONDS);
		}
		
		public void releaseBlockedCall() {
			_lock.lock();
			try {
				_blockCond.signal();
			} finally {
				_lock.unlock();
			}
		}
		
		public void raiseEtermOnBlockedCall() {
			_lock.lock();
			try {
				// only raise if actually blocked
				if (_isBlocked) {
					_raiseEterm = true;
				}
				_blockCond.signal();
			} finally {
				_lock.unlock();
			}
		}
		
		public void block() {
			_lock.lock();
			boolean raiseEterm;
			try {
				_hasBlocked = true;
				_callerHasBlockedCond.signalAll();
				try {
					_blockCond.await();
				} catch (InterruptedException eInterrupted) {
					Thread.currentThread().interrupt();
				}
				raiseEterm = _raiseEterm;
			} finally {
				_lock.unlock();
			}
			if (raiseEterm) {
				throw new ZMQException("Fake Socket Close", (int) ZMQ.Error.ETERM.getCode());
			}
		}
		
	}
	
	
}
