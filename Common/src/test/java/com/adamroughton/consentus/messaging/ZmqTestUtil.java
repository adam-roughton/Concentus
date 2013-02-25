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
				int offset = (int) args[1];
				int bufferLength = (int) args[2];
				int length = content.length > bufferLength? bufferLength : content.length;
				System.arraycopy(content, 0, array, offset, length);
				return length;
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
		
		private boolean _raiseEterm = false;
		private boolean _interrupt = false;
		private boolean _hasBlocked = false;
		
		public boolean waitForBlockingCall(long timeout, TimeUnit unit) throws InterruptedException {
			boolean success = false;
			_lock.lock();
			try {
				if (timeout >= 0) {
					long endTime = System.nanoTime() + unit.toNanos(timeout);
					long remainingNanos;
					while ((remainingNanos = endTime - System.nanoTime()) > 0 && !_hasBlocked) {
						success = _callerHasBlockedCond.await(remainingNanos, TimeUnit.NANOSECONDS);
					}
				} else {
					while(!_hasBlocked) {
						_callerHasBlockedCond.await();
						success = true;
					}
				}
			} finally {
				_lock.unlock();
			}
			return success;
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
		
		public void interruptBlockedCall() {
			_lock.lock();
			try {
				if (_hasBlocked) {
					_interrupt = true;
				}
				_blockCond.signal();
			} finally {
				_lock.unlock();
			}
		}
		
		public void raiseEtermOnBlockedCall() {
			_lock.lock();
			try {
				// only raise if actually blocked
				if (_hasBlocked) {
					_raiseEterm = true;
				}
				_blockCond.signal();
			} finally {
				_lock.unlock();
			}
		}
		
		public void block() {
			boolean raiseEterm;
			boolean interrupt;
			
			_lock.lock();
			try {
				_hasBlocked = true;
				_callerHasBlockedCond.signalAll();
				try {
					_blockCond.await();
				} catch (InterruptedException eInterrupted) {
					Thread.currentThread().interrupt();
				}
				raiseEterm = _raiseEterm;
				interrupt = _interrupt;
			} finally {
				_lock.unlock();
			}
			if (raiseEterm) {
				throw new ZMQException("Fake Socket Close", (int) ZMQ.Error.ETERM.getCode());
			} else if (interrupt) {
				Thread.currentThread().interrupt();
			}
		}
		
	}
	
	
}
