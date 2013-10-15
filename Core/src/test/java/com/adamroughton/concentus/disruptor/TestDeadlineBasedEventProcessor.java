package com.adamroughton.concentus.disruptor;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.DrivableClock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.data.BytesUtil;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;

import static org.junit.Assert.*;

public class TestDeadlineBasedEventProcessor {

	private RingBuffer<byte[]> _ringBuffer;
	private DeadlineBasedEventProcessor<byte[]> _eventProcessor;
	private ValidatingEventHandler _validatingEventHandler;
	private ExecutorService _executor;
	private FatalExceptionCallback _exceptionCallback;
	private DrivableClock _drivableClock;
	
	@Before
	public void setUp() {
		_ringBuffer = RingBuffer.createSingleProducer(new EventFactory<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[512];
			}
			
		}, 2048, new BlockingWaitStrategy());
		SequenceBarrier barrier = _ringBuffer.newBarrier();
		_exceptionCallback = new FatalExceptionCallback() {
			
			@Override
			public void signalFatalException(Throwable exception) {
			}
		};
		_drivableClock = new DrivableClock();
		_validatingEventHandler = new ValidatingEventHandler(_drivableClock);
		_eventProcessor = new DeadlineBasedEventProcessor<>(_drivableClock, 
				_validatingEventHandler, _ringBuffer, barrier, _exceptionCallback);
		_executor = Executors.newCachedThreadPool();
	}
	
	@After
	public void tearDown() {
		_executor.shutdown();
	}
	
	private static interface DeadlineDelegate {
		long nextDeadline(Clock clock);
	}
	
	private static class ValidatingEventHandler implements DeadlineBasedEventHandler<byte[]> {

		private final Clock _clock;
		private DeadlineDelegate _delegate;
		private long _contentXor;
		private long _nextDeadline;
		private volatile int _deadlineCount = 0;
		
		public ValidatingEventHandler(Clock clock) {
			_clock = Objects.requireNonNull(clock);
			_delegate = new DeadlineDelegate() {
				
				@Override
				public long nextDeadline(Clock clock) {
					return Long.MAX_VALUE;
				}
			};
		}
		
		public void setDeadlineDelegate(DeadlineDelegate delegate) {
			_delegate = Objects.requireNonNull(delegate);
		}
		
		@Override
		public void onEvent(byte[] event, long sequence, boolean isEndOfBatch)
				throws Exception {
			long eventContent = BytesUtil.readLong(event, 0);
			_contentXor = xorRotate(_contentXor, eventContent);
		}

		@Override
		public void onDeadline() {
			_deadlineCount++;
		}

		@Override
		public long moveToNextDeadline(long pendingCount) {
			_nextDeadline = _delegate.nextDeadline(_clock);
			return _nextDeadline;
		}

		@Override
		public long getDeadline() {
			return _nextDeadline;
		}

		@Override
		public String name() {
			return "ValidatingEventHandler";
		}
		
		public long getContentXor() {
			return _contentXor;
		}
		
		public int getDeadlineInvocationCount() {
			return _deadlineCount;
		}
		
	}

	@Test(timeout=10000)
	public void noDeadlines() {
		_executor.submit(_eventProcessor);
		
		long eventCount = 10000l;
		
		byte[] buffer;
		long expectedXor = 0;
		for (long val = 0; val < eventCount; val++) {
			expectedXor = xorRotate(expectedXor, val);
			long seq = _ringBuffer.next();
			try {
				buffer = _ringBuffer.get(val);
				BytesUtil.writeLong(buffer, 0, val);
			} finally {
				_ringBuffer.publish(seq);
			}
		}
		while (_eventProcessor.getSequence().get() < eventCount - 2);
		assertEquals(expectedXor, _validatingEventHandler.getContentXor());
		assertEquals(0, _validatingEventHandler.getDeadlineInvocationCount());
	}
	
	@Test(timeout=10000)
	public void oneDeadline() {
		final long eventCount = 10000l;
		
		_validatingEventHandler.setDeadlineDelegate(new DeadlineDelegate() {
			
			boolean afterFirst = false;
			
			@Override
			public long nextDeadline(Clock clock) {
				if (afterFirst) {
					return Long.MAX_VALUE;
				} else {
					afterFirst = true;
					return eventCount / 2;
				}
			}
		});
		_executor.submit(_eventProcessor);
		
		byte[] buffer;
		long expectedXor = 0;
		for (long val = 0; val < eventCount; val++) {
			_drivableClock.setTime(val, TimeUnit.MILLISECONDS);
			expectedXor = xorRotate(expectedXor, val);
			long seq = _ringBuffer.next();
			try {
				buffer = _ringBuffer.get(val);
				BytesUtil.writeLong(buffer, 0, val);
			} finally {
				_ringBuffer.publish(seq);
			}
		}
		while (_eventProcessor.getSequence().get() < eventCount - 2);
		assertEquals(expectedXor, _validatingEventHandler.getContentXor());
		assertEquals(1, _validatingEventHandler.getDeadlineInvocationCount());
	}
	
	@Test(timeout=10000)
	public void multipleDeadlines() {
		final long eventCount = 10000l;
		
		_validatingEventHandler.setDeadlineDelegate(new DeadlineDelegate() {
			
			int nextIndex = 0;
			long base = eventCount / 6;
			long[] deadlines = new long[] { base, 2 * base, 3 * base, 4 * base, 5 * base, Long.MAX_VALUE};
			
			@Override
			public long nextDeadline(Clock clock) {
				return deadlines[nextIndex++];
			}
		});
		_executor.submit(_eventProcessor);
		
		byte[] buffer;
		long expectedXor = 0;
		for (long val = 0; val < eventCount; val++) {
			_drivableClock.setTime(val, TimeUnit.MILLISECONDS);
			expectedXor = xorRotate(expectedXor, val);
			long seq = _ringBuffer.next();
			try {
				buffer = _ringBuffer.get(val);
				BytesUtil.writeLong(buffer, 0, val);
			} finally {
				_ringBuffer.publish(seq);
			}
		}
		while (_eventProcessor.getSequence().get() < eventCount - 2);
		assertEquals(expectedXor, _validatingEventHandler.getContentXor());
		assertEquals(5, _validatingEventHandler.getDeadlineInvocationCount());
	}
	
	private static long xorRotate(long xor, long value) {
		xor ^= value;
		xor = xor << 1 | xor >> 63;
		return xor;
	}
	
}
