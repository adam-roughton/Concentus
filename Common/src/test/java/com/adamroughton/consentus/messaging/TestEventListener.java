package com.adamroughton.consentus.messaging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;
import org.zeromq.ZMQ;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.messaging.EventListener;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.adamroughton.consentus.messaging.events.EventType;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import static org.mockito.Mockito.*;
import static com.adamroughton.consentus.messaging.ZmqTestUtil.*;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TestEventListener {
	
	private ExecutorService _executor;
	
	@Mock private ZMQ.Context _zmqContext;
	@Mock private ZMQ.Socket _zmqSocket;
	private EventListener _eventListener;
	private RingBuffer<byte[]> _disruptor;
	
	private static final int BUFFER_SIZE = 128;
	private static final int EVENT_SIZE = 256;
	
	@Before
	public void setUp() {
		_executor = Executors.newSingleThreadExecutor();
		_disruptor = new RingBuffer<>(new EventFactory<byte[]>() {
			@Override
			public byte[] newInstance() {
				return new byte[256];
			}
		}, BUFFER_SIZE);
		_disruptor.setGatingSequences(new Sequence(BUFFER_SIZE - 1));
		when(_zmqContext.socket(ZMQ.SUB)).thenReturn(_zmqSocket);
		
		SocketSettings socketSetting = SocketSettings.create(ZMQ.SUB)
				.bindToPort(9000)
				.setMessageOffsets(0, 0)
				.setHWM(100);
		_eventListener = new EventListener(socketSetting, _disruptor, _zmqContext, new FatalExceptionCallback() {
			
			@Override
			public void signalFatalException(Throwable exception) {
				throw new RuntimeException(exception);
			}
		});
	}
	
	@Test(timeout=5000)
	public void singleSocket_oneEvent() throws Exception {
		final byte[] expectedContent = new byte[16];
		for (int i = 0; i < expectedContent.length; i += 4) {
			MessageBytesUtil.writeInt(expectedContent, i, 9);
		}
		byte[] subId = Util.getSubscriptionBytes(EventType.STATE_INPUT);
		
		final BlockingCall blockingCall = new BlockingCall();
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_SIZE])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(subId))
			.then(fakeRecv(expectedContent))
			.then(fakeBlockingRecv(blockingCall));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		_executor.execute(_eventListener);
		blockingCall.waitForBlockingCall(1, TimeUnit.SECONDS);
		
		assertTrue(_disruptor.getCursor() >= 0);
		// ensure that the event has the error flag unset
		byte[] event = _disruptor.get(0);
		assertFalse(MessageBytesUtil.readFlagFromByte(event, 0, 0));
		
		// ensure the content of the event matches
		byte[] actualContent = new byte[16];
		System.arraycopy(event, 1, actualContent, 0, 16);
		assertArrayEquals(String.format("Expected: %s, actual: %s", 
				toHexString(expectedContent), 
				toHexString(actualContent)), expectedContent, actualContent);
		
		// assert that any remaining events have the ignore flag set
		for (int i = 1; i < _disruptor.getCursor(); i++) {
			byte[] array = _disruptor.get(i);
			assertTrue(MessageBytesUtil.readFlagFromByte(array, 0, 0));
		}
	}
	
	@Test(timeout=5000)
	public void singleSocket_multipleEvents() throws Exception {
		final int eventCount = 5;
		final byte[][] expectedContent = new byte[eventCount][16];
		byte[][] subIds = new byte[eventCount][];
		for (int i = 0; i < eventCount; i++) {
			subIds[i] = Util.getSubscriptionBytes(EventType.STATE_INPUT);	
			for (int j = 0; j < expectedContent.length; j += 4) {
				MessageBytesUtil.writeInt(expectedContent[i], j, i);
			}	
		}
		
		final BlockingCall blockingCall = new BlockingCall();
		
		OngoingStubbing<Integer> whenCond = when(_zmqSocket.recv(
				argThat(matchesLength(new byte[EVENT_SIZE])), anyInt(), anyInt(), anyInt()));
		for (int i = 0; i < eventCount; i++) {
			whenCond = whenCond.then(fakeRecv(subIds[i]));
			whenCond = whenCond.then(fakeRecv(expectedContent[i]));
		}
		whenCond = whenCond.then(fakeBlockingRecv(blockingCall));
		OngoingStubbing<Boolean> hasReceiveMoreCond = when(_zmqSocket.hasReceiveMore());
		for (int i = 0; i < eventCount; i++) {
			hasReceiveMoreCond = hasReceiveMoreCond.thenReturn(true);
		}
		hasReceiveMoreCond = hasReceiveMoreCond.thenReturn(false);

		_executor.execute(_eventListener);
		blockingCall.waitForBlockingCall(1, TimeUnit.SECONDS);
		
		assertTrue(_disruptor.getCursor() >= eventCount - 1);
		// ensure that the event has the error flag unset
		for (int i = 0; i < eventCount; i++) {
			byte[] event = _disruptor.get(i);
			assertFalse(MessageBytesUtil.readFlagFromByte(event, 0, 0));
			
			// ensure the content of the event matches
			byte[] actualContent = new byte[16];
			System.arraycopy(event, 1, actualContent, 0, 16);
			assertArrayEquals(String.format("Expected: %s, actual: %s", 
					toHexString(expectedContent[i]), 
					toHexString(actualContent)), expectedContent[i], actualContent);
		}
		// assert that any remaining events have the ignore flag set
		for (int i = eventCount; i < _disruptor.getCursor(); i++) {
			byte[] array = _disruptor.get(i);
			assertTrue(MessageBytesUtil.readFlagFromByte(array, 0, 0));
		}
	}
	
	public void GeneralRuntimeException() throws Exception {
		
	}
	
	private String toHexString(byte[] array) {
	   return toHexString(array, 0, array.length);
	}
	
	private String toHexString(byte[] array, int offset, int length) {
		   StringBuilder sb = new StringBuilder();
		   for (int i = offset; i < offset + length; i++) {
			   sb.append(String.format("%02x", array[i] & 0xff));
		   }
		   return sb.toString();
		}
	
	private static class ArrayLengthMatcher<T> extends ArgumentMatcher<T> {

		private final int _expectedLength;
		
		public ArrayLengthMatcher(final int expectedLength) {
			_expectedLength = expectedLength;
		}
		
		@Override
		public boolean matches(Object argument) {
			int length = java.lang.reflect.Array.getLength(argument);
			return length == _expectedLength;
		}
	}
	
	private static <T> ArrayLengthMatcher<T> matchesLength(final T array) {
		int length = java.lang.reflect.Array.getLength(array);
		return new ArrayLengthMatcher<>(length);
	}
	
}
