package com.adamroughton.consentus.canonicalstate;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;
import org.zeromq.ZMQ;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.ZmqTestUtil.BlockingCall;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import static org.mockito.Mockito.*;
import static org.mockito.AdditionalMatchers.aryEq;
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
		Config conf = new Config();
		_disruptor = new RingBuffer<>(new EventFactory<byte[]>() {
			@Override
			public byte[] newInstance() {
				return new byte[256];
			}
		}, BUFFER_SIZE);
		_disruptor.setGatingSequences(new Sequence(BUFFER_SIZE - 1));
		when(_zmqContext.socket(ZMQ.SUB)).thenReturn(_zmqSocket);
		_eventListener = new EventListener(_zmqContext, _disruptor, conf, new FatalExceptionCallback() {
			
			@Override
			public void signalFatalException(Throwable exception) {
				//TODO
			}
		});
	}
	
	@Test(timeout=5000)
	public void standardEventFlow() throws Exception {
		int eventCount = 100;
		fakeEvents(eventCount, new FakeEventGenerator() {
			public void generate(long seq, ByteBuffer event) {
				event.putInt((int)seq);
			}
		});

		assertTrue(_disruptor.getCursor() >= eventCount - 1);
		// ensure that all events have the ignore flag unset
		for (int i = 0; i < eventCount; i++) {
			byte[] array = _disruptor.get(i);
			assertFalse(MessageBytesUtil.readFlagFromByte(array, 0, 0));
			assertEquals(i, MessageBytesUtil.readInt(array, 1));
		}
		// assert that any remaining events have the ignore flag set
		for (int i = eventCount; i < _disruptor.getCursor(); i++) {
			byte[] array = _disruptor.get(i);
			assertTrue(MessageBytesUtil.readFlagFromByte(array, 0, 0));
		}
	}
	
	public void GeneralRuntimeException() throws Exception {
		
	}
	
	private interface FakeEventGenerator {
		void generate(long seq, ByteBuffer event);
	}
	
	private void fakeEvents(int eventCount, FakeEventGenerator eventGenerator) throws Exception {
		BlockingCall blockingCall = new BlockingCall();
		
		OngoingStubbing<Integer> whenCond = when(_zmqSocket.recv(aryEq(new byte[EVENT_SIZE]), eq(1), eq(255), eq(0)));
		for (int i = 0; i < eventCount; i++) {
			byte[] eventBytes = new byte[EVENT_SIZE];
			ByteBuffer buffer = ByteBuffer.wrap(eventBytes, 1, EVENT_SIZE - 1);
			eventGenerator.generate(i, buffer);
			whenCond = whenCond.then(fakeRecv(buffer));
		}
		whenCond.then(fakeBlockingRecv(blockingCall));
		
		Future<?> task = _executor.submit(_eventListener);
		blockingCall.waitForBlockingCall(1, TimeUnit.SECONDS);
		
		task.cancel(true);
		try {
			// wait for the task thread to complete
			task.get();
		} catch (CancellationException | InterruptedException | ExecutionException e) {	
			// we expect such an exception
		}
		verify(_zmqSocket).close();
	}
	
}
