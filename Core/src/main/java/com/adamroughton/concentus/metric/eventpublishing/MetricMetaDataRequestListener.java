package com.adamroughton.concentus.metric.eventpublishing;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;

import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.MetricMetaDataEvent;
import com.adamroughton.concentus.messaging.events.MetricMetaDataRequestEvent;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.RouterPattern;
import com.adamroughton.concentus.metric.MetricMetaData;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;

class MetricMetaDataRequestListener implements Runnable, OwnerDelegate<Messenger> {

	private final Mutex<Messenger> _messengerMutex;
	private final IncomingEventHeader _incomingEventHeader;
	private final OutgoingEventHeader _outgoingEventHeader;
	private final Long2ObjectMap<MetricMetaData> _metaDataLookup = new Long2ObjectRBTreeMap<>();
	private final UUID _sourceId;
	private final FatalExceptionCallback _exceptionCallback;
	
	private enum State {
		STOPPED,
		HALTING,
		RUNNING
	}
	private final AtomicReference<State> _state = new AtomicReference<>(State.STOPPED);
	
	private final MetricMetaDataRequestEvent _metaDataReqEvent = new MetricMetaDataRequestEvent();
	private final MetricMetaDataEvent _metaDataEvent = new MetricMetaDataEvent();
	
	private final byte[] _recvBuffer = new byte[Constants.MSG_BUFFER_ENTRY_LENGTH];
	private final byte[] _sendBuffer = new byte[Constants.MSG_BUFFER_ENTRY_LENGTH];
	
	public MetricMetaDataRequestListener(Mutex<Messenger> messengerMutex, 
			IncomingEventHeader recvHeader, 
			OutgoingEventHeader sendHeader, 
			UUID sourceId, 
			FatalExceptionCallback exceptionCallback) {
		_messengerMutex = Objects.requireNonNull(messengerMutex);
		_incomingEventHeader = Objects.requireNonNull(recvHeader);
		_outgoingEventHeader = Objects.requireNonNull(sendHeader);
		_sourceId = sourceId;
		_exceptionCallback = Objects.requireNonNull(exceptionCallback);
	}
	
	public void putMetaData(MetricMetaData metaData) {
		synchronized(_metaDataLookup) {
			_metaDataLookup.put(metaData.getMetricId(), metaData);
		}
	}
	
	@Override
	public void run() {
		if (!_state.compareAndSet(State.STOPPED, State.RUNNING))
			throw new IllegalStateException(String.format("%s can only be started once.", getClass().getName()));
		_messengerMutex.runAsOwner(this);
	}
	
	@Override
	public void asOwner(final Messenger messenger) {
		try {
			while (_state.get() == State.RUNNING) {
				if (messenger.recv(_recvBuffer, _incomingEventHeader, true)) {
					final byte[] clientId = RouterPattern.getSocketId(_recvBuffer, _incomingEventHeader);
					EventPattern.readContent(_recvBuffer, _incomingEventHeader, _metaDataReqEvent, 
							new EventReader<IncomingEventHeader, MetricMetaDataRequestEvent>() {
	
						@Override
						public void read(IncomingEventHeader header,
								MetricMetaDataRequestEvent event) {
							if (event.getSourceId() == _sourceId) {
								sendMetaData(clientId, event.getMetricId(), messenger);
							}
						}
					});
				}
			}
		} catch (Exception e) {
			_exceptionCallback.signalFatalException(e);
		}
		_state.set(State.STOPPED);
	}
	
	private void sendMetaData(byte[] destId, int metricId, Messenger messenger) {
		final MetricMetaData requestedMetaData;
		synchronized(_metaDataLookup) {
			requestedMetaData = _metaDataLookup.get(metricId);
		}
		RouterPattern.writeEvent(_sendBuffer, _outgoingEventHeader, destId, _metaDataEvent, 
				new EventWriter<OutgoingEventHeader, MetricMetaDataEvent>() {

			@Override
			public void write(OutgoingEventHeader header,
					MetricMetaDataEvent event)
					throws Exception {
				event.setMetricId(requestedMetaData.getMetricId());
				event.setReference(requestedMetaData.getReference());
				event.setMetricName(requestedMetaData.getMetricName());
			}
			
		});
		messenger.send(_sendBuffer, _outgoingEventHeader, true);
	}
	
	public void halt() {
		_state.compareAndSet(State.RUNNING, State.HALTING);
	}

}
