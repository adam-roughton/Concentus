package com.adamroughton.concentus.canonicalstate;

import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.TickEvent;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.lmax.disruptor.LifecycleAware;

public class TickTimerProcessor<TBuffer extends ResizingBuffer> implements DeadlineBasedEventHandler<TBuffer>, LifecycleAware {
	
	private final long _tickDuration;
	private final long _simStartTime;
	private final IncomingEventHeader _updateRecvHeader;
	private final SendQueue<OutgoingEventHeader, TBuffer> _tickSendQueue;
	private final Clock _clock;
	
	private final TickEvent _tickEvent = new TickEvent();
	private final CanonicalStateUpdate _updateEvent = new CanonicalStateUpdate();
	
	private long _startWallTime;
	private long _lastTickTime = -1;
	private long _nextTickSimTime;
	private long _nextTickWallTime;
	
	private boolean _shouldSendTick;
	
	public TickTimerProcessor(long tickDuration, long simStartTime, Clock clock, 
			IncomingEventHeader updateRecvHeader, 
			SendQueue<OutgoingEventHeader, TBuffer> tickSendQueue) {
		_tickDuration = tickDuration;
		_simStartTime = simStartTime;
		_updateRecvHeader = Objects.requireNonNull(updateRecvHeader);
		_tickSendQueue = Objects.requireNonNull(tickSendQueue);
		_clock = Objects.requireNonNull(clock);
	}
	
	@Override
	public void onStart() {
		_startWallTime = _clock.currentMillis();
		_nextTickSimTime = _simStartTime;
		_shouldSendTick = true;
	}

	@Override
	public void onShutdown() {
	}
	
	@Override
	public void onEvent(TBuffer event, long sequence, boolean isEndOfBatch)
			throws Exception {
		if (!EventHeader.isValid(event, 0))
			return;
		
		EventPattern.readContent(event, _updateRecvHeader, _updateEvent, new EventReader<IncomingEventHeader, CanonicalStateUpdate>() {

			@Override
			public void read(IncomingEventHeader header,
					CanonicalStateUpdate event) {				
				if (event.getTime() >= _lastTickTime) {
					_shouldSendTick = true;
				}
			}
			
		});
	}

	@Override
	public void onDeadline() {
		if (_shouldSendTick) {
			final long tickTime = _nextTickSimTime;
			
			_tickSendQueue.send(PubSubPattern.asTask(_tickEvent, new EventWriter<OutgoingEventHeader, TickEvent>() {

				@Override
				public void write(OutgoingEventHeader header,
						TickEvent event) throws Exception {
					event.setTime(tickTime);
				}
				
			}));
			
			_lastTickTime = tickTime;			
			long simTime = (_clock.currentMillis() - _startWallTime) + _simStartTime;
			
			// set the next tick to the next available tick time (skip over any missed ticks)
			_nextTickSimTime = ((simTime / _tickDuration) + 1) * _tickDuration;
			
			_shouldSendTick = false;
		}
	}

	@Override
	public long moveToNextDeadline(long pendingCount) {
		_nextTickWallTime = (_nextTickSimTime - _simStartTime) + _startWallTime;
		return _nextTickWallTime;
	}

	@Override
	public long getDeadline() {
		return _nextTickWallTime;
	}

	@Override
	public String name() {
		return "TickTimerProcessor";
	}

}
