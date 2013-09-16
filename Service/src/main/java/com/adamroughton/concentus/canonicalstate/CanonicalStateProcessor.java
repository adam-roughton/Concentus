package com.adamroughton.concentus.canonicalstate;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.model.CollectiveApplication;

public class CanonicalStateProcessor<TBuffer extends ResizingBuffer> {
	
	private final AtomicBoolean _isRunning = new AtomicBoolean(false);
	
	private final CollectiveApplication _application;
	private final Clock _clock;
	private final SendQueue<OutgoingEventHeader, TBuffer> _canoncialStatePubQueue;
	private final TickTimer _tickTimer;
	
	private final CanonicalStateUpdate _canonicalStateUpdate = new CanonicalStateUpdate();
	
	private final CountMetric _tickThroughputMetric;
	
	private Thread _tickThread;
	private long _nextUpdateId;
	
	public CanonicalStateProcessor(
			CollectiveApplication application,
			Clock clock,
			SendQueue<OutgoingEventHeader, TBuffer> canoncialStatePubQueue,
			TickTimer.TickStrategy tickStrategy,
			MetricGroup metricGroup,
			MetricContext metricContext) {
		_application = Objects.requireNonNull(application);
		_clock = Objects.requireNonNull(clock);
		_canoncialStatePubQueue = Objects.requireNonNull(canoncialStatePubQueue);
		_tickTimer = new TickTimer(_clock, tickStrategy, _application.getTickDuration(), 0);
		_nextUpdateId = 0;
		
		_tickThroughputMetric = metricGroup.add(metricContext.newThroughputMetric("canonicalState", "tickThroughput", false));
	}
	
	public void start() {
		if (!_isRunning.compareAndSet(false, true))
			throw new RuntimeException("The canonical state processor can only be started once.");
		_tickThread = new Thread(_tickTimer);
		_tickThread.start();
		_tickTimer.allowNextTick();
	}
	
	public void stop() {
		try {
			if (_tickThread != null) {
				_tickTimer.stop();
				try {
					_tickThread.join();
				} catch (InterruptedException e) {
					throw new RuntimeException("Interrupted while waiting for the tick timer to stop.", e);
				}
				_tickThread = null;
			}
		} finally {
			_isRunning.set(false);
		}
	}
	
	public void onTickCompleted(final long time, final Int2ObjectMap<CollectiveVariable> variableSet) {
		_canoncialStatePubQueue.send(PubSubPattern.asTask(_canonicalStateUpdate, new EventWriter<OutgoingEventHeader, CanonicalStateUpdate>() {

			@Override
			public void write(OutgoingEventHeader header,
					CanonicalStateUpdate update) throws Exception {
				update.setUpdateId(_nextUpdateId++);
				update.setTime(time);
				_application.createUpdate(update.getData(), time, variableSet);
			}
		}));		
		_tickTimer.allowNextTick();
		_tickThroughputMetric.push(1);
	}
	
}
