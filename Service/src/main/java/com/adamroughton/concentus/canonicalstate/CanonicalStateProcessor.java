package com.adamroughton.concentus.canonicalstate;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Objects;

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
	
	private final CollectiveApplication _application;
	private final SendQueue<OutgoingEventHeader, TBuffer> _canoncialStatePubQueue;
	
	private final CanonicalStateUpdate _canonicalStateUpdate = new CanonicalStateUpdate();
	
	private final CountMetric _tickThroughputMetric;
	
	private long _nextUpdateId;
	
	public CanonicalStateProcessor(
			CollectiveApplication application,
			SendQueue<OutgoingEventHeader, TBuffer> canoncialStatePubQueue,
			MetricGroup metricGroup,
			MetricContext metricContext) {
		_application = Objects.requireNonNull(application);
		_canoncialStatePubQueue = Objects.requireNonNull(canoncialStatePubQueue);
		_nextUpdateId = 0;
		
		_tickThroughputMetric = metricGroup.add(metricContext.newThroughputMetric("canonicalState", "tickThroughput", false));
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
		_tickThroughputMetric.push(1);
	}
	
}
