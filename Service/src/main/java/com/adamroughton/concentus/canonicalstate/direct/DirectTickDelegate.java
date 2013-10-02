package com.adamroughton.concentus.canonicalstate.direct;

import java.util.Iterator;

import com.adamroughton.concentus.actioncollector.TickDelegate;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;

public class DirectTickDelegate<TBuffer extends ResizingBuffer> implements TickDelegate {

	private final EventQueuePublisher<ComputeStateEvent> _recvQueuePubliser;
	
	public DirectTickDelegate(EventQueue<ComputeStateEvent> recvQueue) {
		_recvQueuePubliser = recvQueue.createPublisher("recvQueuePublisher", true);
	}
	
	@Override
	public void onTick(long time, Iterator<CandidateValue> candidateValuesIterator) {
		ComputeStateEvent event = _recvQueuePubliser.next();
		try {
			event.time = time;
			event.candidateValues.clear();
			int count = 0;
			long startTime = System.nanoTime();
			while (candidateValuesIterator.hasNext()) {
				event.candidateValues.add(candidateValuesIterator.next());
				count++;
				if (count % 1000 == 0) { 
					com.esotericsoftware.minlog.Log.info("DirectTickDelegate.onTick: Up to " + count);
				}
			}
			long duration = System.nanoTime() - startTime;
			com.esotericsoftware.minlog.Log.info("DirectTickDelegate.onTick: took " + duration + "ns to push " + count + " elements onto the recvQueue entry");
		} finally {
			_recvQueuePubliser.publish();
		}	
	}


}
