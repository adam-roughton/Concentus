/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.concentus.messaging;

import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.metric.StatsMetric;

public final class TrackingMessengerDecorator implements Messenger {
	
	private final Messenger _messenger;
	private final Clock _clock;
	
	private final MetricContext _metricContext;
	private final MetricGroup _metrics;
	private final CountMetric _sendInvocationThroughputMetric;
	private final CountMetric _recvInvocationThroughputMetric;
	private final CountMetric _sentThroughputMetric;
	private final CountMetric _failedSendThroughputMetric;
	private final CountMetric _recvThroughputMetric;
	private final StatsMetric _nanosBlockingForSendStatsMetric;
	private final StatsMetric _nanosBlockingForRecvStatsMetric;
	
	public TrackingMessengerDecorator(MetricContext metricContext, Messenger messenger, Clock clock) {
		_messenger = Objects.requireNonNull(messenger);
		_clock = Objects.requireNonNull(clock);

		_metricContext = Objects.requireNonNull(metricContext);
		_metrics = new MetricGroup();
		String reference = String.format("Messenger(%s)", messenger.name());
		_sendInvocationThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "sendInvocationThroughput", false));
		_recvInvocationThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "recvInvocationThroughput", false));
		_sentThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "sentThroughput", false));
		_failedSendThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "failedSendThroughput", false));
		_recvThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "recvThroughput", false));
		_nanosBlockingForSendStatsMetric = _metrics.add(_metricContext.newStatsMetric(reference, "nanosBlockingForSendStats", false));
		_nanosBlockingForRecvStatsMetric = _metrics.add(_metricContext.newStatsMetric(reference, "nanosBlockingForRecvStats", false));
	}
	
	private boolean onSend(boolean wasSent) {
		_sendInvocationThroughputMetric.push(1);
		if (wasSent) {
			_sentThroughputMetric.push(1);
		} else {
			_failedSendThroughputMetric.push(1);
		}
		emitMetricIfReady();
		return wasSent;
	}
	
	private boolean onBlockingSend(long startTime, boolean wasSent) {
		long blockingDuration = _clock.nanoTime() - startTime;
		_nanosBlockingForSendStatsMetric.push(blockingDuration);
		return onSend(wasSent);
	}
	
	private boolean onRecv(boolean didRecv) {
		_recvInvocationThroughputMetric.push(1);
		if (didRecv) {
			_recvThroughputMetric.push(1);
			emitMetricIfReady();
		}
		return didRecv;
	}
	
	private boolean onBlockingRecv(long startTime, boolean didRecv) {
		long blockingDuration = _clock.nanoTime() - startTime;
		_nanosBlockingForRecvStatsMetric.push(blockingDuration);
		return onRecv(didRecv);
	}
	
	private void emitMetricIfReady() {
		if (_clock.currentMillis() >= _metrics.nextBucketReadyTime()) {
			_metrics.publishPending();
		}
	}

	@Override
	public boolean send(byte[] outgoingBuffer, OutgoingEventHeader header,
			boolean isBlocking) throws MessengerClosedException {
		if (isBlocking) {
			long startTime = _clock.nanoTime();
			return onBlockingSend(startTime, _messenger.send(outgoingBuffer, header, isBlocking));
		} else {
			return onSend(_messenger.send(outgoingBuffer, header, isBlocking));
		}
	}

	@Override
	public boolean recv(byte[] eventBuffer, IncomingEventHeader header,
			boolean isBlocking) throws MessengerClosedException {
		if (isBlocking) {
			long startTime = _clock.nanoTime();
			return onBlockingRecv(startTime, _messenger.recv(eventBuffer, header, isBlocking));
		} else {
			return onRecv(_messenger.recv(eventBuffer, header, isBlocking));
		}
	}

	@Override
	public int[] getEndpointIds() {
		return _messenger.getEndpointIds();
	}

	@Override
	public String name() {
		return _messenger.name();
	}

}
