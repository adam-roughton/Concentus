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
	private final StatsMetric _sendCallNanosStatsMetric;
	private final StatsMetric _recvCallNanosStatsMetric;
	
	private long _nextMetricTime;
	
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
		_sendCallNanosStatsMetric = _metrics.add(_metricContext.newStatsMetric(reference, "sendCallNanosStatsMetric", false));
		_recvCallNanosStatsMetric = _metrics.add(_metricContext.newStatsMetric(reference, "recvCallNanosStatsMetric", false));
	}
	
	private boolean onSend(long startTime, boolean wasSent) {
		long sendNanos = _clock.nanoTime() - startTime;
		_sendCallNanosStatsMetric.push(sendNanos);
		_sendInvocationThroughputMetric.push(1);
		if (wasSent) {
			_sentThroughputMetric.push(1);
		} else {
			_failedSendThroughputMetric.push(1);
		}
		emitMetricIfReady();
		return wasSent;
	}
	
	private boolean onRecv(long startTime, boolean didRecv) {
		long recvNanos = _clock.nanoTime() - startTime;
		_recvCallNanosStatsMetric.push(recvNanos);
		_recvInvocationThroughputMetric.push(1);
		if (didRecv) {
			_recvThroughputMetric.push(1);
			emitMetricIfReady();
		}
		return didRecv;
	}
	
	private void emitMetricIfReady() {
		if (_clock.currentMillis() >= _nextMetricTime) {
			_metrics.publishPending();
			_nextMetricTime = _metrics.nextBucketReadyTime();
		}
	}

	@Override
	public boolean send(byte[] outgoingBuffer, OutgoingEventHeader header,
			boolean isBlocking) throws MessengerClosedException {
		long startTime = _clock.nanoTime();
		return onSend(startTime, _messenger.send(outgoingBuffer, header, isBlocking));
	}

	@Override
	public boolean recv(byte[] eventBuffer, IncomingEventHeader header,
			boolean isBlocking) throws MessengerClosedException {
		long startTime = _clock.nanoTime();
		return onRecv(startTime, _messenger.recv(eventBuffer, header, isBlocking));
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
