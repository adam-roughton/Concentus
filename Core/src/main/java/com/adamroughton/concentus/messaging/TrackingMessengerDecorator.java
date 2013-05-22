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
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.Clock;
import com.esotericsoftware.minlog.Log;

public final class TrackingMessengerDecorator implements Messenger {

	private final Messenger _messenger;
	private final Clock _clock;
	
	private long _sendInvocations = 0;
	private long _recvInvocations = 0;
	private long _sentCount = 0;
	private long _failedSendCount = 0;
	private long _recvCount = 0;
	private long _lastReportTime = 0;
	private long _nanosBlockingForSend = 0;
	private long _nanosBlockingForRecv = 0;
	
	public TrackingMessengerDecorator(Messenger messenger, Clock clock) {
		_messenger = Objects.requireNonNull(messenger);
		_clock = Objects.requireNonNull(clock);
	}
	
	private boolean onSend(boolean wasSent) {
		_sendInvocations++;
		if (wasSent) {
			_sentCount++;
		} else {
			_failedSendCount++;
		}
		logTick();
		return wasSent;
	}
	
	private boolean onBlockingSend(long startTime, boolean wasSent) {
		long blockingDuration = _clock.nanoTime() - startTime;
		_nanosBlockingForSend += blockingDuration;
		return onSend(wasSent);
	}
	
	private boolean onRecv(boolean didRecv) {
		_recvInvocations++;
		if (didRecv) {
			_recvCount++;
			logTick();
		}
		return didRecv;
	}
	
	private boolean onBlockingRecv(long startTime, boolean didRecv) {
		long blockingDuration = _clock.nanoTime() - startTime;
		_nanosBlockingForRecv += blockingDuration;
		return onRecv(didRecv);
	}
	
	private void logTick() {
		long now = _clock.currentMillis();
		long elapsed = now - _lastReportTime;
		if (elapsed > 1000) {
			double sentThroughput = getThroughput(_sentCount, elapsed);
			double failedSendThroughput = getThroughput(_failedSendCount, elapsed);
			double recvThroughput = getThroughput(_recvCount, elapsed);
			long millisBlockingForSend = TimeUnit.NANOSECONDS.toMillis(_nanosBlockingForSend);
			long millisBlockingForRecv = TimeUnit.NANOSECONDS.toMillis(_nanosBlockingForRecv);
			Log.info(String.format("Messaging (%s): %d sendCalls, %f sent/s, %f failedSend/s, %d millisBlockingInSend, %d recvCalls, %f recv/s, %d millisBlockingForRecv", 
					_messenger.toString(), _sendInvocations, sentThroughput, failedSendThroughput, millisBlockingForSend, _recvInvocations, recvThroughput, millisBlockingForRecv));
			_lastReportTime = now;
			_sendInvocations = 0;
			_recvInvocations = 0;
			_sentCount = 0;
			_recvCount = 0;
			_failedSendCount = 0;
			_nanosBlockingForSend = 0;
			_nanosBlockingForRecv = 0;
		}
	}
	
	private double getThroughput(long counter, long elapsedMillis) {
		return ((double) counter) / elapsedMillis * 1000;
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
	public boolean hasPendingEvents() {
		return _messenger.hasPendingEvents();
	}

}
