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

public final class StubMessenger implements Messenger {

	public static interface FakeRecvDelegate {
		
		/**
		 * Gives the delegate the option to fake the reception of a message into the event buffer.
		 * If this call returns {@code true} then an event is acted upon. Otherwise the messenger
		 * acts as if no message was received.
		 * @param endPointIds
		 * @param recvSeq
		 * @param eventBuffer
		 * @param header
		 * @param isBlocking
		 * @return whether the caller should act as if a message was placed in the buffer
		 */
		boolean fakeRecv(int[] endPointIds, long recvSeq, byte[] eventBuffer, IncomingEventHeader header, boolean isBlocking);
	}
	
	public static interface FakeSendDelegate {
		
		/**
		 * Gives the delegate the option to fake the sending of a message. If the callee returns
		 * {@code false}, the caller acts as if the messenger was not ready.
		 * @param sendSeq
		 * @param eventBuffer
		 * @param header
		 * @param isBlocking
		 * @return whether the caller should act as if the message was sent
		 */
		boolean fakeSend(long sendSeq, byte[] eventBuffer, OutgoingEventHeader header, boolean isBlocking);
	}
	
	private final int[] _endPointIds;
	private long _recvSeq = 0;
	private long _sendSeq = 0;
	private FakeRecvDelegate _fakeRecvDelegate;
	private FakeSendDelegate _fakeSendDelegate;
	
	public StubMessenger(int[] endPointIds) {
		_endPointIds = endPointIds;
		setFakeRecvDelegate(null);
		setFakeSendDelegate(null);
	}

	public void setFakeRecvDelegate(FakeRecvDelegate fakeRecvDelegate) {
		if (fakeRecvDelegate == null) {
			fakeRecvDelegate = new FakeRecvDelegate() {
				
				@Override
				public boolean fakeRecv(int[] endPointIds, long recvSeq, byte[] eventBuffer,
						IncomingEventHeader header, boolean isBlocking) {
					return true;
				}
			};
		}
		_fakeRecvDelegate = fakeRecvDelegate;
	}
	
	public void setFakeSendDelegate(FakeSendDelegate fakeSendDelegate) {
		if (fakeSendDelegate == null) {
			fakeSendDelegate = new FakeSendDelegate() {
				
				@Override
				public boolean fakeSend(long sendSeq, byte[] eventBuffer,
						OutgoingEventHeader header, boolean isBlocking) {
					return true;
				}
			};
		}
		_fakeSendDelegate = fakeSendDelegate;
	}
	
	@Override
	public boolean send(byte[] outgoingBuffer, OutgoingEventHeader header,
			boolean isBlocking) throws MessengerClosedException {
		return _fakeSendDelegate.fakeSend(_sendSeq++, outgoingBuffer, header, isBlocking);
	}

	@Override
	public boolean recv(byte[] eventBuffer, IncomingEventHeader header,
			boolean isBlocking) throws MessengerClosedException {
		return _fakeRecvDelegate.fakeRecv(_endPointIds, _recvSeq++, eventBuffer, header, isBlocking);
	}

	@Override
	public int[] getEndpointIds() {
		return _endPointIds;
	}

	@Override
	public boolean hasPendingEvents() {
		return true;
	}

}
