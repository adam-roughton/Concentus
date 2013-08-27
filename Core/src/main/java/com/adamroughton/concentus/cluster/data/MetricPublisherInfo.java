package com.adamroughton.concentus.cluster.data;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

import com.adamroughton.concentus.data.BytesUtil;

public final class MetricPublisherInfo {

	private final UUID _sourceId;
	private final String _sourceType;
	private final String _pubAddress;
	private final String _metaDataReqAddress;
	
	public MetricPublisherInfo(UUID sourceId, String sourceType, String pubAddress, String metaDataReqAddress) {
		_sourceId = sourceId;
		_sourceType = Objects.requireNonNull(sourceType);
		_pubAddress = Objects.requireNonNull(pubAddress);
		_metaDataReqAddress = Objects.requireNonNull(metaDataReqAddress);
	}

	public UUID getSourceId() {
		return _sourceId;
	}
	
	public String getSourceType() {
		return _sourceType;
	}
	
	public String getPubAddress() {
		return _pubAddress;
	}

	public String getMetaDataReqAddress() {
		return _metaDataReqAddress;
	}
	
	public byte[] getBytes() {
		return toBytes(this);
	}
	
	public static byte[] toBytes(MetricPublisherInfo pubInfo) {
		int reqLength = 16;
		
		// convert strings to bytes
		String[] strings = new String[] {pubInfo.getSourceType(), pubInfo.getPubAddress(), pubInfo.getMetaDataReqAddress()};
		byte[][] stringBytesArray = new byte[strings.length][];
		for (int i = 0; i < strings.length; i++) {
			stringBytesArray[i] = strings[i].getBytes(StandardCharsets.UTF_8);
			reqLength += stringBytesArray[i].length;
		}
		
		byte[] bytes = new byte[reqLength];
		int offset = 0;
		BytesUtil.writeUUID(bytes, offset, pubInfo.getSourceId());
		offset += 16;
		for (byte[] stringBytes : stringBytesArray) {
			BytesUtil.writeInt(bytes, 0, stringBytes.length);
			offset += 4;
			System.arraycopy(stringBytes, 0, bytes, offset, stringBytes.length);
			offset += stringBytes.length;
		}
		
		return bytes;
	}
	
	public static MetricPublisherInfo fromBytes(byte[] bytes) {
		int offset = 0;
		assertCanRead(bytes, offset, 16);
		UUID sourceId = BytesUtil.readUUID(bytes, offset);
		offset += 16;
		
		StringRes res;
		res = readString(offset, bytes);
		String sourceType = res.string;
		offset = res.newOffset;
		
		res = readString(offset, bytes);
		String pubAddress = res.string;
		offset = res.newOffset;
		
		res = readString(offset, bytes);
		String metaDataAddress = res.string;
		offset = res.newOffset;
		
		return new MetricPublisherInfo(sourceId, sourceType, pubAddress, metaDataAddress);
	}
	
	private static class StringRes {
		public int newOffset;
		public String string;
	}
	
	private static StringRes readString(int offset, byte[] bytes) {
		assertCanRead(bytes, offset, 4);
		int stringLength = BytesUtil.readInt(bytes, offset);
		offset += 4;
		assertCanRead(bytes, offset, stringLength);
		String string = new String(bytes, offset, stringLength, StandardCharsets.UTF_8);
		offset += stringLength;
		StringRes res = new StringRes();
		res.newOffset = offset;
		res.string = string;
		return res;
	}
	
	private static void assertCanRead(byte[] array, int offset, int length) {
		if (array.length < offset + length) 
			throw new IllegalArgumentException(String.format("The array was not long enough (tried to " +
					"read length %d at offset %d on array with length %d", length, offset, array.length));
	}
	
}
