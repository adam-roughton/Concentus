package com.adamroughton.concentus.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;

import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.KryoRegistratorDelegate;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class TestUtilKryoOps {
	
	private static class TestType implements TestTypeInterface {
		long value;

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof TestType)) {
				return false;
			}
			TestType other = (TestType) obj;
			if (value != other.value) {
				return false;
			}
			return true;
		}
		
	}
	
	public static interface TestTypeInterface {
		boolean equals(Object obj);
	}
	
	public static class TestTypeSubClass extends TestType {
	}
	
	private Kryo _kryo;
	
	@Before
	public void setUp() throws Exception {
		KryoRegistratorDelegate registrator = new KryoRegistratorDelegate() {
			
			@Override
			public void register(Kryo kryo) {
				int nextId = DataType.nextFreeId();
				kryo.register(TestType.class, nextId++);
				kryo.register(TestTypeSubClass.class, nextId++);
				
			}
		};
		_kryo = Util.newKryoInstance();
		registrator.register(_kryo);
	}
	
	@Test
	public void readKryoEncodedNull() throws Throwable {
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		Output output = new Output(bOut);
		_kryo.writeClassAndObject(output, null);
		output.close();
		
		assertNull(Util.fromKryoBytes(_kryo, bOut.toByteArray(), Object.class));
	}
	
	@Test
	public void readActualNull() throws Throwable {
		assertNull(Util.fromKryoBytes(_kryo, (byte[]) null, Object.class));
	}
	
	@Test
	public void readAndWritePrimitive() throws Throwable {
		assertReadAndWrite(5, Integer.class);
	}
	
	@Test
	public void readAndWriteRegisteredObjectType() throws Throwable {
		TestType testData = new TestType();
		testData.value = 9;
		assertReadAndWrite(testData, TestType.class);
	}
	
	@Test
	public void readAndWriteRegisteredObjectSuperClassType() throws Throwable {
		TestType testData = new TestTypeSubClass();
		testData.value = 9;
		assertReadAndWrite(testData, TestType.class);
	}
	
	@Test
	public void readAndWriteRegisteredObjectInterfaceType() throws Throwable {
		TestType testData = new TestType();
		testData.value = 9;
		assertReadAndWrite(testData, TestTypeInterface.class);
	}
	
	@Test
	public void readAndWriteUnregisteredObjectType() throws Throwable {
		assertReadAndWrite("Hello world!", String.class);
	}
	
	private <TIn, TOut> void assertReadAndWrite(TIn in, Class<TOut> expected) {
		byte[] data = Util.toKryoBytes(_kryo, in);
		assertEquals(in, Util.fromKryoBytes(_kryo, data, expected));
	}
	
}
