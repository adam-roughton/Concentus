package com.adamroughton.concentus.cluster.worker;

import java.io.BufferedInputStream;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.data.cluster.kryo.GuardianInit;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.minlog.Log;
import com.esotericsoftware.minlog.Log.Logger;

/**
 * Executed by the Guardian process - not intended to be executed otherwise
 * @author Adam Roughton
 *
 */
public class GuardianServiceHost {
	
	public static void main(String[] args) {
		try {
			Log.setLogger(new StdErrLogger());
			
			// read service in from StdIn
			Kryo kryo = Util.newKryoInstance();
			
			System.out.println("Reading in initMsg");
			Input input = new Input(new BufferedInputStream(System.in));
			GuardianInit initMsg = (GuardianInit) kryo.readClassAndObject(input);
			if (initMsg == null) {
				throw new RuntimeException("The initMsg was null"); 
			}
			System.out.println("Starting service...");
		
			ConcentusExecutableOperations.executeClusterService(initMsg.cmdLineArgs(), 
					initMsg.deployment(), 
					initMsg.componentResolver());
		} catch (Exception e) {
			Log.error("GuardianServiceHost.main", e);
			System.exit(1);
		}
	}
	
	private final static class StdErrLogger extends Logger {
		
		@Override
		public void log(int level, String category, String message, Throwable ex) {
			super.log(level, category, message, ex);
			if (level == Log.LEVEL_ERROR) {
				captureError(message, ex);
			}
		}
		
		private void captureError(String message, Throwable ex) {
			System.err.println(message + ex != null? ": " + Util.stackTraceToString(ex) : "");
			System.err.flush();
		}
		
	}
	
}