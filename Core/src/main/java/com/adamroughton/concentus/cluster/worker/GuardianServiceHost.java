package com.adamroughton.concentus.cluster.worker;

import java.io.BufferedInputStream;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.data.cluster.kryo.GuardianInit;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

/**
 * Executed by the Guardian process - not intended to be executed otherwise
 * @author Adam Roughton
 *
 */
public class GuardianServiceHost {
	
	public static void main(String[] args) {
		try {
			// read service in from StdIn
			Kryo kryo = Util.newKryoInstance();
			
			System.out.println("Reading in initMsg");
			Input input = new Input(new BufferedInputStream(System.in));
			GuardianInit initMsg = (GuardianInit) kryo.readClassAndObject(input);
			if (initMsg == null) {
				System.exit(1); 
			}
			System.out.println("Starting service...");
		
			ConcentusExecutableOperations.executeClusterService(initMsg.cmdLineArgs(), 
					initMsg.deployment(), 
					initMsg.componentResolver());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}