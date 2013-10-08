package com.adamroughton.concentus.actioncollector;

public interface TickDriven {

	/**
	 * Requests that the entity process the tick for the
	 * given time.
	 * @param time
	 * @return {@code true} if the process is ready to process
	 * ticks, {@code false} otherwise (i.e. not initialised)
	 */
	boolean tick(long time);
	
}
