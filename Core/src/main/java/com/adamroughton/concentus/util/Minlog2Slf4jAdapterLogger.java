package com.adamroughton.concentus.util;

import org.slf4j.LoggerFactory;
import com.esotericsoftware.minlog.Log;

import static com.esotericsoftware.minlog.Log.*;

public class Minlog2Slf4jAdapterLogger extends Log.Logger {

	private final org.slf4j.Logger _slf4jLogger;
	
	public Minlog2Slf4jAdapterLogger(String name) {
		_slf4jLogger = LoggerFactory.getLogger(name);
	}
			
	@Override
	public void log(int level, String category, String message, Throwable ex) {
		switch (level) {
			case LEVEL_TRACE:
				_slf4jLogger.trace(message, ex);
				break;
			case LEVEL_DEBUG:
				_slf4jLogger.debug(message, ex);
				break;
			case LEVEL_INFO:
				_slf4jLogger.info(message, ex);
				break;
			case LEVEL_WARN:
				_slf4jLogger.warn(message, ex);
				break;
			case LEVEL_ERROR:
				_slf4jLogger.error(message, ex);
				break;
		}
	}

	
	
}
