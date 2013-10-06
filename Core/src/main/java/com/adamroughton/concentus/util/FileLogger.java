package com.adamroughton.concentus.util;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.esotericsoftware.minlog.Log;
import com.esotericsoftware.minlog.Log.Logger;

public class FileLogger extends Log.Logger implements Closeable {

	private BufferedWriter _logFileWriter;
	private boolean _hasWrittenFailMessage = false;
	
	public FileLogger(Path logFilePath) throws IOException {
		_logFileWriter = Files.newBufferedWriter(Util.createUniqueFile(logFilePath), 
				StandardCharsets.ISO_8859_1, StandardOpenOption.APPEND);
	}

	@Override
	protected void print(String message) {
		try {
			_logFileWriter.write(message);
			_logFileWriter.newLine();
			_logFileWriter.flush();
		} catch (IOException eIO) {
			if (!_hasWrittenFailMessage) {
				super.print("Failed to write to log file! " +
						"Switching to stdout! - " + Util.stackTraceToString(eIO));
				Log.setLogger(new Logger());
				_hasWrittenFailMessage = true;
			}
			super.print(message);
		}
	}

	@Override
	public void close() throws IOException {
		_logFileWriter.close();
	}
	
}