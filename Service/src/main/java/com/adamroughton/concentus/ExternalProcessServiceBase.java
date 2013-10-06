package com.adamroughton.concentus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase;
import com.adamroughton.concentus.cluster.worker.ServiceContext;
import com.adamroughton.concentus.cluster.worker.StateData;
import com.adamroughton.concentus.data.cluster.kryo.ProcessReturnInfo;
import com.adamroughton.concentus.data.cluster.kryo.ProcessReturnInfo.ReturnType;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.util.ProcessTask;
import com.adamroughton.concentus.util.ProcessTask.ProcessDelegate;
import com.adamroughton.concentus.util.TimeoutTracker;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.minlog.Log;

public class ExternalProcessServiceBase extends ConcentusServiceBase {

	private final ExecutorService _processMonitorExecutor = Executors.newSingleThreadExecutor();
	private final ServiceContext<ServiceState> _serviceContext;
	private final ConcentusHandle _concentusHandle;
	private ProcessTask _currentProcessTask = null;
	
	public ExternalProcessServiceBase(ServiceContext<ServiceState> serviceContext, ConcentusHandle concentusHandle) {
		_serviceContext = Objects.requireNonNull(serviceContext);
		_concentusHandle = Objects.requireNonNull(concentusHandle);
	}
	
	protected void startProcess(String command, String...args) {
		startProcess(command, Arrays.asList(args));
	}

	protected void startProcess(String command, List<String> args) {
		if (_currentProcessTask != null) {
			throw new IllegalStateException("Only one process can be active on this external process service");
		}
		List<String> commands = new ArrayList<>(args.size() + 1);
		commands.add(command);
		commands.addAll(args);
		final ProcessBuilder processBuilder = new ProcessBuilder(commands);
		processBuilder.redirectOutput(Redirect.INHERIT);
		ProcessDelegate processTaskDelegate = new ProcessDelegate() {
			
			@Override
			public void runProcess(final Process process) throws InterruptedException {
				final StringBuilder stdErrBuilder = new StringBuilder();
				Thread stdErrCollector = new Thread() {
				
					public void run() {
						try (BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
							String readLine;
							do {
								readLine = stderrReader.readLine();
								Log.info("External process stderr: " + readLine);
								if (readLine != null) stdErrBuilder.append(readLine + '\n');
							} while (readLine != null);
						} catch (IOException eIO) {
							Log.error("Error reading stderr stream from external process", eIO);
						}
					}
				};
				stdErrCollector.start();				
				
				int retCode = process.waitFor();
				stdErrCollector.join();
				
				ProcessReturnInfo retInfo;
				if (retCode == 0) {
					retInfo = new ProcessReturnInfo(ReturnType.OK, null);
				} else {
					retInfo = new ProcessReturnInfo(ReturnType.ERROR, stdErrBuilder.toString());
				}
				Log.info("ExternalProcessServiceBase.startProcess: Signalling process death - retInfo=" + retInfo);
				_serviceContext.enterState(ServiceState.SHUTDOWN, retInfo);
			}

			@Override
			public void handleError(Exception e) {
				ProcessReturnInfo retInfo = new ProcessReturnInfo(ReturnType.ERROR, Util.stackTraceToString(e));
				_serviceContext.enterState(ServiceState.SHUTDOWN, retInfo);
			}
		};
		_currentProcessTask = new ProcessTask(processBuilder, processTaskDelegate);
		_processMonitorExecutor.execute(_currentProcessTask);
	}

	@Override
	protected void onShutdown(StateData stateData, ClusterHandle cluster)
			throws Exception {
		if (stateData.hasData()) {
			ProcessReturnInfo retInfo = stateData.getData(ProcessReturnInfo.class);
			if (retInfo.getReturnType() == ReturnType.ERROR) {
				_concentusHandle.signalFatalException(new RuntimeException(retInfo.getReason()));
			}
		}
		
		if (_currentProcessTask != null) {
			// wait for the host process to stop
			TimeoutTracker timeoutTracker = new TimeoutTracker(5000, TimeUnit.MILLISECONDS);
			try {
				_currentProcessTask.waitForStop(timeoutTracker.getTimeout(), timeoutTracker.getUnit());
			} catch (TimeoutException eTimeOut) {
				_currentProcessTask.stop();
				_currentProcessTask.waitForStop();
			}
			_currentProcessTask = null;
		}
	}
	
}
