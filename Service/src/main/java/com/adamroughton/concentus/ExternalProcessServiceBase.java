package com.adamroughton.concentus;

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
	private final String _name;
	private final ServiceContext<ServiceState> _serviceContext;
	private final ConcentusHandle _concentusHandle;
	private ProcessTask _currentProcessTask = null;
	
	public ExternalProcessServiceBase(String name, ServiceContext<ServiceState> serviceContext, ConcentusHandle concentusHandle) {
		_name = Objects.requireNonNull(name);
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
		
		ProcessDelegate processTaskDelegate = new ProcessDelegate() {
			
			@Override
			public void configureProcess(final Process process) throws InterruptedException {				
			}

			@Override
			public void onStop(ReasonType reason, int retCode, String stdoutBuffer,
					String stdErrBuffer, Exception exception) {
				ProcessReturnInfo retInfo;
				if (reason == ReasonType.ERROR) {
					retInfo = new ProcessReturnInfo(ReturnType.ERROR, Util.stackTraceToString(exception));
				} else if (retCode == 0) {
					retInfo = new ProcessReturnInfo(ReturnType.OK, null);
				} else {
					retInfo = new ProcessReturnInfo(ReturnType.ERROR, stdErrBuffer);
				}
				
				Log.info("ExternalProcessServiceBase.startProcess: Signalling process death - retInfo=" + retInfo);
				_serviceContext.enterState(ServiceState.SHUTDOWN, retInfo);
			}
			
		};
		int stdErrBufferLength = 256 * 1024;
		_currentProcessTask = new ProcessTask(_name, 0, stdErrBufferLength, commands, processTaskDelegate);
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
