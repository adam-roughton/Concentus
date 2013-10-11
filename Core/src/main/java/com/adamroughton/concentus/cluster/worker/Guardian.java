package com.adamroughton.concentus.cluster.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.adamroughton.concentus.ArrayBackedComponentResolver;
import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.GuardianInit;
import com.adamroughton.concentus.data.cluster.kryo.GuardianState;
import com.adamroughton.concentus.data.cluster.kryo.ProcessReturnInfo;
import com.adamroughton.concentus.data.cluster.kryo.ProcessReturnInfo.ReturnType;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.util.ProcessTask;
import com.adamroughton.concentus.util.ProcessTask.ProcessDelegate;
import com.adamroughton.concentus.util.TimeoutTracker;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

import java.io.BufferedOutputStream;
import java.nio.file.Paths;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.javatuples.Pair;

public final class Guardian implements ClusterService<GuardianState> {
	
	public static final String SERVICE_TYPE = "guardian";
	
	private static class GuardianDeployment extends ServiceDeploymentBase<GuardianState> {
		
		private String[] _serviceVmArgs;
		private String[] _args;
		
		// for Kryo
		@SuppressWarnings("unused")
		private GuardianDeployment() { }
		
		public GuardianDeployment(String[] serviceVmArgs, String[] args) {
			super(new ServiceInfo<>(SERVICE_TYPE, GuardianState.class));
			_serviceVmArgs = serviceVmArgs;
			_args = args;
		}

		@Override
		public <TBuffer extends ResizingBuffer> ClusterService<GuardianState> createService(
				int serviceId,
				StateData initData,
				ServiceContext<GuardianState> serviceContext,
				ConcentusHandle handle, MetricContext metricContext,
				ComponentResolver<TBuffer> resolver) {
			return new Guardian(_args, _serviceVmArgs, serviceContext, handle);
		}

		@Override
		public void onPreStart(StateData stateData) {
		}
		
	}
	
	public static void main(final String[] args) {
		@SuppressWarnings("static-access")
		Option serviceVmOption = OptionBuilder.withArgName("ZooKeeper Address")
			.hasArg()
			.isRequired(true)
			.withDescription("the address of the ZooKeeper server")
			.create("svmargs");
		Pair<Map<String, String>, String[]> cmdLinePair = ConcentusExecutableOperations
				.parseCommandLineKeepRemaining("Guardian", Arrays.asList(serviceVmOption), args, true);
		String svmArgsString = cmdLinePair.getValue0().get("svmargs");
		String[] svmArgs;
		if (svmArgsString == null) {
			svmArgs = new String[0];
		} else {
			svmArgs = svmArgsString.split(" ");
		}
		String[] remArgs = cmdLinePair.getValue1();
		
		try {
			ConcentusExecutableOperations.executeClusterService(remArgs, 
					new GuardianDeployment(svmArgs, remArgs),
					new ArrayBackedComponentResolver());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private final ExecutorService _taskExecutor = Executors.newCachedThreadPool();
	private final ServiceContext<GuardianState> _serviceContext;
	private final String[] _args;
	private final String[] _serviceVmArgs;
	private final String _classpath;
	private final Kryo _kryo;
	
	private ProcessTask _currentProcessTask = null;
	
	public Guardian(String[] args, String[] serviceVmArgs, ServiceContext<GuardianState> serviceContext, ConcentusHandle handle) {
		_serviceContext = Objects.requireNonNull(serviceContext);
		_kryo = Util.newKryoInstance();
		
		_args = Objects.requireNonNull(args);
		_serviceVmArgs = Objects.requireNonNull(serviceVmArgs);
		_classpath = System.getProperty("java.class.path");
	}

	@Override
	public void onStateChanged(GuardianState newServiceState, int stateChangeIndex,
			StateData stateData, ClusterHandle cluster) throws Exception {
		switch (newServiceState) {
			case READY:
				onReady(stateChangeIndex, stateData, cluster);
				break;
			case RUN:
				try {
					onRun(stateChangeIndex, stateData, cluster);
				} catch (Exception e) {
					_serviceContext.enterState(GuardianState.READY, 
							new ProcessReturnInfo(ReturnType.ERROR, "Failed to enter the run state: " 
									+ Util.stackTraceToString(e)), 
							stateChangeIndex);
				}
				break;
			case SHUTDOWN:
				onShutdown(stateData, cluster);
				break;
			default:
		}
	}
	
	private void onReady(int stateChangeIndex, StateData stateData, ClusterHandle cluster) throws Exception {
		stopHostProcess();
		ProcessReturnInfo retInfo = stateData.getData(ProcessReturnInfo.class);
		stateData.setDataForCoordinator(retInfo);
	}
	
	private void onRun(final int stateChangeIndex, StateData stateData, final ClusterHandle cluster) throws Exception {	
		stopHostProcess();
		
		// get allocated service deployment
		Pair<?, ?> deploymentPair = stateData.getData(Pair.class);
		final ServiceDeployment<?> deployment = Util.checkedCast(deploymentPair.getValue0(), ServiceDeployment.class);
		if (deployment == null) {
			throw new RuntimeException("The deployment was null");
		}
		
		ServiceInfo<?> serviceInfo = deployment.serviceInfo();
		if (!serviceInfo.stateType().equals(ServiceState.class)) {
			throw new RuntimeException("This guardian service only supports deployments with state type " + 
				ServiceState.class.getCanonicalName() + "; the service allocated for deployment requires " +
					" a state type of " + serviceInfo.stateType().getCanonicalName());
		}
		
		@SuppressWarnings("unchecked")
		ComponentResolver<? extends ResizingBuffer> resolver = Util.checkedCast(deploymentPair.getValue1(), ComponentResolver.class);
		if (resolver == null) {
			throw new RuntimeException("The provided component resolver was null");
		}
		
		Log.info("Deploying " + serviceInfo.serviceType());
		
		@SuppressWarnings("unchecked")
		final GuardianInit initMsg = new GuardianInit(_args, (ServiceDeployment<ServiceState>) deployment, resolver);
		
		// use the jvm that ran this process
		String javaHome = System.getProperty("java.home");
		
		List<String> arguments = new ArrayList<>();
		arguments.add(Paths.get(javaHome, "bin").resolve("java").toString());
		arguments.addAll(Arrays.asList(_serviceVmArgs));
		arguments.add("-cp");
		arguments.add(_classpath);
		arguments.add(GuardianServiceHost.class.getCanonicalName());
		
		ProcessDelegate processTaskDelegate = new ProcessDelegate() {
			
			@Override
			public void configureProcess(final Process process) throws InterruptedException {	
				// write init message to GuardianServiceHost
				BufferedOutputStream hostStdIn = new BufferedOutputStream(process.getOutputStream());
				Output output = new Output(hostStdIn);
				_kryo.writeClassAndObject(output, initMsg);
				output.flush();
				output.close();				
			}

			@Override
			public void onStop(ReasonType reason, int retCode,
					String stdoutBuffer, String stdErrBuffer, Exception e) {
				ProcessReturnInfo retInfo;
				if (reason == ReasonType.ERROR) {
					retInfo = new ProcessReturnInfo(ReturnType.ERROR, "Error running process: " 
									+ Util.stackTraceToString(e));
				} else if (retCode == 0) {
					retInfo = new ProcessReturnInfo(ReturnType.OK, null);
				} else {
					retInfo = new ProcessReturnInfo(ReturnType.ERROR, stdErrBuffer);
				}
				Log.info("Guardian.onRun: Signalling process death - retInfo=" + retInfo + ", stateChangeIndex=" + stateChangeIndex);
				_serviceContext.enterState(GuardianState.READY, retInfo, stateChangeIndex);
			}
		};
		// buffer up to 512kb of stderr (buffer is chars) (ZooKeeper max is 1MB)
		int stdErrBufferLength = 256 * 1024;
		_currentProcessTask = new ProcessTask(deployment.serviceInfo().serviceType(), 0, stdErrBufferLength, arguments, processTaskDelegate);
		_taskExecutor.execute(_currentProcessTask);
	}
	
	private void onShutdown(StateData stateData, ClusterHandle cluster) throws Exception {
		stopHostProcess();
	}
	
	private void stopHostProcess() throws Exception {
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
