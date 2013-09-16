package com.adamroughton.concentus.cluster.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.GuardianDeploymentReturnInfo;
import com.adamroughton.concentus.data.cluster.kryo.GuardianInit;
import com.adamroughton.concentus.data.cluster.kryo.GuardianState;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.data.cluster.kryo.GuardianDeploymentReturnInfo.ReturnType;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.util.TimeoutTracker;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Paths;

public final class Guardian implements ClusterService<GuardianState> {
	
	public static final String SERVICE_TYPE = "guardian";
	
	public static void main(final String[] args) {
		ConcentusExecutableOperations.executeClusterService(args, new ServiceDeployment<GuardianState>() {

			@Override
			public Class<GuardianState> stateType() {
				return GuardianState.class;
			}

			@Override
			public String serviceType() {
				return SERVICE_TYPE;
			}

			@Override
			public String[] serviceDependencies() {
				return new String[0];
			}

			@Override
			public <TBuffer extends ResizingBuffer> ClusterService<GuardianState> createService(
					int serviceId,
					ServiceContext<GuardianState> serviceContext,
					ConcentusHandle handle, MetricContext metricContext,
					ComponentResolver<TBuffer> resolver) {
				return new Guardian(args, serviceContext, handle);
			}

			@Override
			public void onPreStart(StateData<GuardianState> stateData) {
			}

		});
	}

	private final ExecutorService _taskExecutor = Executors.newCachedThreadPool();
	private final ServiceContext<GuardianState> _serviceContext;
	private final String[] _args;
	private final String _classpath;
	private final List<String> _vmArgs;
	private final Kryo _kryo;
	private Process _guardianHostProcess;
	
	private final Object _taskStopMonitor = new Object();
	private boolean _isRunning = false;
	
	public Guardian(String[] args, ServiceContext<GuardianState> serviceContext, ConcentusHandle handle) {
		_serviceContext = Objects.requireNonNull(serviceContext);
		_kryo = Util.newKryoInstance();
		
		_args = Objects.requireNonNull(args);
		_classpath = System.getProperty("java.class.path");
		
		RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
		_vmArgs = runtimeMxBean.getInputArguments();
	}

	@Override
	public void onStateChanged(GuardianState newServiceState,
			StateData<GuardianState> stateData, ClusterHandle cluster) throws Exception {
		switch (newServiceState) {
			case READY:
				onReady(stateData, cluster);
				break;
			case RUN:
				onRun(stateData, cluster);
				break;
			case SHUTDOWN:
				onShutdown(stateData, cluster);
				break;
			default:
		}
		
	}
	
	private void onReady(StateData<GuardianState> stateData, ClusterHandle cluster) throws Exception {
		stopHostProcess();
	}
	
	private void onRun(StateData<GuardianState> stateData, final ClusterHandle cluster) throws Exception {
		// get allocated service deployment
		ServiceDeployment<?> deployment = stateData.getData(ServiceDeployment.class);
		if (deployment != null && ServiceState.class.equals(Util.getGenericParameter(deployment, ServiceDeployment.class, 0))) {
			@SuppressWarnings("unchecked")
			GuardianInit initMsg = new GuardianInit(_args, (ServiceDeployment<ServiceState>) deployment);
			
			// use the jvm that ran this process
			String javaHome = System.getProperty("java.home");
			
			List<String> arguments = new ArrayList<>();
			arguments.add(Paths.get(javaHome, "bin").resolve("java").toString());
			arguments.addAll(_vmArgs);
			arguments.add("-cp");
			arguments.add(_classpath);
			arguments.add(GuardianServiceHost.class.getCanonicalName());
			for (String arg : _args) {
				arguments.add(arg);
			}
			
			ProcessBuilder processBuilder = new ProcessBuilder(arguments);
			_guardianHostProcess = processBuilder.start();
			_isRunning = true;
			
			BufferedOutputStream hostStdIn = new BufferedOutputStream(_guardianHostProcess.getOutputStream());
			Output output = new Output(hostStdIn);
			_kryo.writeClassAndObject(output, initMsg);
			output.flush();
			
			_taskExecutor.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						int retCode = _guardianHostProcess.waitFor();
						GuardianDeploymentReturnInfo retInfo;
						if (retCode == 0) {
							retInfo = new GuardianDeploymentReturnInfo(ReturnType.OK, null);
						} else {
							StringBuilder reasonBuilder = new StringBuilder();
							// there was an error, read from std err
							try (BufferedReader stderrReader = new BufferedReader(new InputStreamReader(_guardianHostProcess.getErrorStream()))) {
								String readLine;
								do {
									readLine = stderrReader.readLine();
									reasonBuilder.append(readLine + '\n');
								} while (readLine != null);
							} catch (IOException eIO) {
							}
							retInfo = new GuardianDeploymentReturnInfo(ReturnType.ERROR, reasonBuilder.toString());
						}
						
						_serviceContext.enterState(GuardianState.READY, retInfo, GuardianState.RUN);
						
						synchronized (_taskStopMonitor) {
							_isRunning = false;
							_taskStopMonitor.notifyAll();
						}
						
					} catch (InterruptedException e) {
					}
				}
			});
		}
	}
	
	private void onShutdown(StateData<GuardianState> stateData, ClusterHandle cluster) throws Exception {
		stopHostProcess();
	}
	
	private void stopHostProcess() {
		if (_guardianHostProcess != null) {
			try {
				// wait for the host process to stop
				TimeoutTracker timeoutTracker = new TimeoutTracker(5000, TimeUnit.MILLISECONDS);
				synchronized (_taskStopMonitor) {
					while (_isRunning && !timeoutTracker.hasTimedOut()) {
						_taskStopMonitor.wait(timeoutTracker.remainingMillis());
					}
				}
			} catch (Exception e) {
			} finally {
				// force host process termination if it hasn't already terminated
				_guardianHostProcess.destroy();
				_guardianHostProcess = null;
				synchronized (_taskStopMonitor) {
					_isRunning = false;
				}
			}
		}
	}
	
	public static class GuardianServiceHost {
		
		public static void main(String[] args) {
			// read service in from StdIn
			Kryo kryo = Util.newKryoInstance();
			Input input = new Input(new BufferedInputStream(System.in));
			GuardianInit initMsg = (GuardianInit) kryo.readClassAndObject(input);
			if (initMsg == null) {
				System.exit(1); 
			}
			
			ConcentusExecutableOperations.executeClusterService(initMsg.cmdLineArgs(), initMsg.deployment());
		}
		
	}
	
}
