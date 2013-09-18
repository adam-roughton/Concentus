package com.adamroughton.concentus.cluster.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.adamroughton.concentus.ArrayBackedComponentResolver;
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
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Paths;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.javatuples.Pair;

public final class Guardian implements ClusterService<GuardianState> {
	
	public static final String SERVICE_TYPE = "guardian";
	
	private static class GuardianDeployment implements ServiceDeployment<GuardianState> {
		
		private String[] _serviceVmArgs;
		private String[] _args;
		
		// for Kryo
		@SuppressWarnings("unused")
		private GuardianDeployment() { }
		
		public GuardianDeployment(String[] serviceVmArgs, String[] args) {
			_serviceVmArgs = serviceVmArgs;
			_args = args;
		}
		
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
			return new Guardian(_args, _serviceVmArgs, serviceContext, handle);
		}

		@Override
		public void onPreStart(StateData<GuardianState> stateData) {
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
	
	private Future<?> _currentProcessTask = null;
	
	public Guardian(String[] args, String[] serviceVmArgs, ServiceContext<GuardianState> serviceContext, ConcentusHandle handle) {
		_serviceContext = Objects.requireNonNull(serviceContext);
		_kryo = Util.newKryoInstance();
		
		_args = Objects.requireNonNull(args);
		_serviceVmArgs = Objects.requireNonNull(serviceVmArgs);
		_classpath = System.getProperty("java.class.path");
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
		GuardianDeploymentReturnInfo retInfo = stateData.getData(GuardianDeploymentReturnInfo.class);
		stateData.setDataForCoordinator(retInfo);
	}
	
	private void onRun(StateData<GuardianState> stateData, final ClusterHandle cluster) throws Exception {	
		stopHostProcess();
		
		// get allocated service deployment
		Pair<?, ?> deploymentPair = stateData.getData(Pair.class);
		final ServiceDeployment<?> deployment = Util.checkedCast(deploymentPair.getValue0(), ServiceDeployment.class);
		
		@SuppressWarnings("unchecked")
		ComponentResolver<? extends ResizingBuffer> resolver = Util.checkedCast(deploymentPair.getValue1(), ComponentResolver.class);
		
		if (deployment != null && resolver != null && ServiceState.class.equals(deployment.stateType())) {
			Log.info("Deploying " + deployment.serviceType());
			
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
			
			final ProcessBuilder processBuilder = new ProcessBuilder(arguments);
			processBuilder.redirectOutput(Redirect.INHERIT);
			Runnable processTask = new Runnable() {
				
				@Override
				public void run() {
					final Process process;
					try {
						process = processBuilder.start();
					} catch (IOException eIO) {
						StringWriter stackTraceWriter = new StringWriter();
						eIO.printStackTrace(new PrintWriter(stackTraceWriter));
						_serviceContext.enterState(GuardianState.READY, 
								new GuardianDeploymentReturnInfo(ReturnType.ERROR, "Could not start the guardian service host process: " 
										+ stackTraceWriter.toString()), 
								GuardianState.RUN);
						return;
					}
					try {
						final StringBuilder stdErrBuilder = new StringBuilder();
						Thread stdErrCollector = new Thread() {
						
							public void run() {
								try (BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
									String readLine;
									do {
										readLine = stderrReader.readLine();
										stdErrBuilder.append(readLine + '\n');
									} while (readLine != null);
								} catch (IOException eIO) {
								}
							}
						};
						stdErrCollector.start();				
					
						BufferedOutputStream hostStdIn = new BufferedOutputStream(process.getOutputStream());
						Output output = new Output(hostStdIn);
						_kryo.writeClassAndObject(output, initMsg);
						output.flush();
						output.close();
						
						int retCode = process.waitFor();
						stdErrCollector.join();
						
						GuardianDeploymentReturnInfo retInfo;
						if (retCode == 0) {
							retInfo = new GuardianDeploymentReturnInfo(ReturnType.OK, null);
						} else {
							retInfo = new GuardianDeploymentReturnInfo(ReturnType.ERROR, stdErrCollector.toString());
						}
						_serviceContext.enterState(GuardianState.READY, retInfo, GuardianState.RUN);
					} catch (InterruptedException eInterrupt) {
						process.destroy();
					} finally {
						try {
							process.waitFor();
						} catch (InterruptedException e) {
						}
					}
				}
			};
			_currentProcessTask = _taskExecutor.submit(processTask);
		}
	}
	
	private void onShutdown(StateData<GuardianState> stateData, ClusterHandle cluster) throws Exception {
		stopHostProcess();
	}
	
	private void stopHostProcess() throws Exception {
		if (_currentProcessTask != null) {
			// wait for the host process to stop
			TimeoutTracker timeoutTracker = new TimeoutTracker(5000, TimeUnit.MILLISECONDS);
			try {
				_currentProcessTask.get(timeoutTracker.getTimeout(), timeoutTracker.getUnit());
			} catch (TimeoutException eTimeOut) {
				_currentProcessTask.cancel(true);
			}
			_currentProcessTask = null;
		}
	}
	
}
