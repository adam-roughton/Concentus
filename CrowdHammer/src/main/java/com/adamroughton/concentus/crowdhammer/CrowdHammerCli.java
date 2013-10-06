package com.adamroughton.concentus.crowdhammer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.javatuples.Pair;

import asg.cliche.Command;
import asg.cliche.Shell;
import asg.cliche.ShellFactory;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.cluster.ClusterHandleSettings;
import com.adamroughton.concentus.cluster.coordinator.CoordinatorClusterHandle;
import com.adamroughton.concentus.crowdhammer.CrowdHammer.CrowdHammerEvent;
import com.adamroughton.concentus.crowdhammer.CrowdHammer.CrowdHammerEvent.EventType;
import com.adamroughton.concentus.crowdhammer.CrowdHammer.CrowdHammerListener;
import com.adamroughton.concentus.crowdhammer.TestTask.TestTaskEvent;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.util.FileLogger;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.minlog.Log;

/* TODO for now we assume that the test jar is on the classpath,
 * and take as an argument the test class name. Ideally we would
 * host a web server here that allows jar files to be uploaded for
 * testing (and additionally hosts a user interface for interacting
 * with the testing tools).
 */
public class CrowdHammerCli<TBuffer extends ResizingBuffer> {
	
	public final static String RES_DIR_OPTION = "resDir";
	
	public static void main(String[] args) {	
		@SuppressWarnings("static-access")
		Option resDirOption = OptionBuilder.withArgName("result directory path")
			.hasArg()
			.isRequired(false)
			.withDescription("the director to store results in")
			.create(RES_DIR_OPTION);
		
		Map<String, String> resDirCmdOptionMap = ConcentusExecutableOperations.parseCommandLine(
				"CrowdHammer", Arrays.asList(resDirOption), args, true);
		
		Path resDirPath;
		if (resDirCmdOptionMap.containsKey(RES_DIR_OPTION)) {
			resDirPath = Paths.get(resDirCmdOptionMap.get(RES_DIR_OPTION));
		} else {
			resDirPath = Paths.get(System.getProperty("user.dir"));
		}
		
		try {			
			Log.setLogger(new FileLogger(resDirPath.resolve("CrowdHammer.log")));
		} catch (IOException eIO) {
			Log.info("Unable to log to file! Using default stdout logger.", eIO);
		}
		
		Pair<ClusterHandleSettings, ConcentusHandle> coreComponents = 
				ConcentusExecutableOperations.createCoreComponents("CrowdHammer", args);
		
		ClusterHandleSettings clusterHandleSettings = coreComponents.getValue0();
		ConcentusHandle handle = coreComponents.getValue1();
		
		try (CoordinatorClusterHandle clusterHandle = new CoordinatorClusterHandle(clusterHandleSettings)) {
			System.out.println("Starting CrowdHammer Coordinator");
			clusterHandle.start();
			CrowdHammerCli<?> coordinator = new CrowdHammerCli<>(clusterHandle, handle, resDirPath);
			Shell shell = ShellFactory.createConsoleShell(">", "CrowdHammer", coordinator);
			coordinator.help();
			shell.commandLoop();
		} catch (Exception e) {
			Log.error("Error creating console shell", e);
		}
	}
	
	private final CrowdHammer _crowdHammer;
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final AtomicReference<Future<?>> _currentTask = new AtomicReference<>();
	
	private final CrowdHammerListener _crowdHammerListener = new CrowdHammerListener() {
		
		@Override
		public void onCrowdHammerEvent(CrowdHammer crowdHammer,
				CrowdHammerEvent event) {
			if (event.getEventType() == EventType.TEST_EVENT) {
				TestTaskEvent testTaskEvent = event.getTestTaskEvent();
				String outputString;
				if (testTaskEvent.getEventType() == TestTaskEvent.EventType.STATE_CHANGE) {
					outputString = "Test '" + testTaskEvent.getTestRunInfo() + "' changed to state " + testTaskEvent.getState() + 
							(testTaskEvent.hadException()? " with exception: " + Util.stackTraceToString(testTaskEvent.getException()) : "");
				} else if (testTaskEvent.getEventType() == TestTaskEvent.EventType.PROGRESS_MESSAGE) {
					outputString = testTaskEvent.getTestRunInfo() + ": " + testTaskEvent.getProgressMessage();
				} else {
					outputString = "Unknown test task event: " + testTaskEvent.toString();
				}
				Log.info(outputString);
				System.out.println(outputString);
			}
		}
	};
	
	public CrowdHammerCli(CoordinatorClusterHandle clusterHandle, ConcentusHandle concentusHandle, Path resultDir) throws Exception {
		_crowdHammer = CrowdHammer.createInstance(clusterHandle, concentusHandle, resultDir);
		_crowdHammer.getListenable().addListener(_crowdHammerListener);
		_crowdHammer.start();
	}
	
	@Command(name="start")
	public void startRun(final String testClassName) throws Exception {
		if (_currentTask.get() != null) {
			System.out.println("Test already running - stop the test before starting another.");
		}
		_currentTask.set(_executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					Util.runMain(testClassName);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					_currentTask.set(null);
				}
			}
			
		}));
	}
	
	@Command(name="stop")
	public void stopRun() {
		clearExisting();
	}
	
	@Command(name="quit")
	public void quit() {
		try {
			_crowdHammer.close();
		} catch (IOException eIO) {
			eIO.printStackTrace();
		}
		System.exit(0);
	}
	
	@Command(name="help")
	public void help() {
		System.out.println("Type start [test class name] to begin a run, stop to cancel any " +
					"existing runs, and quit to exit.");
	}
	
	private void clearExisting() {
		Future<?> currentTask = _currentTask.getAndSet(null);
		if (currentTask != null) {
			System.out.println("Stopping existing run");
			currentTask.cancel(true);
		}
		try {
			_crowdHammer.stopRun();
		} catch (InterruptedException e) {
		}
	}
	
}
