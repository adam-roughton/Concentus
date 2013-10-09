package com.adamroughton.concentus.application;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.DirectoryStream.Filter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;
import com.adamroughton.concentus.service.spark.SparkMasterServiceDeployment;
import com.adamroughton.concentus.service.spark.SparkStreamingDriverDeployment;
import com.adamroughton.concentus.service.spark.SparkWorkerServiceDeployment;
import com.google.common.base.Objects;

class SparkDriverConfigurator implements DeploymentConfigurator {

	static class DataCache {
		
		public final static String SPARK_HOME;
		public final static String[] DRIVER_DEPENDENCIES;
		
		static {
			// running from the workingDirectory
			Path workingDir = Paths.get(System.getProperty("user.dir"));
			Path sparkHomePath = assertDirExists(workingDir.resolve("spark-0.7.3"));
			SPARK_HOME = sparkHomePath.toString();
			
			// spark requires the path of the driver jar and all of its dependencies
			List<String> jarFilePaths = new ArrayList<>();
			for (String projectJar : new String[] {
					"concentus-core-1.0-SNAPSHOT.jar", 
					"concentus-service-1.0-SNAPSHOT.jar", 
					"concentus-sparkstreamingdriver-1.0-SNAPSHOT.jar",
					"concentus-crowdhammer-1.0-SNAPSHOT.jar",
					"concentus-tests-1.0-SNAPSHOT.jar"}) {
				Path projectJarPath = assertFileExists(workingDir.resolve(projectJar));
				jarFilePaths.add(projectJarPath.toString());
			}
			
			// not sure which specific libraries are needed, so just grab them all
			Path libDir = assertDirExists(workingDir.resolve("lib"));
			Filter<Path> jarFilter = new Filter<Path>() {
				
				@Override
				public boolean accept(Path path) throws IOException {
					if (Files.isDirectory(path)) return false;
					String ext = FilenameUtils.getExtension(path.toString());
					return Objects.equal(ext, "jar");
				}
			};
			try (DirectoryStream<Path> libDirStream = Files.newDirectoryStream(libDir, jarFilter)) {
				for (Path path : libDirStream) {
					String jarPath = path.toString();
					if (jarPath.contains("asm-all-3.3.1")) continue;
					if (jarPath.contains("asm-4.0")) continue;
					jarFilePaths.add(path.toString());
				}
			} catch (IOException eIO) {
				throw new RuntimeException("Unable to get spark streaming driver jar dependency paths", eIO);
			}
			DRIVER_DEPENDENCIES = jarFilePaths.toArray(new String[jarFilePaths.size()]);
		}
		
		private static Path assertDirExists(Path path) {
			return assertPathExists(path, true);
		}
		
		private static Path assertFileExists(Path path) {
			return assertPathExists(path, false);
		}
		
		private static Path assertPathExists(Path path, boolean isDir) {
			if (isDir) {
				if (!Files.isDirectory(path)) {
					throw new RuntimeException("The folder '" + path.toString() + "' was not found: this test " +
							"should be executed from the Concentus working directory");
				}
			} else {
				if (!Files.exists(path)) {
					throw new RuntimeException("The file '" + path.toString() + "' was not found: this test " +
							"should be executed from the Concentus working directory");
				}
			}
			return path;
		}
	}
	
	@Override
	public TestDeploymentSet configure(TestDeploymentSet deploymentSet, int receiverCount) {
		return deploymentSet
			.addDeployment(new SparkMasterServiceDeployment(DataCache.SPARK_HOME, 7077), 1)
			.addDeployment(new SparkWorkerServiceDeployment(DataCache.SPARK_HOME), receiverCount)
			.addDeployment(new SparkStreamingDriverDeployment(DataCache.SPARK_HOME, DataCache.DRIVER_DEPENDENCIES, receiverCount, -1, 
					Constants.MSG_BUFFER_ENTRY_LENGTH, Constants.MSG_BUFFER_ENTRY_LENGTH, -1, 
					Constants.MSG_BUFFER_ENTRY_LENGTH), 1);
	}

	@Override
	public String deploymentName() {
		return "spark";
	}
	
}
