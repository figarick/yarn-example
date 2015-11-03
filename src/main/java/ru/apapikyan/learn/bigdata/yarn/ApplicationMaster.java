package ru.apapikyan.learn.bigdata.yarn;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {

	// Hardcoded path to custom log_properties
	private static final String log4jPath = "log4j.properties";
	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		final Path jarPath = new Path("hdfs:///user//apapikyan//yex//inp//yarn-example-0.1.0.jar");
		final String hdfsPathToPatentFiles = args[0];
		final String destHdfsFolder = args[1];
		List<String> files = DownloadUtilities.getFileListing(hdfsPathToPatentFiles);
		String command = "";

		// ?????? can't force logging - PITA!!!!
		LOG.warn("Initializing ApplicationMaster");
		// Check whether customer log4j.properties file exists
		if (fileExist(log4jPath)) {
			try {
				Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class, log4jPath);
			} catch (Exception e) {
				LOG.fatal("Can not set up custom log4j properties. " + e);
			}
		}
		// ??????

		// Initialize clients to ResourceManager and NodeManagers
		LOG.warn("Initialize clients to ResourceManager and NodeManagers");
		Configuration conf = new YarnConfiguration();
		AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
		rmClient.init(conf);
		rmClient.start();

		NMClient nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();

		// Register with ResourceManager
		rmClient.registerApplicationMaster("", 0, "");
		// Priority for worker containers - priorities are intra-application
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);

		// Resource requirements for worker containers
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(128);
		capability.setVirtualCores(1);

		// Make container requests to ResourceManager
		for (int i = 0; i < files.size(); ++i) {
			ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
			rmClient.addContainerRequest(containerAsk);
		}

		// Obtain allocated containers and launch
		int allocatedContainers = 0;
		while (allocatedContainers < files.size()) {
			AllocateResponse response = rmClient.allocate(0);
			for (Container container : response.getAllocatedContainers()) {

				// Command to execute to download url to HDFS
				command = "java ru.apapikyan.learn.bigdata.yarn.DownloadFileService" + " "
				        + files.get(allocatedContainers) + " " + destHdfsFolder + " " + (allocatedContainers + 1);
				
				LOG.warn("Container command string is = " + command);

				// Launch container by create ContainerLaunchContext
				ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
				ctx.setCommands(Collections.singletonList(command));

				// Setup jar for ApplicationMaster
				LocalResource appMasterJar = Records.newRecord(LocalResource.class);
				FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
				appMasterJar.setType(LocalResourceType.FILE);
				appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
				appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
				appMasterJar.setSize(jarStat.getLen());
				appMasterJar.setTimestamp(jarStat.getModificationTime());
				ctx.setLocalResources(Collections.singletonMap("yarn-example-0.1.0.jar", appMasterJar));

				Map<String, String> appMasterEnv = new HashMap<String, String>();
				for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
					Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(), c.trim());
				}

				Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
				        ApplicationConstants.Environment.PWD.$() + File.separator + "*");
				ctx.setEnvironment(appMasterEnv);

				nmClient.startContainer(container, ctx);
				++allocatedContainers;
			}

			Thread.sleep(100);
		}

		// Now wait for containers to complete
		int completedContainers = 0;
		while (completedContainers < files.size()) {
			AllocateResponse response = rmClient.allocate(completedContainers / files.size());
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				LOG.warn("container status is " + status);
				++completedContainers;
			}
			Thread.sleep(1000);
		}

		// Un-register with ResourceManager
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
	}

	private static boolean fileExist(String filePath) {
		return new File(filePath).exists();
	}
}
