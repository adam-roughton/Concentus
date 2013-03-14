package com.adamroughton.consentus.cluster.coordinator;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.cluster.ClusterParticipant;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.consentus.cluster.ClusterPath.*;

public final class ClusterCoordinator extends ClusterParticipant implements Cluster, Closeable {

	public ClusterCoordinator(String zooKeeperAddress, String root,
			FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, exHandler);
	}

	@Override
	public List<byte[]> getAssignmentRequests(String serviceType) {
		List<byte[]> assignmentRequests = null;
		try {
			String serviceAssignmentPath = ZKPaths.makePath(getPath(ASSIGN_REQ), serviceType);
			ensurePathCreated(serviceAssignmentPath);
			List<String> assignmentReqPaths = getClient().getChildren().forPath(serviceAssignmentPath);
			assignmentRequests = new ArrayList<>(assignmentReqPaths.size());
			for (String path : assignmentReqPaths) {
				byte[] reqData = getClient().getData().forPath(path);
				assignmentRequests.add(reqData);
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return assignmentRequests;
	}

	@Override
	public void setAssignment(String serviceType, UUID serviceId,
			byte[] assignment) {
		String serviceTypeAssignmentPath = ZKPaths.makePath(getPath(ASSIGN_RES), serviceType);
		ensurePathCreated(serviceTypeAssignmentPath);
		String serviceAssignmentPath = ZKPaths.makePath(serviceTypeAssignmentPath, Util.toHexString(serviceId));
		createOrSetEphemeral(serviceAssignmentPath, assignment);
	}

	@Override
	public void setState(int state) {
		// clear all assignment nodes
		
		
		
	}

	@Override
	public void waitForReady() throws InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void waitForReady(long time, TimeUnit unit)
			throws InterruptedException {
		// TODO Auto-generated method stub
		
	}
	
}
