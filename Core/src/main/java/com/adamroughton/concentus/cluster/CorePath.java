/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.concentus.cluster;

import java.util.Objects;
import com.netflix.curator.utils.ZKPaths;

public enum CorePath implements ClusterPath {
	APP_ROOT				(null, true),
	
	APPLICATION   			(APP_ROOT, "application"),
	
	/*
	 * ========
	 * SERVICES
	 * ========
	 * 
	 * AppRoot
	 * 	 - services
	 *      - type1 [SERVICE_ROOT]
	 *         - instance1
	 *            - stateSignal {StateEntry}
	 *            - state {StateEntry}
	 *            - serviceInitData: {ServiceInit}
	 *        - instance2
	 *            ...
	 *      - type2
	 *            ...     
	 * 
	 */
	SERVICES				(APP_ROOT, "services"),
	SERVICE_ROOT			(null, false),
	SERVICE_INIT_DATA		(SERVICE_ROOT, "serviceInitData"),
	SERVICE_STATE_TYPE		(SERVICE_ROOT, "stateType"),
	SERVICE_STATE_SIGNAL	(SERVICE_ROOT, "stateSignal"),
	SERVICE_STATE			(SERVICE_ROOT, "state"),
	
	/*
	 * =================
	 * SERVICE ENDPOINTS
	 * =================
	 * 
	 * AppRoot
	 * 	 - serviceEndpoints
	 *      - type1 
	 *      	- instance1 {ServiceEndpoint}
	 *      	- instance2 {ServiceEndpoint}
	 *      - type2
	 *          ...
	 */
	
	SERVICE_ENDPOINTS		(APP_ROOT, "serviceEndpoints"),
	
	/*
	 * ================
	 * METRIC META DATA
	 * ================
	 * 
	 * AppRoot
	 *   - metricCollector {ServiceEndpoint}
	 *   - metrics
	 *     - metricSourceId1_metricId1 {MetricMetaData}
	 *     - metricSourceId1_metricId2 {MetricMetaData}
	 *     - metricSourceId1_metricId3 {MetricMetaData}
	 *     - metricSourceId2_metricId1 {MetricMetaData}
	 *     	 ...
	 */
	
	METRICS					(APP_ROOT, "metrics"),
	METRIC_COLLECTOR		(APP_ROOT, "metricCollector"),
	
	;
	
	private final String _relativePath;
	private final boolean _isRelativeToRoot;
	
	private CorePath(String relativePath, boolean isRelativeToRoot) {
		_relativePath = relativePath;
		_isRelativeToRoot = isRelativeToRoot;
	}
	
	private CorePath(ClusterPath parentPath, String relativePath) {
		Objects.requireNonNull(relativePath);
		
		if (parentPath.getRelativePath() == null) {
			_relativePath = relativePath;
		} else {
			_relativePath = ZKPaths.makePath(parentPath.getRelativePath(), relativePath);			
		}
		_isRelativeToRoot = parentPath.isRelativeToRoot();
	}
	
	public String getRelativePath() {
		return _relativePath;
	}
	
	public String getAbsolutePath(String basePath) {
		if (_relativePath == null) {
			return basePath;
		} else {
			return ZKPaths.makePath(basePath, _relativePath);			
		}
	}
	
	public boolean isRelativeToRoot() {
		return _isRelativeToRoot;
	}
}
