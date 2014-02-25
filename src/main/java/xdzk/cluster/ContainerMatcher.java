/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xdzk.cluster;

import java.util.Iterator;


/**
 * Strategy interface for matching a ModuleDeploymentRequest to one of the candidate container nodes.
 *
 * @author Mark Fisher
 */
public interface ContainerMatcher {

	/**
	 * Matches the provided module against one of the candidate containers.
	 *
	 * @param moduleDeploymentRequest the module deployment request
	 * @param candidates set of containers to which a module could be deployed
	 * @return the matched container, or <code>null</code> if no suitable match
	 */
	// TODO: The module name should be replaced with ModuleDeploymentRequest which will
	// include the criteria for matching against container attributes as well as other
	// metadata extracted from the stream deployment manifest, such as the number of instances.
	// Also, the String return and collection element type should be Container instances
	// where ultimately matching will take Container attributes and metrics into consideration.
	Container match(String module, Iterator<Container> candidates);

}
