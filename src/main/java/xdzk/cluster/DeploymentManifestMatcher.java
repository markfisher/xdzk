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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdzk.core.ModuleDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link ContainerMatcher} that returns a collection of
 * containers to deploy a {@link ModuleDescriptor} to. If the manifest specifies
 * a group of "all" for a module, all available containers are returned.
 * Otherwise a collection of at least one {@link Container} is returned
 * for module deployment. An attempt at round robin distribution will be
 * made (but not guaranteed).
 *
 * @author Patrick Peralta
 */
public class DeploymentManifestMatcher implements ContainerMatcher {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(DeploymentManifestMatcher.class);

	/**
	 * Current index for iterating over containers.
	 */
	private int index = 0;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<Container> match(ModuleDescriptor moduleDescriptor, ContainerRepository containerRepository) {
		// todo: this method needs unit testing
		LOG.debug("Matching containers for module {}", moduleDescriptor);

		String group = moduleDescriptor.getGroup();
		List<Container> containers = new ArrayList<Container>();

		for (Iterator<Container> iterator = containerRepository.getContainerIterator(); iterator.hasNext();) {
			Container container = iterator.next();
			LOG.trace("Evaluating container {}", container);
			if (group == null || container.getGroups().contains(group)) {
				LOG.trace("\tAdded container {}", container);
				containers.add(container);
			}
		}

		int count = moduleDescriptor.getCount();
		if (count <= 0) {
			// count of 0 means all members of the group;
			// if no group specified it means all containers
			return containers;
		}
		else if (count == 1) {
			if (index + 1 > containers.size()) {
				index = 0;
			}
			return Collections.singleton(containers.get(index++));
		}
		else {
			// create a new list with the specific number
			// of targeted containers
			List<Container> targets = new ArrayList<Container>();
			// todo: this will create the exact number of module deployments
			// regardless of the number of containers; this means some
			// containers may get multiple deployments if the module
			// specifies more deployments than containers
			while (targets.size() < count) {
				if (index + 1 > containers.size()) {
					index = 0;
				}
				targets.add(containers.get(index++));
			}
			return targets;
		}
	}

}
