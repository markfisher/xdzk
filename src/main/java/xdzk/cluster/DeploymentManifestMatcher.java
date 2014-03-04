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

import xdzk.core.ModuleDescriptor;

import java.util.ArrayList;
import java.util.Collection;
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
	 * Current index for iterating over containers.
	 */
	private int index = 0;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<Container> match(ModuleDescriptor moduleDescriptor, ContainerRepository containerRepository) {
		List<Container> containers = new ArrayList<Container>();
		Iterator<Container> iterator = containerRepository.getContainerIterator();
		while (iterator.hasNext()) {
			containers.add(iterator.next());
		}

		if ("all".equals(moduleDescriptor.getGroup())) {
			return containers;
		}
		else {
			// todo: needs unit testing
			List<Container> targets = new ArrayList<Container>();
			while (targets.size() < moduleDescriptor.getCount()) {
				if (index + 1 > containers.size()) {
					index = 0;
				}
				targets.add(containers.get(index++));
			}

			return targets;
		}
	}

}
