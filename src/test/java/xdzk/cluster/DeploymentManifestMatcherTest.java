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

import org.junit.Test;
import xdzk.core.Module;
import xdzk.core.ModuleDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Tests for {@link DeploymentManifestMatcher}.
 *
 * @author Patrick Peralta
 */
public class DeploymentManifestMatcherTest {

	/**
	 * Test container matching for a module descriptor that specifies one containers.
	 */
	@Test
	public void testOneModule() {
		DeploymentManifestMatcher matcher = new DeploymentManifestMatcher();

		ModuleDescriptor descriptor = newModuleDescriptor(null, 1);

		Collection<Container> one = matcher.match(descriptor, new ContainerRepositoryBuilder().add(1).build());
		assertEquals(1, one.size());
		assertEquals(0, one.iterator().next().getGroups().size());

		Collection<Container> five = matcher.match(descriptor, new ContainerRepositoryBuilder().add(5).build());
		assertEquals(1, five.size());
	}

	/**
	 * Test container matching for a module descriptor that specifies two containers.
	 */
	@Test
	public void testTwoModules() {
		DeploymentManifestMatcher matcher = new DeploymentManifestMatcher();

		ModuleDescriptor descriptor = newModuleDescriptor(null, 2);

		Collection<Container> one = matcher.match(descriptor, new ContainerRepositoryBuilder().add(1).build());
		assertEquals(1, one.size());
		assertEquals(0, one.iterator().next().getGroups().size());

		Collection<Container> five = matcher.match(descriptor, new ContainerRepositoryBuilder().add(5).build());
		assertEquals(2, five.size());
	}

	/**
	 * Test container matching for a module descriptor that specifies all containers.
	 */
	@Test
	public void testAll() {
		DeploymentManifestMatcher matcher = new DeploymentManifestMatcher();

		ModuleDescriptor descriptor = newModuleDescriptor(null, 0);

		Collection<Container> one = matcher.match(descriptor, new ContainerRepositoryBuilder().add(1).build());
		assertEquals(1, one.size());
		assertEquals(0, one.iterator().next().getGroups().size());

		Collection<Container> five = matcher.match(descriptor, new ContainerRepositoryBuilder().add(5).build());
		assertEquals(5, five.size());
	}

	/**
	 * Test container matching for a module descriptor that specifies all containers
	 * in a group.
	 */
	@Test
	public void testGroup() {
		DeploymentManifestMatcher matcher = new DeploymentManifestMatcher();

		ModuleDescriptor descriptor = newModuleDescriptor("blue", 0);
		Collection<Container> containers = matcher.match(descriptor,
				new ContainerRepositoryBuilder().add(5, "blue").add(5, "red").build());

		assertEquals(5, containers.size());
		for (Container container : containers) {
			assertTrue(container.getGroups().contains("blue"));
		}
	}

	/**
	 * Test container matching for a module descriptor that specifies five containers
	 * in a group.
	 */
	@Test
	public void testFiveGroup() {
		DeploymentManifestMatcher matcher = new DeploymentManifestMatcher();

		ModuleDescriptor descriptor = newModuleDescriptor("blue", 5);
		Collection<Container> containers = matcher.match(descriptor,
				new ContainerRepositoryBuilder().add(10, "blue").add(10, "red").build());

		assertEquals(5, containers.size());
		for (Container container : containers) {
			assertTrue(container.getGroups().contains("blue"));
		}
	}

	/**
	 * Test container matching where the module descriptor indicates more
	 * containers than are available.
	 */
	@Test
	public void testMoreModulesThanContainers() {
		DeploymentManifestMatcher matcher = new DeploymentManifestMatcher();
		ModuleDescriptor descriptor = newModuleDescriptor(null, 20);

		Collection<Container> containers = matcher.match(descriptor, new ContainerRepositoryBuilder().add(5).build());
		assertEquals(5, containers.size());
	}

	/**
	 * Test multiple uses of a {@link DeploymentManifestMatcher}. It is expected
	 * that multiple invocations will return different container instances if possible
	 * in a round robin fashion.
	 */
	@Test
	public void testMultipleIterations() {
		DeploymentManifestMatcher matcher = new DeploymentManifestMatcher();
		ModuleDescriptor descriptor = newModuleDescriptor(null, 5);
		ContainerRepository repository = new ContainerRepositoryBuilder().add(20).build();

		Collection<Container> first = matcher.match(descriptor, repository);
		assertEquals(5, first.size());

		Collection<Container> second = matcher.match(descriptor, repository);
		assertEquals(5, second.size());

		for (Container container : first) {
			assertFalse(second.contains(container));
		}
		for (Container container : second) {
			assertFalse(first.contains(container));
		}

		assertEquals(5, matcher.match(descriptor, repository).size());
		assertEquals(5, matcher.match(descriptor, repository).size());

		Collection<Container> last = matcher.match(descriptor, repository);
		assertEquals(5, last.size());
		assertEquals(first, last);
	}

	/**
	 * Create a new {@link ModuleDescriptor} for testing.
	 *
	 * @param group  group for module deployment; may be null
	 * @param count  number of instances requested for a module
	 *
	 * @return new module descriptor
	 */
	protected ModuleDescriptor newModuleDescriptor(String group, int count) {
		Module module = new Module("file", Module.Type.SOURCE, "file:///file");
		return new ModuleDescriptor(module, "file", "file0", 0, group, count);
	}

	/**
	 * Create a new instance of a {@link Container}.
	 *
	 * @param groups comma delimited list of groups this container
	 *               belongs to; may be null
	 *
	 * @return new instance of container
	 */
	protected Container newContainer(String groups) {
		return new Container(UUID.randomUUID().toString(),
				groups == null
						? Collections.<String, String>emptyMap()
						: Collections.singletonMap("groups", groups));
	}

	/**
	 * Builder object for {@link ContainerRepository}.
	 */
	class ContainerRepositoryBuilder {
		/**
		 * Map of group (comma delimited) string to number of container instances.
		 */
		Map<String, Integer> groupCounts = new HashMap<String, Integer>();

		/**
		 * Number of containers with no group.
		 */
		int noGroup;

		/**
		 * Add the provided number of non group containers to the container repository.
		 *
		 * @param count number of containers
		 *
		 * @return this builder
		 */
		public ContainerRepositoryBuilder add(int count) {
			noGroup += count;
			return this;
		}

		/**
		 * Add the provided number of containers that belong to the provided string of
		 * groups.
		 *
		 * @param count   number of containers
		 * @param groups  comma delimited list of groups these containers belong to
		 *
		 * @return this builder
		 */
		public ContainerRepositoryBuilder add(int count, String groups) {
			groupCounts.put(groups, count);
			return this;
		}

		/**
		 * Return a new instance of {@link ContainerRepository}.
		 *
		 * @return new instance of ContainerRepository
		 */
		public ContainerRepository build() {
			final List<Container> containers = new ArrayList<Container>();
			for (int i = 0; i < noGroup; i++) {
				containers.add(newContainer(null));
			}
			for (Map.Entry<String, Integer> entry : groupCounts.entrySet()) {
				for (int i = 0; i < entry.getValue(); i++) {
					containers.add(newContainer(entry.getKey()));
				}
			}

			return new ContainerRepository() {
				@Override
				public Iterator<Container> getContainerIterator() {
					return containers.iterator();
				}
			};
		}

	}

}
