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
 * Interface definition for an object with the ability to look up
 * the set of containers available in the cluster.
 *
 * @author Patrick Peralta
 */
public interface ContainerRepository {
	/**
	 * Return an {@link Iterator} over the available containers.
	 *
	 * @return iterator over the available containers
	 */
	Iterator<Container> getContainerIterator();
}
