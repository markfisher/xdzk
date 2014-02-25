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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Implementation of the {@link ContainerMatcher} strategy that selects any one of the candidate containers at random.
 *
 * @author Mark Fisher
 */
public class RandomContainerMatcher implements ContainerMatcher {
	private static final Random RANDOM = new Random();

	/**
	 * Randomly selects one of the candidate containers.
	 */
	@Override
	public Container match(String module, Iterator<Container> candidates) {
		List<Container> containers = new ArrayList<>();
		while (candidates.hasNext()) {
			containers.add(candidates.next());
		}
		return containers.isEmpty() ? null : containers.get(RANDOM.nextInt(containers.size()));
	}

}
