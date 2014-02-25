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

package xdzk.curator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Domain object for an XD container. This object is typically constructed
 * from container data maintained in ZooKeeper.
 *
 * @author Patrick Peralta
 */
public class Container {
	/**
	 * Container name.
	 */
	private final String name;

	/**
	 * Container attributes.
	 */
	private final Map<String, String> attributes;

	/**
	 * Construct a Container object.
	 *
	 * @param name        container name
	 * @param attributes  container attributes
	 */
	public Container(String name, Map<String, String> attributes) {
		this.name = name;
		this.attributes = Collections.unmodifiableMap(new HashMap<String, String>(attributes));
	}

	/**
	 * Return the container name.
	 *
	 * @return container name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Return the container attributes.
	 *
	 * @return read-only map of container attributes
	 */
	public Map<String, String> getAttributes() {
		return attributes;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Container{" +
				"name='" + name + '\'' +
				", attributes=" + attributes +
				'}';
	}
}
