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

package xdzk.core;

/**
 * A descriptor for module deployment for a specific {@link Stream}.
 * Many attributes of this class are derived from the stream deployment manifest.
 *
 * @author Patrick Peralta
 */
public class ModuleDescriptor {
	/**
	 * Module type.
	 */
	private final Module module;

	/**
	 * Name of stream using this module.
	 */
	private final String streamName;

	/**
	 * Order of processing for this module. Only applies to modules of type
	 * {@link xdzk.core.Module.Type#PROCESSOR}.
	 */
	private final int index;

	/**
	 * Group of containers this module should be deployed to.
	 */
	private final String group;

	/**
	 * Number of container instances this module should be deployed to.
	 */
	private final int count;

	/**
	 * Construct a ModuleDescriptor.
	 *
	 * @param module      module type
	 * @param streamName  name of stream using this module
	 * @param index       order of processing for this module
	 * @param group       container group this module should be deployed to
	 * @param count       number of container instances this module should be deployed to
	 */
	public ModuleDescriptor(Module module, String streamName, int index, String group, int count) {
		this.module = module;
		this.streamName = streamName;
		this.index = index;
		this.group = group;
		this.count = count;
	}

	/**
	 * Return the module type.
	 *
	 * @return module type
	 */
	public Module getModule() {
		return module;
	}

	/**
	 * Return the name of the stream using this module.
	 *
	 * @return stream name
	 */
	public String getStreamName() {
		return streamName;
	}

	/**
	 * Return the order of processing for this module. Only applies to modules of type
	 * {@link xdzk.core.Module.Type#PROCESSOR}.
	 *
	 * @return order of processing for this module
	 */

	public int getIndex() {
		return index;
	}

	/**
	 * Return the group of containers this module should be deployed to.
	 *
	 * @return container group name
	 */
	public String getGroup() {
		return group;
	}

	/**
	 * Return the number of container instances this module should be deployed to.
	 *
	 * @return number of container instances
	 */
	public int getCount() {
		return count;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return module.getType() + "/" + module.getName();
	}
}
