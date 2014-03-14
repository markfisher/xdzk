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

import org.springframework.util.Assert;

/**
 * A descriptor for module deployment for a specific {@link Stream}.
 * Many attributes of this class are derived from the stream deployment manifest.
 *
 * @author Patrick Peralta
 */
public class ModuleDescriptor {
	/**
	 * The module.
	 */
	private final Module module;

	/**
	 * Name of stream using this module.
	 */
	private final String streamName;

	/**
	 * Label used to uniquely identify this module in the context
	 * of the stream it belongs to.
	 */
	private final String label;

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
	 * A value of 0 indicates that this module should be deployed to
	 * all containers in the {@link #group}. If {@code group} is null,
	 * this module should be deployed to all containers.
	 */
	private final int count;

	/**
	 * Construct a ModuleDescriptor.
	 *
	 * @param module      the module for this descriptor
	 * @param streamName  name of stream using this module
	 * @param label       label for this module as defined by its stream
	 * @param index       order of processing for this module
	 * @param group       container group this module should be deployed to
	 * @param count       number of container instances this module should be deployed to
	 */
	public ModuleDescriptor(Module module, String streamName, String label, int index, String group, int count) {
		this.module = module;
		this.streamName = streamName;
		this.label = label;
		this.index = index;
		this.group = group;
		this.count = count;
	}

	/**
	 * Return the module for this descriptor.
	 *
	 * @return the module
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
	 * Return the label for this module as defined by its stream.
	 *
	 * @return label for this module
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * Return the order of processing for this module. Module 0 indicates
	 * this is a source module, 1 indicates that a source is sending
	 * data to this module, etc.
	 *
	 * @return order of processing for this module
	 */
	public int getIndex() {
		return index;
	}

	/**
	 * Return the group of containers this module should be deployed to.
	 * This may return {@code null}.
	 *
	 * @return container group name
	 */
	public String getGroup() {
		return group;
	}

	/**
	 * Return the number of container instances this module should be deployed to.
	 * A value of 0 indicates that this module should be deployed to
	 * all containers in the {@link #group}. If {@code group} is null,
	 * this module should be deployed to all containers.
	 *
	 * @return number of container instances
	 */
	public int getCount() {
		return count;
	}

	/**
	 * Create a new {@link Key} based on this ModuleDescriptor.
	 *
	 * @return key that can be used to refer to this object
	 */
	public Key newKey() {
		return new Key(module.getType(), label);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return String.format("%s: %s type=%s, group=%s", label, module.getName(), module.getType(), group);
	}


	/**
	 * Key class that can be used to refer to a {@link ModuleDescriptor}.
	 * It can be used as a key in a map, both hash and tree based.
	 */
	public static class Key implements Comparable<Key> {
		/**
		 * Module type.
		 */
		private final Module.Type type;

		/**
		 * Module label.
		 */
		private final String label;

		/**
		 * Construct a key.
		 *
		 * @param type   module type
		 * @param label  module label
		 */
		public Key(Module.Type type, String label) {
			Assert.notNull(type, "Type is required");
			Assert.hasText(label, "Label is required");
			this.type = type;
			this.label = label;
		}

		/**
		 * Return the module type.
		 *
		 * @return module type
		 */
		public Module.Type getType() {
			return type;
		}

		/**
		 * Return the module label.
		 *
		 * @return module label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int compareTo(Key other) {
			int c = type.compareTo(other.getType());
			if (c == 0) {
				c = label.compareTo(other.getLabel());
			}
			return c;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o instanceof Key) {
				Key other = (Key) o;
				return type.equals(other.getType()) && label.equals(other.getLabel());
			}

			return false;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {
			int result = type.hashCode();
			result = 31 * result + label.hashCode();
			return result;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String toString() {
			return "Key{" +
					"type=" + type +
					", label='" + label + '\'' +
					'}';
		}
	}
}
