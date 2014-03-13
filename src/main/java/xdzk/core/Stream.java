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
import xdzk.cluster.ContainerMatcher;
import xdzk.cluster.DeploymentManifestMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Domain model for an XD Stream. A stream consists of a set of modules
 * used to process the flow of data.
 *
 * @author Patrick Peralta
 */
public class Stream {
	/**
	 * Name of stream.
	 */
	private final String name;

	/**
	 * Stream definition.
	 */
	private final String definition;

	/**
	 * Source module for this stream. This module is responsible for
	 * obtaining data for this stream.
	 */
	private final ModuleDescriptor source;

	/**
	 * Ordered list of processor modules. The data obtained by
	 * the source module will be fed to these processors in the order
	 * indicated by this list.
	 */
	private final List<ModuleDescriptor> processors;

	/**
	 * Sink module for this stream. This is the ultimate destination
	 * for the stream data.
	 */
	private final ModuleDescriptor sink;

	/**
	 * Container matcher for this stream. This is used by the stream
	 * to indicate which container(s) should deploy the various
	 * modules in the stream.
	 */
	private final ContainerMatcher containerMatcher;

	/**
	 * Properties for this stream.
	 */
	private final Map<String, String> properties;

	/**
	 * Construct a Stream.
	 *
	 * @param name        stream name
	 * @param source      source module
	 * @param processors  processor modules
	 * @param sink        sink module
	 * @param properties  stream properties
	 */
	private Stream(String name, ModuleDescriptor source, List<ModuleDescriptor> processors,
				ModuleDescriptor sink, ContainerMatcher containerMatcher, Map<String, String> properties) {
		this.name = name;
		this.source = source;
		this.processors = new LinkedList<ModuleDescriptor>(processors == null
				? Collections.<ModuleDescriptor>emptyList()
				: processors);
		this.sink = sink;
		this.containerMatcher = containerMatcher;
		this.properties = properties;
		this.definition = properties.get("definition");
		Assert.hasText(this.definition, "Stream properties require a 'definition' value");
	}

	/**
	 * Return the name of this stream.
	 *
	 * @return stream name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Return the source module for this stream.
	 *
	 * @return source module
	 */
	public ModuleDescriptor getSource() {
		return source;
	}

	/**
	 * Return the ordered list of processors for this stream.
	 *
	 * @return list of processors
	 */
	public List<ModuleDescriptor> getProcessors() {
		return Collections.unmodifiableList(processors);
	}

	/**
	 * Return the sink for this stream.
	 *
	 * @return sink module
	 */
	public ModuleDescriptor getSink() {
		return sink;
	}

	/**
	 * Return the container matcher for this stream.
	 *
	 * @return container matcher
	 */
	public ContainerMatcher getContainerMatcher() {
		return containerMatcher;
	}

	/**
	 * Return an iterator that indicates the order of module deployments
	 * for this stream. The modules are returned in reverse order; i.e.
	 * the sink is returned first followed by the processors in reverse
	 * order followed by the source.
	 *
	 * @return iterator that iterates over the modules in deployment order
	 */
	public Iterator<ModuleDescriptor> getDeploymentOrderIterator() {
		return new DeploymentOrderIterator();
	}

	/**
	 * Return the properties for this stream.
	 *
	 * @return stream properties
	 */
	public Map<String, String> getProperties() {
		return properties;
	}

	/**
	 * Return the module descriptor for the given module label and type.
	 *
	 * @param moduleLabel  module label
	 * @param moduleType  module type
	 *
	 * @return module descriptor
	 *
	 * @throws IllegalArgumentException if the module name/type cannot be found
	 */
	public ModuleDescriptor getModuleDescriptor(String moduleLabel, String moduleType) {
		Module.Type type = Module.Type.valueOf(moduleType.toUpperCase());
		ModuleDescriptor moduleDescriptor = null;
		switch (type) {
			case SOURCE:
				moduleDescriptor = getSource();
				break;
			case SINK:
				moduleDescriptor = getSink();
				break;
			case PROCESSOR:
				for (ModuleDescriptor processor : processors) {
					if (processor.getLabel().equals(moduleLabel)) {
						moduleDescriptor = processor;
						break;
					}
				}
		}
		if (moduleDescriptor == null || !moduleLabel.equals(moduleDescriptor.getLabel())) {
			throw new IllegalArgumentException(String.format("Module %s of type %s not found", moduleLabel, moduleType));
		}

		return moduleDescriptor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Stream{" +
				"name='" + name + '\'' +
				", definition='" + definition +
				"'}";
	}

	/**
	 * Enumeration indicating the state of {@link DeploymentOrderIterator}.
	 */
	enum IteratorState {
		/**
		 * Indicates the iterator has not been used yet.
		 */
		INITIAL,

		/**
		 * Indicates that the first item in the iterator
		 * has been returned (the sink module), thus causing the activation
		 * of the processor module iterator.
		 */
		PROC_ITERATOR_ACTIVE,

		/**
		 * Indicates that the last item (the source module)
		 * has been iterated over. Any subsequent iteration
		 * will result in {@link java.util.NoSuchElementException}
		 */
		LAST_ITERATED
	}

	/**
	 * Iterator that returns the modules in the order in which
	 * they should be deployed. The sink module is returned
	 * first, followed by processor modules in reverse order,
	 * followed by the source module.
	 */
	class DeploymentOrderIterator implements Iterator<ModuleDescriptor> {
		/**
		 * Iterator state.
		 */
		private IteratorState state;

		/**
		 * Iterator over the processor modules.
		 */
		private final Iterator<ModuleDescriptor> processorIterator;

		/**
		 * Construct a DeploymentOrderIterator.
		 */
		@SuppressWarnings("unchecked")
		DeploymentOrderIterator() {
			Assert.isInstanceOf(Deque.class, processors);
			processorIterator = ((Deque) processors).descendingIterator();
			state = IteratorState.INITIAL;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean hasNext() {
			return state != IteratorState.LAST_ITERATED;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public ModuleDescriptor next() {
			switch(state) {
				case INITIAL:
					state = IteratorState.PROC_ITERATOR_ACTIVE;
					return sink;
				case PROC_ITERATOR_ACTIVE:
					if (processorIterator.hasNext()) {
						return processorIterator.next();
					}
					else {
						state = IteratorState.LAST_ITERATED;
						return source;
					}
				case LAST_ITERATED:
					throw new NoSuchElementException();
				default:
					throw new IllegalStateException();
			}
		}

		/**
		 * {@inheritDoc}
		 * <p/>
		 * This implementation throws {@link java.lang.UnsupportedOperationException}.
		 */
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Builder object for {@link Stream} that supports fluent style configuration.
	 */
	public static class Builder {
		/**
		 * Stream name.
		 */
		private String name;

		/**
		 * Map of module labels to modules.
		 */
		private Map<String, Module> modules = new LinkedHashMap<String, Module>();

		/**
		 * Container matcher. Defaults to {@link DeploymentManifestMatcher}.
		 */
		private ContainerMatcher containerMatcher = new DeploymentManifestMatcher();

		/**
		 * Stream properties
		 */
		private Map<String, String> properties = Collections.emptyMap();

		/**
		 * Set the stream name.
		 *
		 * @param name stream name
		 *
		 * @return this builder
		 */
		public Builder setName(String name) {
			this.name = name;
			return this;
		}

		/**
		 * Add a module to this stream builder. Processor
		 * modules will be added to the stream in the order
		 * they are added to this builder.
		 *
		 * @param module  module to add
		 * @param label   label for this module
		 * @return this builder
		 */
		public Builder addModule(Module module, String label) {
			if (modules.containsKey(label)) {
				throw new IllegalArgumentException(String.format("Label %s already in use", label));
			}
			modules.put(label, module);
			return this;
		}

		/**
		 * Set the properties for the stream.
		 *
		 * @param properties stream properties
		 *
		 * @return this builder
		 */
		public Builder setProperties(Map<String, String> properties) {
			this.properties = properties;
			return this;
		}

		/**
		 * Create a new instance of {@link Stream}.
		 *
		 * @return new Stream instance
		 */
		public Stream build() {
			ModuleDescriptor sourceDescriptor = null;
			ModuleDescriptor sinkDescriptor = null;
			List<ModuleDescriptor> processorDescriptors = new ArrayList<ModuleDescriptor>();
			int i = 0;

			for (Map.Entry<String, Module> entry : modules.entrySet()) {
				String label = entry.getKey();
				Module module = entry.getValue();
				String group = properties.get(String.format("module.%s.group", module.getName()));
				int count = convert(properties.get(String.format("module.%s.count", module.getName())));

				ModuleDescriptor descriptor = new ModuleDescriptor(module, name, label, i++, group, count);
				switch (module.getType()) {
					case SOURCE:
						sourceDescriptor = descriptor;
						break;
					case PROCESSOR:
						processorDescriptors.add(descriptor);
						break;
					case SINK:
						sinkDescriptor = descriptor;
				}
			}

			Assert.notNull(sourceDescriptor);
			Assert.notNull(sinkDescriptor);

			// TODO: if the manifest includes a container matcher, an instance should
			// be obtained and passed along to the constructor. Perhaps this can
			// be loaded from an application context?
			//String matcher = properties.get("matcher");

			return new Stream(name, sourceDescriptor, processorDescriptors, sinkDescriptor, containerMatcher, properties);
		}

		/**
		 * Convert a String value to an int. Returns 1 if the String is null.
		 *
		 * @param s string to convert
		 *
		 * @return int value of String, or 1 if null
		 */
		private int convert(String s) {
			return s == null ? 1 : Integer.valueOf(s);
		}
	}

}
