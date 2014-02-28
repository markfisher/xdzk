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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
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
	 * Source module for this stream. This module is responsible for
	 * obtaining data for this stream.
	 */
	private final Module source;

	/**
	 * Ordered list of processor modules. The data obtained by
	 * the sink module will be fed to these processors in the order
	 * indicated by this list.
	 */
	private final List<Module> processors;

	/**
	 * Sink module for this stream. This is the ultimate destination
	 * for the stream data.
	 */
	private final Module sink;

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
	private Stream(String name, Module source, List<Module> processors,
				Module sink, Map<String, String> properties) {
		this.name = name;
		this.source = source;
		this.processors = new LinkedList<Module>(processors);
		this.sink = sink;
		this.properties = properties;
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
	public Module getSource() {
		return source;
	}

	/**
	 * Return the ordered list of processors for this stream.
	 *
	 * @return list of processors
	 */
	public List<Module> getProcessors() {
		return Collections.unmodifiableList(processors);
	}

	/**
	 * Return the sink for this stream.
	 *
	 * @return sink module
	 */
	public Module getSink() {
		return sink;
	}

	/**
	 * Return an iterator that indicates the order of module deployments
	 * for this stream. The modules are returned in reverse order; i.e.
	 * the sink is returned first followed by the processors in reverse
	 * order followed by the source.
	 *
	 * @return iterator that iterates over the modules in deployment order
	 */
	public Iterator<Module> getDeploymentOrderIterator() {
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
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Stream{" +
				"name='" + name + '\'' +
				", source=" + source +
				", processors=" + processors +
				", sink=" + sink +
				", properties=" + properties +
				'}';
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
	class DeploymentOrderIterator implements Iterator<Module> {
		/**
		 * Iterator state.
		 */
		private IteratorState state;

		/**
		 * Iterator over the processor modules.
		 */
		private final Iterator<Module> processorIterator;

		/**
		 * Construct a DeploymentOrderIterator.
		 */
		@SuppressWarnings("unchecked")
		DeploymentOrderIterator() {
			assert processors instanceof Deque;
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
		public Module next() {
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
		 * Source module.
		 */
		private Module source;

		/**
		 * List of processor modules.
		 */
		private List<Module> processors = new ArrayList<Module>();

		/**
		 * Sink module.
		 */
		private Module sink;

		/**
		 * Stream properties
		 */
		private Map<String, String> properties;

		/**
		 * Return the stream name.
		 *
		 * @return stream name
		 */
		public String getName() {
			return name;
		}

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
		 * @param module module to add
		 *
		 * @return this builder
		 */
		public Builder addModule(Module module) {
			switch (module.getType()) {
				case SOURCE:
					source = module;
					break;
				case PROCESSOR:
					processors.add(module);
					break;
				case SINK:
					sink = module;
					break;
			}

			return this;
		}

		/**
		 * Return the properties for the stream.
		 *
		 * @return stream properties
		 */
		public Map<String, String> getProperties() {
			return properties;
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
			return new Stream(name, source, processors, sink, properties);
		}
	}

}
