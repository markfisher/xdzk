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

import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * @author Patrick Peralta
 */
public class StreamTest {
	@Test
	public void testSimpleStream() {
		ModuleRepository moduleRepository = new StubModuleRepository();
		StreamFactory streamFactory = new StreamFactory(moduleRepository);
		Stream stream = streamFactory.createStream("ticktock", "time | log", null);

		assertEquals("ticktock", stream.getName());
		Iterator<ModuleDescriptor> iterator = stream.getDeploymentOrderIterator();
		assertTrue(iterator.hasNext());

		String[] moduleNames = {"log", "time"};
		for (String moduleName : moduleNames) {
			ModuleDescriptor module = iterator.next();
			assertEquals(moduleName, module.getModule().getName());
		}

		assertFalse(iterator.hasNext());

		try {
			iterator.next();
			fail("Expected NoSuchElementException");
		}
		catch (NoSuchElementException e) {
			// expected
		}
	}

	@Test
	public void testProcessorStream() {
		ModuleRepository moduleRepository = new StubModuleRepository();
		StreamFactory streamFactory = new StreamFactory(moduleRepository);

		Stream stream = streamFactory.createStream("fancy-http", "http | transform | filter | log", null);

		assertEquals("fancy-http", stream.getName());
		Iterator<ModuleDescriptor> iterator = stream.getDeploymentOrderIterator();
		assertTrue(iterator.hasNext());

		String[] moduleNames = {"log", "filter", "transform", "http"};
		for (String moduleName : moduleNames) {
			ModuleDescriptor module = iterator.next();
			assertEquals(moduleName, module.getModule().getName());
		}

		assertFalse(iterator.hasNext());

		try {
			iterator.next();
			fail("Expected NoSuchElementException");
		}
		catch (NoSuchElementException e) {
			// expected
		}
	}

}
