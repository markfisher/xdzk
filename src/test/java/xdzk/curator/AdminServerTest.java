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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;
import com.oracle.tools.util.CompletionListener;

/**
 * Admin server functional tests. These tests launch Admin servers on
 * separate JVMs.
 *
 * @author Patrick Peralta
 */
public class AdminServerTest {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AdminServerTest.class);

	/**
	 * Instance of {@link AdminServer} used for testing.
	 *
	 * @see #startServer
	 * @see #stopServer
	 */
	private static JavaApplication<SimpleJavaApplication> adminServer;

	/**
	 * Start an {@link AdminServer} used for testing.
	 *
	 * @throws Exception
	 */
	@BeforeClass
	public static void startServer() throws Exception {
		String classpath = System.getProperty("java.class.path");
		SimpleJavaApplicationSchema schema = new SimpleJavaApplicationSchema(AdminServer.class.getName(), classpath);
		NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema> builder =
				new NativeJavaApplicationBuilder<>();
		adminServer = builder.realize(schema, "Admin Server", new SystemApplicationConsole());

		// Wait for the application to start; don't have a better way
		// of doing this at the moment.
		Thread.sleep(500);

		Assert.assertNotNull(adminServer);
	}

	/**
	 * Stop the {@link AdminServer} after testing is complete.
	 */
	@AfterClass
	public static void stopServer() {
		adminServer.close();
	}

	/**
	 * Obtain the list of running containers.
	 *
	 * @throws Exception
	 */
	@Test
	public void containerListTest() throws Exception {
		// Once the server has started up, send a remote invocation to obtain
		// the list of container nodes the admin knows of.
		final Set<String> containers = Collections.synchronizedSet(new HashSet<String>());
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Exception> exception = new AtomicReference<>();

		adminServer.submit(new AdminServer.CurrentContainers(), new CompletionListener<Collection<String>>() {
			@Override
			public void onCompletion(Collection<String> result) {
				containers.addAll(result);
				latch.countDown();
			}

			@Override
			public void onException(Exception e) {
				exception.set(e);
				latch.countDown();
			}
		});

		if (latch.await(10, TimeUnit.SECONDS)) {
			if (exception.get() != null) {
				throw exception.get();
			}
			// todo: this is where we would assert a set of containers
			LOG.info("Found the following running containers: {}", containers);
		}
		else {
			Assert.fail("Time out while waiting for method completion");
		}
	}

	/**
	 * Test stream deployment.
	 *
	 * @throws Exception
	 */
	@Test
	public void testStreamDeploymentRequest() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Exception> exception = new AtomicReference<>();

		adminServer.submit(new AdminServer.StreamDeploymentRequest("ticktock", "time|log"), new CompletionListener<Void>() {
			@Override
			public void onCompletion(Void result) {
				latch.countDown();
			}

			@Override
			public void onException(Exception e) {
				exception.set(e);
				latch.countDown();
			}
		});

		if (latch.await(10, TimeUnit.SECONDS)) {
			if (exception.get() != null) {
				throw exception.get();
			}
			// todo: assert that the deployment request was written to zk?
			LOG.info("Deployment request sent");
		}
		else {
			Assert.fail("Time out while waiting for method completion");
		}
	}

}
