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

package xdzk.admin;

import static com.oracle.tools.deferred.DeferredHelper.eventually;
import static com.oracle.tools.deferred.DeferredHelper.invoking;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oracle.tools.deferred.Eventually;
import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;
import com.oracle.tools.util.CompletionListener;
import xdzk.server.AdminApplication;
import xdzk.server.AdminServer;
import xdzk.server.ContainerApplication;

/**
 * Test to assert the ability of {@link xdzk.server.AdminServer} to observe
 * the number of {@link xdzk.server.ContainerServer} instances in a cluster.
 *
 * @author Patrick Peralta
 */
public class AdminContainerObserverTest {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AdminContainerObserverTest.class);

	/**
	 * ZooKeeper port.
	 */
	public static final int PORT = 3181;

	/**
	 * Launch the given class's {@code main} method in a separate JVM.
	 *
	 * @param clz class to launch
	 * @param args command line arguments
	 *
	 * @return launched application
	 *
	 * @throws IOException
	 */
	protected JavaApplication<SimpleJavaApplication> launch(Class<?> clz, boolean remoteDebug, String... args) throws IOException {
		String classpath = System.getProperty("java.class.path");
		SimpleJavaApplicationSchema schema = new SimpleJavaApplicationSchema(clz.getName(), classpath);
		if (args != null) {
			for (String arg : args) {
				schema.addArgument(arg);
			}
		}
		NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema> builder =
				new NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema>();
		builder.setRemoteDebuggingEnabled(remoteDebug);
		return builder.realize(schema, clz.getName(), new SystemApplicationConsole());
	}

	/**
	 * Return the set of containers known to the given admin server.
	 *
	 * @param adminServer admin server application
	 *
	 * @return set of containers known to the admin server
	 *
	 * @throws Exception
	 */
	public Set<String> getCurrentContainers(JavaApplication<SimpleJavaApplication> adminServer) throws Exception {
		final Set<String> containers = Collections.synchronizedSet(new HashSet<String>());
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Exception> exception = new AtomicReference<Exception>();

		adminServer.submit(new AdminApplication.CurrentContainers(), new CompletionListener<Collection<String>>() {
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
			return containers;
		}
		else {
			throw new TimeoutException();
		}
	}

	/**
	 * Execute the following sequence:
	 * <ol>
	 *     <li>Launch a ZooKeeper standalone server</li>
	 *     <li>Launch an {@link AdminServer}</li>
	 *     <li>Launch 3 instances of {@link xdzk.server.ContainerServer}</li>
	 *     <li>Assert that the admin server sees 3 containers</li>
	 *     <li>Shut down a container</li>
	 *     <li>Assert that the admin server sees 2 containers</li>
	 * </ol>
	 * @throws Exception
	 */
	@Test
	public void test() throws Exception {
		TestingServer zooKeeper = new TestingServer(new InstanceSpec(
				null,  // dataDirectory
				PORT,  // port
				-1,    // electionPort
				-1,    // quorumPort
				true,  // deleteDataDirectoryOnClose
				-1,    // serverId
				500,   // tickTime
				-1));  // maxClientCnxns
		JavaApplication<SimpleJavaApplication> adminServer = null;
		JavaApplication<SimpleJavaApplication> container1 = null;
		JavaApplication<SimpleJavaApplication> container2 = null;
		JavaApplication<SimpleJavaApplication> container3 = null;

		try {
			String zkAddress = "localhost:" + PORT;
			// The Admin server is set up with remote debugging enabled.
			// By default it will be port 30001, but this port can be
			// obtained via the log file output. For this to be useful,
			// a break point should be set in here to pause execution of
			// the test while another debug session is launched to attach
			// to the admin server.
			adminServer = launch(AdminApplication.class, true, "--zk=" + zkAddress);
			container1 = launch(ContainerApplication.class, false, "--zk=" + zkAddress);
			container2 = launch(ContainerApplication.class, false, "--zk=" + zkAddress);
			container3 = launch(ContainerApplication.class, false, "--zk=" + zkAddress);

			Eventually.assertThat(eventually(invoking(this).getCurrentContainers(adminServer).size()), is(3),
					90, TimeUnit.SECONDS);

			container3.close();
			container3 = null;

			Eventually.assertThat(eventually(invoking(this).getCurrentContainers(adminServer).size()), is(2),
					60, TimeUnit.SECONDS);
		}

		finally {
			if (adminServer != null) {
				adminServer.close();
			}
			if (container1 != null) {
				container1.close();
			}
			if (container2 != null) {
				container2.close();
			}
			if (container3 != null) {
				container3.close();
			}
			zooKeeper.stop();
		}
	}

}
