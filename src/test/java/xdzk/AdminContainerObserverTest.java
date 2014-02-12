package xdzk;


import com.oracle.tools.deferred.Eventually;
import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;
import com.oracle.tools.util.CompletionListener;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.oracle.tools.deferred.DeferredHelper.eventually;
import static com.oracle.tools.deferred.DeferredHelper.invoking;
import static org.hamcrest.Matchers.is;

/**
 * Test to assert the ability of {@link xdzk.AdminServer} to observe
 * the number of {@link xdzk.ContainerServer} instances in a cluster.
 *
 * @author Patrick Peralta
 */
public class AdminContainerObserverTest {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AdminContainerObserverTest.class);

	/**
	 * Launch the given class's {@code main} method in a separate JVM.
	 *
	 * @param clz class to launch
	 *
	 * @return launched application
	 *
	 * @throws IOException
	 */
	protected JavaApplication<SimpleJavaApplication> launch(Class clz) throws IOException {
		return launch(clz, null);
	}

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
	protected JavaApplication<SimpleJavaApplication> launch(Class clz, String[] args) throws IOException {
		String classpath = System.getProperty("java.class.path");
		SimpleJavaApplicationSchema schema = new SimpleJavaApplicationSchema(clz.getName(), classpath);
		if (args != null) {
			for (String arg : args) {
				schema.addArgument(arg);
			}
		}
		NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema> builder =
				new NativeJavaApplicationBuilder<>();
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
	 *     <li>Launch an {@link xdzk.AdminServer}</li>
	 *     <li>Launch 3 instances of {@link xdzk.ContainerServer}</li>
	 *     <li>Assert that the admin server sees 3 containers</li>
	 *     <li>Shut down a container</li>
	 *     <li>Assert that the admin server sees 2 containers</li>
	 * </ol>
	 * @throws Exception
	 */
	@Test
	public void test() throws Exception {
		JavaApplication<SimpleJavaApplication> adminServer = null;
		JavaApplication<SimpleJavaApplication> container1 = null;
		JavaApplication<SimpleJavaApplication> container2 = null;
		JavaApplication<SimpleJavaApplication> container3 = null;

		try {
			ZooKeeperStandalone.start();
			adminServer = launch(AdminServer.class);
			container1 = launch(ContainerServer.class);
			container2 = launch(ContainerServer.class);
			container3 = launch(ContainerServer.class);

			Eventually.assertThat(eventually(invoking(this).getCurrentContainers(adminServer).size()), is(3));

			container3.close();
			container3 = null;

			Eventually.assertThat(eventually(invoking(this).getCurrentContainers(adminServer).size()), is(2));
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
			ZooKeeperStandalone.stop();
		}
	}

}
