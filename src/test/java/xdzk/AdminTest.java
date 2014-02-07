package xdzk;

import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;
import com.oracle.tools.util.CompletionListener;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Admin server functional tests. These tests launch Admin servers on
 * separate JVMs.
 *
 * @author Patrick Peralta
 */
public class AdminTest {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AdminTest.class);

	/**
	 * Start up an Admin server and obtain the list of running containers.
	 *
	 * @throws Exception
	 */
	@Test
	public void simpleAdminTest() throws Exception {
		String classpath = System.getProperty("java.class.path");
		SimpleJavaApplicationSchema schema = new SimpleJavaApplicationSchema(Admin.class.getName(), classpath);
		NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema> builder =
				new NativeJavaApplicationBuilder<>();
		JavaApplication<SimpleJavaApplication> admin = builder.realize(schema, "Admin Server",
				new SystemApplicationConsole());

		// Wait for the application to start; don't have a better way
		// of doing this at the moment.
		Thread.sleep(500);

		Assert.assertNotNull(admin);

		// Once the server has started up, send a remote invocation to obtain
		// the list of container nodes the admin knows of.
		final Set<String> containers = Collections.synchronizedSet(new HashSet<String>());
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Exception> exception = new AtomicReference<>();

		admin.submit(new Admin.CurrentContainers(), new CompletionListener<Collection<String>>() {
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

}
