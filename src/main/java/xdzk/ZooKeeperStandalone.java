package xdzk;

import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;

import java.io.IOException;

/**
 * Utility class to launch a stand alone (non clustered)
 * {@link org.apache.zookeeper.ZooKeeperMain ZooKeeper server}
 * in a separate JVM.
 *
 * @author Patrick Peralta
 */
public class ZooKeeperStandalone {
	/**
	 * ZooKeeper port.
	 */
	public static final int PORT = 3181;

	/**
	 * ZooKeeper Java application.
	 */
	private static JavaApplication<SimpleJavaApplication> application;


	/**
	 * Simple test application (not intended for use in functional tests).
	 *
	 * @param args program arguments
	 *
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		start();

		Thread.sleep(5000);

		stop();
	}

	/**
	 * Launch the {@link org.apache.zookeeper.ZooKeeperMain} class in
	 * a separate JVM.
	 */
	public synchronized static void start() {
		if (application == null) {
			String classpath = System.getProperty("java.class.path");
			SimpleJavaApplicationSchema schema =
					new SimpleJavaApplicationSchema("org.apache.zookeeper.server.ZooKeeperServerMain", classpath)
							.setArgument(String.valueOf(PORT))
							.setArgument(System.getProperty("java.io.tmpdir"));

			NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema> builder =
					new NativeJavaApplicationBuilder<>();

			try {
				application = builder.realize(schema, "ZooKeeper Server", new SystemApplicationConsole());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Shut down the JVM hosting the ZooKeeper server.
	 */
	public synchronized static void stop() {
		if (application != null) {
			application.close();
			application = null;
		}
	}
}
