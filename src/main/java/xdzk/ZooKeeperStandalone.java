package xdzk;

import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;

import javax.net.SocketFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;

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
	 * a separate JVM. The executing thread is blocked until a socket
	 * connection can be established with the ZooKeeper server.
	 *
	 * @throws InterruptedException
	 */
	public synchronized static void start() throws InterruptedException {
		if (application == null) {
			String classpath = System.getProperty("java.class.path");
			SimpleJavaApplicationSchema schema =
					new SimpleJavaApplicationSchema("org.apache.zookeeper.server.ZooKeeperServerMain", classpath)
							.setArgument(String.valueOf(PORT))
							.setArgument(System.getProperty("java.io.tmpdir") + File.pathSeparator + UUID.randomUUID());

			NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema> builder =
					new NativeJavaApplicationBuilder<>();

			try {
				application = builder.realize(schema, "ZooKeeper Server", new SystemApplicationConsole());
				waitForServer();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Block the executing thread until a socket connection can be established
	 * with the ZooKeeper server.
	 *
	 * @throws InterruptedException
	 */
	private static void waitForServer() throws InterruptedException {
		boolean canConnect = false;
		int tries = 0;
		Socket socket = null;

		do {
			try {
				Thread.sleep(100);
				socket = SocketFactory.getDefault().createSocket();
				socket.connect(new InetSocketAddress("localhost", PORT), 5000);
				canConnect = true;
				socket.close();
				socket = null;
			}
			catch (IOException e) {
				// ignore
			}
			finally {
				if (socket != null) {
					try {
						socket.close();
					}
					catch (IOException e) {
						// ignore
					}
				}
			}
		}
		while ((!canConnect) && tries < 100);

		if (!canConnect) {
			throw new IllegalStateException("Cannot connect to ZooKeeper server");
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
