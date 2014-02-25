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

package xdzk;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.UUID;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to start an embedded instance of standalone (non clustered)
 * ZooKeeper for testing.
 *
 * @author Patrick Peralta
 */
public class ZooKeeperEmbedded {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperEmbedded.class);

	/**
	 * ZooKeeper port.
	 */
	public static final int PORT = 3181;

	/**
	 * Thread for running the ZooKeeper server.
	 */
	private static Thread zkServerThread;

	/**
	 * ZooKeeper server.
	 */
	private static volatile ZooKeeperServerMain zkServer;

	/**
	 * Exception thrown while running the ZooKeeper server. This will be
	 * {@code null} if no exceptions are thrown.
	 */
	private static volatile Exception zkException;

	/**
	 * Runnable implementation that starts the ZooKeeper server.
	 */
	private static class ServerRunnable implements Runnable {
		@Override
		public void run() {
			try {
				Properties properties = new Properties();
				File file = new File(System.getProperty("java.io.tmpdir")
						+ File.pathSeparator + UUID.randomUUID());
				file.deleteOnExit();
				properties.setProperty("dataDir", file.getAbsolutePath());
				properties.setProperty("clientPort", String.valueOf(PORT));

				QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
				quorumPeerConfig.parseProperties(properties);

				zkServer = new ZooKeeperServerMain();
				ServerConfig configuration = new ServerConfig();
				configuration.readFrom(quorumPeerConfig);

				zkServer.runFromConfig(configuration);
			}
			catch (Exception e) {
				LOG.error("Exception running embedded ZooKeeper", e);
				zkException = e;
			}
		}
	}

	/**
	 * Start the ZooKeeper server in a background thread.
	 * <p>
	 * If an exception is thrown during startup or execution, it will be made
	 * available via {@link #getException}.
	 */
	public static synchronized void start() {
		if (zkServerThread == null) {
			zkServerThread = new Thread(new ServerRunnable(), "ZooKeeper Server Starter");
			zkServerThread.setDaemon(true);
			zkServerThread.start();
		}
	}

	/**
	 * Shutdown the ZooKeeper server.
	 */
	public static synchronized void stop() {
		if (zkServerThread != null) {
			// The shutdown method is protected...thus this hack to invoke it.
			// This will log an exception on shutdown; see
			// https://issues.apache.org/jira/browse/ZOOKEEPER-1873 for details.
			try {
				Method shutdown = ZooKeeperServerMain.class.getDeclaredMethod("shutdown");
				shutdown.setAccessible(true);
				shutdown.invoke(zkServer);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}

			// It is expected that the thread will exit after
			// the server is shutdown; this will block until
			// the shutdown is complete.
			try {
				zkServerThread.join(5000);
				zkServerThread = null;
			}
			catch (InterruptedException e) {
				LOG.warn("Interrupted while waiting for embedded ZooKeeper to exit");
				// abandoning zk thread
				zkServerThread = null;
			}
		}
	}

	/**
	 * Return the exception thrown (if any) when starting the ZooKeeper server.
	 *
	 * @return exception thrown when starting ZooKeeper or {@code null} if no
	 *         exception was thrown
	 */
	public static Exception getException() {
		return zkException;
	}

}
