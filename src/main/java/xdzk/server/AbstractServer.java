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

package xdzk.server;

import java.util.UUID;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xdzk.curator.Paths;

/**
 * Common base class for servers that need to coordinate via ZooKeeper.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 */
public class AbstractServer implements Runnable {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);

	/**
	 * Unique id string for this Server.
	 */
	private final String id;

	/**
	 * Curator client retry policy.
	 *
	 * todo: make this pluggable
	 */
	private final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

	/**
	 * Curator client.
	 */
	private final CuratorFramework client;

	/**
	 * Connection listener for Curator client.
	 */
	private final ConnectionListener connectionListener = new ConnectionListener();

	/**
	 * Server constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	AbstractServer(String hostPort) {
		id = UUID.randomUUID().toString();
		client = CuratorFrameworkFactory.builder()
				.namespace(Paths.XD_NAMESPACE)
				.retryPolicy(retryPolicy)
				.connectString(hostPort).build();
		client.getConnectionStateListenable().addListener(connectionListener);
	}

	/**
	 * Provide subclasses with access to the unique ID.
	 *
	 * @return this Server's unique ID.
	 */
	protected String getId() {
		return this.id;
	}

	protected CuratorFramework getClient() {
		return client;
	}

	/**
	 * Start the server.
	 */
	public void start() {
		client.start();
	}

	/**
	 * Method that concrete Server subclasses can override for processing watch connect events.
	 * This default implementation simply logs each event at info level.
	 *
	 * @param newState the watched event to be processed
	 */
	protected void onConnect(ConnectionState newState) {
		LOG.info(">>> Curator connected event: {}", newState);
	}

	/**
	 * Method that concrete Server subclasses can override for processing watch disconnected events.
	 * This default implementation simply logs each event at info level.
	 *
	 * @param newState the watched event to be processed
	 */
	protected void onDisconnect(ConnectionState newState) {
		LOG.info(">>> Curator disconnected event: {}", newState);
	}

	class ConnectionListener implements ConnectionStateListener {

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			switch (newState) {
				case CONNECTED:
				case RECONNECTED:
					onConnect(newState);
					break;
				case LOST:
				case SUSPENDED:
					onDisconnect(newState);
					break;
				case READ_ONLY:
					// todo: ???
			}
		}
	}

	/**
	 * Start the server and sleep until interrupted.
	 */
	@Override
	public void run() {
		try {
			this.start();
			Thread.sleep(Integer.MAX_VALUE);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
