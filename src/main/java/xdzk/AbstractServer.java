package xdzk;

import java.util.UUID;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common base class for servers that need to coordinate via ZooKeeper.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 */
public abstract class AbstractServer implements Runnable {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);

	/**
	 * Unique id string for this Server.
	 */
	private final String id;

	/**
	 * ZooKeeper client.
	 */
	// Marked as volatile because this is assigned by the thread that
	// invokes public method start and is read by the ZK event
	// dispatch thread.
	private volatile ZooKeeper zk;

	/**
	 * String containing the host name and port of the ZooKeeper
	 * server in the format {@code host:port}.
	 */
	private final String hostPort;

	/**
	 * Watcher instance for the ZooKeeper client to notify
	 * of connections, disconnections, etc.
	 */
	// todo: need to check if this watcher needs to be
	// re-registered after every event
	private final ZooKeeperWatcher zkWatcher = new ZooKeeperWatcher();

	/**
	 * Server constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	AbstractServer(String hostPort) {
		this.hostPort = hostPort;
		this.id = UUID.randomUUID().toString();
	}

	/**
	 * Provide subclasses with access to the unique ID.
	 *
	 * @return this Server's unique ID.
	 */
	protected String getId() {
		return this.id;
	}

	/**
	 * Start the server.
	 *
	 * @throws InterruptedException
	 */
	public void start() throws InterruptedException {
		try {
			this.zk = new ZooKeeper(hostPort, 15000, zkWatcher);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Method that concrete Server subclasses can override for processing watch connect events.
	 * This default implementation simply logs each event at info level.
	 *
	 * @param event the watched event to be processed
	 */
	protected void onConnect(WatchedEvent event) {
		LOG.info(">>> ZooKeeperWatcher connected event: {}", event);
	}

	/**
	 * Method that concrete Server subclasses can override for processing watch disconnected events.
	 * This default implementation simply logs each event at info level.
	 *
	 * @param event the watched event to be processed
	 */
	protected void onDisconnect(WatchedEvent event) {
		LOG.info(">>> ZooKeeperWatcher disconnected event: {}", event);
	}

	/**
	 * Stops the server.
	 *
	 * @throws InterruptedException
	 */
	public void stop() throws InterruptedException {
		zk.close();
	}

	/**
	 * Provides subclasses access to the ZooKeeper client instance.
	 *
	 * @return the ZooKeeper client instance
	 */
	protected ZooKeeper getClient() {
		return this.zk;
	}

	/**
	 * Watcher implementation for the ZooKeeper client to notify
	 * of connections, disconnections, etc.
	 */
	class ZooKeeperWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			if (KeeperState.SyncConnected.equals(event.getState())) {
				onConnect(event);
			}
			else if (KeeperState.Disconnected.equals(event.getState())) {
				LOG.info("Server {} DISCONNECTED", getId());
				try {
					getClient().close();
					start();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				onDisconnect(event);
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
