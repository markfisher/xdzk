package xdzk;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Prototype implementation of an XD Admin server that watches ZooKeeper
 * for Container arrivals and departures from the XD cluster.
 *
 * @author Patrick Peralta
 */
public class Admin {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(Admin.class);

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
	 * Watcher instance that watches the {@code /xd/container}
	 * znode path.
	 */
	private final PathWatcher pathWatcher = new PathWatcher();

	/**
	 * Callback instance that is invoked upon invocation
	 * of {@link ZooKeeper#getChildren}.
	 */
	private final PathCallback pathCallback = new PathCallback();

	/**
	 * Set of current container paths under {@code /xd/container}.
	 */
	// Marked as volatile because this reference is updated by the
	// ZK event dispatch thread and is read by public method getContainerPaths
	private volatile Set<String> containerPaths = Collections.emptySet();


	/**
	 * Admin constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public Admin(String hostPort) {
		this.hostPort = hostPort;
	}

	/**
	 * Start the Admin server.
	 *
	 * @throws InterruptedException
	 */
	public void start() throws InterruptedException {
		try {
			zk = new ZooKeeper(hostPort, 15000, zkWatcher);
			ensureContainerPath();
			watchChildren();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Stops the Admin server.
	 *
	 * @throws InterruptedException
	 */
	public void stop() throws InterruptedException {
		zk.close();
	}

	/**
	 * Obtain the set of current container paths.
	 *
	 * @return read-only set of container paths
	 */
	public Set<String> getContainerPaths() {
		return containerPaths;
	}

	/**
	 * Ensure that the {@code /xd/containers} znode exists.
	 *
	 * @throws InterruptedException
	 */
	protected void ensureContainerPath() throws InterruptedException {
		try {
			if (zk.exists("/xd/containers", false) == null) {
				if (zk.exists("/xd", false) == null) {
					zk.create("/xd", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				zk.create("/xd/containers", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
		catch (KeeperException.NodeExistsException e) {
			// Assume this means that another member of the cluster
			// is creating the /xd/containers path
		}
		catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Asynchronously obtain a list of children under {@code /xd/containers}.
	 *
	 * @see xdzk.Admin.PathCallback
	 */
	protected void watchChildren() {
		zk.getChildren("/xd/containers", pathWatcher, pathCallback, null);
	}


	/**
	 * Watcher implementation that watches the {@code /xd/container}
	 * znode path.
	 */
	class PathWatcher
			implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			LOG.info(">> PathWatcher event: {}", event);
			watchChildren();
		}
	}

	/**
	 * Callback implementation that is invoked upon invocation
	 * of {@link ZooKeeper#getChildren}.
	 */
	class PathCallback
			implements AsyncCallback.ChildrenCallback {
		/**
		 * {@inheritDoc}
		 * <p>
		 * This callback updates the {@link #containerPaths} set with the latest
		 * known container paths.
		 */
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			LOG.info(">> PathCallback result: {}, {}, {}, {}", new Object[]{rc, path, ctx, children});

			Set<String> arrived = new HashSet<>();
			Set<String> departed = new HashSet<>();

			for (String child : children) {
				if (!containerPaths.contains(child)) {
					arrived.add(child);
				}
			}

			Set<String> newPaths = Collections.unmodifiableSet(new HashSet<>(children));
			for (String child : containerPaths) {
				if (!newPaths.contains(child)) {
					departed.add(child);
				}
			}

			containerPaths = newPaths;

			// todo: consider a pluggable listener for new and departed containers

			LOG.info("New containers:      {}", arrived);
			LOG.info("Departed containers: {}", departed);
			LOG.info("All containers:      {}", containerPaths);
		}
	}

	/**
	 * Watcher implementation for the ZooKeeper client to notify
	 * of connections, disconnections, etc.
	 */
	class ZooKeeperWatcher
			implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			LOG.info(">>> ZooKeeperWatcher event: {}", event);
		}
	}

	/**
	 * Start an Admin server. A ZooKeeper host:port may be optionally
	 * passed in as an argument. The default ZooKeeper host/port is
	 * {@code localhost:2181}.
	 *
	 * @param args command line arguments
	 *
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Admin admin = new Admin(args.length == 1 ? args[0] : "localhost:2181");

		admin.start();

		Thread.sleep(Integer.MAX_VALUE);
	}
}
