package xdzk;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prototype implementation of an XD Admin server that watches ZooKeeper
 * for Container arrivals and departures from the XD cluster.
 *
 * @author Patrick Peralta
 */
public class Admin extends AbstractServer {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);

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
	 * Singleton instance of the Admin server.
	 */
	// Marked as volatile because this reference is updated by the
	// main thread and is read by the CurrentContainers callable.
	public static volatile Admin INSTANCE;


	/**
	 * Admin constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public Admin(String hostPort) {
		super(hostPort);
	}

	/**
	 * Start the Admin server.
	 *
	 * @throws InterruptedException
	 */
	@Override
	protected void doStart() throws InterruptedException {
		Path.CONTAINERS.verify(this.getClient());
	}

	@Override
	protected void processEvent(WatchedEvent event) {
		super.processEvent(event);
		if (KeeperState.SyncConnected.equals(event.getState())) {
			watchChildren();
		}
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
	 * Asynchronously obtain a list of children under {@code /xd/containers}.
	 *
	 * @see xdzk.Admin.PathCallback
	 */
	protected void watchChildren() {
		getClient().getChildren(Path.CONTAINERS.toString(), pathWatcher, pathCallback, null);
	}

	/**
	 * Watcher implementation that watches the {@code /xd/container}
	 * znode path.
	 */
	class PathWatcher implements Watcher {
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
	class PathCallback implements AsyncCallback.ChildrenCallback {
		/**
		 * {@inheritDoc}
		 * <p>
		 * This callback updates the {@link #containerPaths} set with the latest
		 * known container paths.
		 */
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			LOG.info(">> PathCallback result: {}, {}, {}, {}", rc, path, ctx, children);

			Set<String> arrived = new HashSet<>();
			Set<String> departed = new HashSet<>();
			if (children == null) {
				children = Collections.emptyList();
			}

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
	 * Callable implementation that returns the known container paths.
	 */
	public static class CurrentContainers implements Callable<Collection<String>>, Serializable {
		@Override
		public Collection<String> call() throws Exception {
			return INSTANCE.getContainerPaths();
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
		INSTANCE = new Admin(args.length == 1 ? args[0] : "localhost:2181");
		INSTANCE.run();
	}

}
