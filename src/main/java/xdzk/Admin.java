package xdzk;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
		watchChildren();
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
	 * Start an Admin server. A ZooKeeper host:port may be optionally
	 * passed in as an argument. The default ZooKeeper host/port is
	 * {@code localhost:2181}.
	 *
	 * @param args command line arguments
	 *
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new Admin(args.length == 1 ? args[0] : "localhost:2181").run();
	}

}
