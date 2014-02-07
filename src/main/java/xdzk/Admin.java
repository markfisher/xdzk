package xdzk;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prototype implementation of an XD Admin server that watches ZooKeeper
 * for Container arrivals and departures from the XD cluster. Each Admin
 * instance will attempt to request leadership, but at any given time only
 * one Admin instance in the cluster will have leadership status. Those
 * instances not elected will watch the {@code /xd/admin} znode so that one
 * of them will take over leadership if the leader Admin closes or crashes.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 */
public class Admin extends AbstractServer {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);

	enum LeadershipStatus {
		REQUESTING,
		ELECTED,
		NOT_ELECTED
	}

	/**
	 * Current status of this Admin server instance.
	 */
	private volatile LeadershipStatus status;

	/**
	 * Watcher instance that watches the {@code /xd/admin} znode path.
	 */
	private final AdminPathWatcher adminPathWatcher = new AdminPathWatcher();

	/**
	 * Callback instance that is invoked to process the result of an attempt
	 * to write the {@code /xd/admin} znode, i.e. a request to assume leadership.
	 */
	private final LeadershipRequestCallback leadershipRequestCallback = new LeadershipRequestCallback();

	/**
	 * Callback instance that is invoked to process the result of
	 * getting data from the {@code /xd/admin} znode.
	 */
	private final LeadershipCheckCallback leadershipCheckCallback = new LeadershipCheckCallback();

	/**
	 * Callback instance that is invoked to process the result of
	 * checking the existence of the {@code /xd/admin} znode.
	 */
	private final LeaderExistsCallback leaderExistsCallback = new LeaderExistsCallback();

	/**
	 * Watcher instance that watches the {@code /xd/container} znode path.
	 */
	private final ContainerPathWatcher containerPathWatcher = new ContainerPathWatcher();

	/**
	 * Callback instance that is invoked to process the result of
	 * {@link ZooKeeper#getChildren} on the {@code /xd/container} znode.
	 */
	private final ContainerPathCallback containerPathCallback = new ContainerPathCallback();

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
	 * Obtain the set of current container paths.
	 *
	 * @return read-only set of container paths
	 */
	public Set<String> getContainerPaths() {
		return containerPaths;
	}

	/**
	 * Start the Admin server.
	 *
	 * @throws InterruptedException
	 */
	@Override
	protected void doStart() throws InterruptedException {
		ZooKeeper zk = this.getClient();
		Path.CONTAINERS.verify(zk);
	}

	/**
	 * Processes WatchedEvents from the ZooKeeper client. Specifically,
	 * reacts to connection events by requesting leadership and
	 * establishing a watch for container nodes.
	 */
	@Override
	protected void processEvent(WatchedEvent event) {
		super.processEvent(event);
		if (KeeperState.SyncConnected.equals(event.getState())) {
			requestLeadership();
			watchContainers();
		}
	}

	/**
	 * Asynchronously request leadership by attempting to write to {@code /xd/admin}.
	 *
	 * @see xdzk.Admin.LeadershipRequestCallback
	 */
	private void requestLeadership() {
		getClient().create(Path.ADMIN.toString(), this.getId().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, leadershipRequestCallback, null);
	}

	/**
	 * Asynchronously obtain a list of children under {@code /xd/containers}.
	 *
	 * @see xdzk.Admin.ContainerPathCallback
	 */
	protected void watchContainers() {
		getClient().getChildren(Path.CONTAINERS.toString(), containerPathWatcher, containerPathCallback, null);
	}

	/**
	 * Callback method that is invoked when the current Admin server instance
	 * has been elected to take leadership.
	 */
	private void takeLeadership() {
		LOG.info("I AM THE LEADER!");
	}

	/**
	 * Asynchronously attempt to get data from the {@code /xd/admin} znode.
	 *
	 * @see xdzk.Admin.LeadershipCheckCallback
	 */
	private void checkLeadership() {
		getClient().getData(Path.ADMIN.toString(), false, leadershipCheckCallback, null);
	}

	/**
	 * Asynchronously check for the existence of the {@code /xd/admin} znode.
	 *
	 * @see xdzk.Admin.AdminPathWatcher
	 * @see xdzk.Admin.LeaderExistsCallback
	 */
	private void leaderExists() {
		getClient().exists(Path.ADMIN.toString(), adminPathWatcher, leaderExistsCallback, null);
	}

	/**
	 * Callback implementation that is invoked upon reading the {@code /xd/admin} znode.
	 */
	class LeadershipCheckCallback implements AsyncCallback.DataCallback {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkLeadership();
				return;
			case NONODE:
				requestLeadership();
				return;
			default:
				break;
			}
		}
	}

	/**
	 * Watcher implementation that watches the {@code /xd/admin} znode path.
	 */
	class AdminPathWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			if (EventType.NodeDeleted == event.getType()
					&& Path.ADMIN.toString().equals(event.getPath())) {
				requestLeadership();
			}
		}
	};

	/**
	 * Callback implementation that is invoked upon writing to the {@code /xd/admin} znode.
	 */
	class LeadershipRequestCallback implements AsyncCallback.StringCallback {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			LOG.info(">> LeaderCallback result: {}, {}, {}, {}", rc, path, ctx, name);
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkLeadership();
				return;
			case OK:
				status = LeadershipStatus.ELECTED;
				takeLeadership();
				break;
			case NODEEXISTS:
				status = LeadershipStatus.NOT_ELECTED;
				leaderExists();
				break;
			default:
				status = LeadershipStatus.NOT_ELECTED;
				LOG.error("Something went wrong requesting leadership.",
						KeeperException.create(Code.get(rc), path));
			}
			LOG.info("Admin {} status: {}", getId(), status);
		}
	}

	/**
	 * Callback implementation that is invoked when checking the
	 * existence of the {@code /xd/admin} znode.
	 */
	class LeaderExistsCallback implements AsyncCallback.StatCallback {

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				leaderExists();
				break;
			case OK:
				if (stat == null) {
					status = LeadershipStatus.REQUESTING;
					requestLeadership();
				}
				break;
			default:
				checkLeadership();
				break;
			}
		}
	}

	/**
	 * Watcher implementation that watches the {@code /xd/container} znode path.
	 */
	class ContainerPathWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			LOG.info(">> PathWatcher event: {}", event);
			watchContainers();
		}
	}

	/**
	 * Callback implementation that is invoked to process the result of
	 * {@link ZooKeeper#getChildren} on the {@code /xd/container} znode.
	 */
	class ContainerPathCallback implements AsyncCallback.ChildrenCallback {
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
