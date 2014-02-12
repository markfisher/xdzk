package zk.election;

import java.io.IOException;

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
 * Represents a nomination on behalf of a {@link Candidate} capable of being elected as a leader.
 * Call the {@link #submit()} method to initiate the request for leadership.
 *
 * @author Mark Fisher
 */
public class Nomination {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(Nomination.class);

	/**
	 * Status of this nomination.
	 */
	private enum Status {
		SUBMITTING,
		ELECTED,
		REJECTED
	}

	/**
	 * Candidate proposed by this nomination.
	 */
	private final Candidate candidate;

	/**
	 * Current status of this nomination.
	 */
	private volatile Status status;

	/**
	 * Path of the node which the elected leader will create.
	 */
	private final String path;

	/**
	 * ZooKeeper client connect string. Default is {@code localhost:2181}.
	 */
	private final String connectString;

	/**
	 * Timeout for the ZooKeeper session. Default is 15 seconds.
	 */
	private final int sessionTimeout;

	/**
	 * ZooKeeper client used for all interaction with nodes.
	 */
	private ZooKeeper client;

	/**
	 * Watcher instance that watches the leader node.
	 */
	private final LeaderNodeWatcher leaderNodeWatcher = new LeaderNodeWatcher();

	/**
	 * Callback instance that is invoked to process the result of an attempt
	 * to write the leader node, i.e. a request to assume leadership.
	 */
	private final LeadershipRequestCallback leadershipRequestCallback = new LeadershipRequestCallback();

	/**
	 * Callback instance that is invoked to process the result of getting data from the leader node.
	 */
	private final LeadershipCheckCallback leadershipCheckCallback = new LeadershipCheckCallback();

	/**
	 * Callback instance that is invoked to process the result of checking the existence of the leader node.
	 */
	private final LeaderExistsCallback leaderExistsCallback = new LeaderExistsCallback();

	/**
	 * Watcher instance for the ZooKeeper client to notify of connections, disconnections, etc.
	 */
	private final ZooKeeperWatcher zkWatcher = new ZooKeeperWatcher();

	/**
	 * Create a Nomination using the default connect string of
	 * {@code localhost:2181} and the default session timeout of 15 seconds.
	 *
	 * @param candidate the Candidate for leadership
	 * @param path the path of the leader node in ZooKeeper
	 */
	public Nomination(Candidate candidate, String path) {
		this(candidate, path, "localhost:2181", 15000);
	}

	/**
	 * Create a Nomination.
	 *
	 * @param candidate the Candidate for leadership
	 * @param path the path of the leader node in ZooKeeper
	 * @param connectString the ZooKeeper client connect string
	 * @param sessionTimeout the ZooKeeper session timeout in milliseconds
	 */
	public Nomination(Candidate candidate, String path, String connectString, int sessionTimeout) {
		this.candidate = candidate;
		this.path = path;
		this.connectString = connectString;
		this.sessionTimeout = sessionTimeout;
	}

	/**
	 * Submit this nomination. Establishes a ZooKeeper client connection.
	 * The callback for successful connection will then proceed with a
	 * request to take leadership. A watch will be set on the leader node
	 * so that this nomination's candidate may become a leader in the future
	 * even if it is not elected in the initial attempt.
	 */
	public void submit() {
		try {
			this.client = new ZooKeeper(connectString, sessionTimeout, zkWatcher);
		}
		catch (IOException e) {
			throw new RuntimeException("failed to connect to ZooKeeper", e);
		}
	}

	/**
	 * Method invoked when the leader's elect method returns (resignation).
	 * Deletes the leader path eagerly so that another leader may take over ASAP.
	 * Also closes the ZooKeeper client.
	 */
	private void handleResignation() {
		try {
			client.delete(path, -1);
			client.close();
		}
		catch (KeeperException e) {
			throw new RuntimeException("failed during cleanup", e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Asynchronously request leadership by attempting to write the leader node.
	 *
	 * @see zk.election.Nomination.LeadershipRequestCallback
	 */
	private void requestLeadership() {
		client.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, leadershipRequestCallback, null);
	}

	/**
	 * Asynchronously attempt to get data from the leader node.
	 *
	 * @see zk.election.Nomination.LeadershipCheckCallback
	 */
	private void checkLeadership() {
		client.getData(path, false, leadershipCheckCallback, null);
	}

	/**
	 * Asynchronously check for the existence of the leader node.
	 *
	 * @see zk.election.Nomination.LeaderNodeWatcher
	 * @see zk.election.Nomination.LeaderExistsCallback
	 */
	private void leaderExists() {
		client.exists(path, leaderNodeWatcher, leaderExistsCallback, null);
	}

	/**
	 * Creates all parent nodes (but not the leader node) if necessary.
	 */
	private void ensureParentPathExists() {
		String parentPath = path.substring(0, path.lastIndexOf('/'));
		if (parentPath.length() == 0) {
			return;
		}
		try {
			if (client.exists(parentPath, false) == null) {
				String traversed = "/";
				if (parentPath.startsWith("/")) {
					parentPath = parentPath.substring(1);
				}
				String[] nodes = parentPath.split("/");
				for (String node : nodes) {
					String current = traversed + node;
					if (client.exists(current, false) == null) {
						client.create(current, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
					traversed = current + "/";
				}
			}
		}
		catch (KeeperException.NodeExistsException e) {
			// Assume this means that another member of the cluster is creating the same path
		}
		catch (KeeperException e) {
			throw new RuntimeException(e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Callback implementation that is invoked upon reading the leader node.
	 */
	private class LeadershipCheckCallback implements AsyncCallback.DataCallback {
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
	 * Watcher implementation that watches the leader node.
	 */
	private class LeaderNodeWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			if (EventType.NodeDeleted == event.getType() && path.equals(event.getPath())) {
				requestLeadership();
			}
		}
	}

	/**
	 * Callback implementation that is invoked upon writing the leader node.
	 */
	private class LeadershipRequestCallback implements AsyncCallback.StringCallback {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			LOG.info(">> LeadershipRequestCallback result: {}, {}, {}, {}", rc, path, ctx, name);
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkLeadership();
				return;
			case OK:
				status = Status.ELECTED;
				candidate.elect();
				// the elect method should block until resignation
				handleResignation();
				break;
			case NODEEXISTS:
				status = Status.REJECTED;
				leaderExists();
				break;
			default:
				status = Status.REJECTED;
				LOG.error("Something went wrong requesting leadership.",
						KeeperException.create(Code.get(rc), path));
			}
			LOG.info("Nomination status: {}", status);
		}
	}

	/**
	 * Callback implementation that is invoked when checking the existence of the leader node.
	 */
	private class LeaderExistsCallback implements AsyncCallback.StatCallback {

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				leaderExists();
				break;
			case OK:
				if (stat == null) {
					status = Status.SUBMITTING;
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
	 * Watcher implementation for the ZooKeeper client to notify of connections, disconnections, etc.
	 */
	private class ZooKeeperWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			if (KeeperState.SyncConnected.equals(event.getState())) {
				ensureParentPathExists();
				requestLeadership();
			}
			else if (KeeperState.Disconnected.equals(event.getState())) {
				LOG.info("DISCONNECTED");
				try {
					client.close();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

}
