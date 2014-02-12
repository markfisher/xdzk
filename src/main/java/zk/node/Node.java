package zk.node;


import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

/**
 * Representation of a ZooKeeper node with the following features:
 * <ul>
 *     <li>Maintains a cache of children nodes</li>
 *     <li>Allows for the registration of {@link zk.node.NodeListener NodeListeners}
 *         that are notified upon the addition and removal of child nodes</li>
 * </ul>
 * Instances must be initialized prior to usage. Idiomatic construction
 * of this object is as such:
 * <pre>
 *     Node node = new Node(client, path).init();
 * </pre>
 *
 * @author Patrick Peralta
 */
public class Node {
	private static final Logger LOG = LoggerFactory.getLogger(Node.class);

	/**
	 * ZooKeeper client.
	 */
	private final ZooKeeper client;

	/**
	 * Absolute path for this node.
	 */
	private final String path;

	/**
	 * Watcher instance that is notified when children are added/removed
	 * from this node.
	 */
	private final ChildWatcher childWatcher = new ChildWatcher();

	/**
	 * Callback instance that is invoked to process the result
	 * of {@link org.apache.zookeeper.ZooKeeper#getChildren}.
	 *
	 * @see #watchChildren
	 */
	private final ChildCallback childCallback = new ChildCallback();

	/**
	 * Cache of children for this node.
	 */
	private volatile Set<String> cache = Collections.emptySet();

	/**
	 * Set of {@link zk.node.NodeListener NodeListeners} to be notified
	 * when children are added/removed from this node.
	 */
	private final Set<NodeListener> listeners = new CopyOnWriteArraySet<>();


	/**
	 * Construct a Node.
	 * <p>
	 * Note that {@link #init} must be invoked prior to usage. Consider
	 * the following pattern for construction:
	 * <pre>
	 *     Node node = new Node(client, path).init();
	 * </pre>
	 *
	 * @param client  ZooKeeper client
	 * @param path    absolute path for this node
	 */
	public Node(ZooKeeper client, String path) {
		this.path = path;
		this.client = client;
	}

	/**
	 * Initialize this node. Initialization consists of the following:
	 * <ul>
	 *     <li>Ensure that the full path exists in ZooKeeper</li>
	 *     <li>Invoke {@link org.apache.zookeeper.ZooKeeper#getChildren}
	 *         in order to populate the cache and register the watch</li>
	 * </ul>
	 *
	 * @return this object
	 *
	 * @throws InterruptedException
	 */
	public Node init() throws InterruptedException {
		try {
			if (client.exists(path, false) == null) {
				String traversed = "/";
				String p = this.path;
				if (p.startsWith("/")) {
					p = p.substring(1);
				}
				String[] nodes = p.split("/");
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
			// Assume this means that another member of the cluster
			// is creating the same path
		}
		catch (KeeperException e) {
			throw new RuntimeException(e);
		}

		watchChildren();

		return this;
	}

	/**
	 * Return the cached set of children for this node.
	 *
	 * @return set of children for this node
	 */
	public Set<String> getChildren() {
		return cache;
	}

	/**
	 * Force a refresh of children for this node. This method
	 * updates the cache and returns the latest set of children.
	 *
	 * @return refreshed set of children for this node
	 *
	 * @throws InterruptedException
	 */
	public Set<String> refreshChildren() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		client.getChildren(path, childWatcher, childCallback, latch);
		latch.await();
		return cache;
	}

	/**
	 * Add a {@link zk.node.NodeListener} to this node.
	 *
	 * @param listener node listener to add
	 */
	public void addListener(NodeListener listener) {
		listeners.add(listener);
	}

	/**
	 * Remove the indicated {@link zk.node.NodeListener}
	 * from this node.
	 *
	 * @param listener node listener to remove
	 */
	public void removeListener(NodeListener listener) {
		listeners.remove(listener);
	}

	/**
	 * Asynchronously update the cache of children for this node.
	 * This also registers a watch that will be triggered when
	 * children are added/removed from this node.
	 */
	protected void watchChildren() {
		client.getChildren(path, childWatcher, childCallback, null);
	}

	/**
	 * Watcher implementation for the children of this node.
	 */
	class ChildWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			watchChildren();
		}
	}

	/**
	 * Callback implementation that is invoked to process the result
	 * of {@link org.apache.zookeeper.ZooKeeper#getChildren} for this node.
	 */
	class ChildCallback implements AsyncCallback.ChildrenCallback {

		/**
		 * {@inheritDoc}
		 * <p>
		 * This callback implementation does the following:
		 * <ul>
		 *     <li>Updates the {@link #cache} of children</li>
		 *     <li>Fires events to the registered {@link zk.node.NodeListener NodeListeners}</li>
		 * </ul>
		 */
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			LOG.debug(">>> path: {}, children: {}", path, children);
			Set <String> arrived = new HashSet<>();
			Set <String> departed = new HashSet<>();
			if (children == null) {
				children = Collections.emptyList();
			}

			for (String child : children) {
				if (!cache.contains(child)) {
					arrived.add(child);
				}
			}

			Set<String> newChildren = Collections.unmodifiableSet(new HashSet<>(children));
			for (String child : cache) {
				if (!newChildren.contains(child)) {
					departed.add(child);
				}
			}

			cache = newChildren;

			LOG.debug("New:      {}", arrived);
			LOG.debug("Departed: {}", departed);
			LOG.debug("All:      {}", cache);

			if (arrived.isEmpty() && departed.isEmpty()) {
				return;
			}

			for (NodeListener listener : listeners) {
				if (!arrived.isEmpty()) {
					listener.onChildrenAdded(Collections.unmodifiableSet(arrived));
				}
				if (!departed.isEmpty()) {
					listener.onChildrenRemoved(Collections.unmodifiableSet(departed));
				}
			}

			if (ctx instanceof CountDownLatch) {
				((CountDownLatch) ctx).countDown();
			}
		}
	}

}
