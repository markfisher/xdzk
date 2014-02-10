package xdzk;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prototype for a container node that writes its attributes to an
 * ephemeral znode under {@code /xd/containers/}. The name of that
 * znode matches the UUID generated for this ContainerServer instance.
 *
 * @author Mark Fisher
 * @author Patrick Peralta
 */
public class ContainerServer extends AbstractServer {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(ContainerServer.class);

	/**
	 * Watcher instance that watches the {@code /xd/deployments} znode path.
	 */
	private final DeploymentPathWatcher deploymentPathWatcher = new DeploymentPathWatcher();

	/**
	 * Callback instance that is invoked to process the result of
	 * {@link ZooKeeper#getChildren} on the {@code /xd/deployments} znode.
	 */
	private final DeploymentPathCallback deploymentPathCallback = new DeploymentPathCallback();

	/**
	 * Set of deployments under {@code /xd/container/[this-server-id]}.
	 */
	// Marked as volatile because this reference is updated by the
	// ZK event dispatch thread and is read by public method getDeployments
	private volatile Set<String> deployments = Collections.emptySet();

	/**
	 * Construct a ContainerServer.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public ContainerServer(String hostPort) {
		super(hostPort);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void doStart() throws Exception {
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Upon connection the container will ensure the creation of required
	 * znode paths and will set up appropriate watches.
	 */
	@Override
	protected void onConnect(WatchedEvent event) {
		try {
			ZooKeeper zk = this.getClient();
			Path.CONTAINERS.verify(zk);

			Map<String, String> attributes = new HashMap<>();
			String mxBeanName = ManagementFactory.getRuntimeMXBean().getName();
			String tokens[] = mxBeanName.split("@");
			attributes.put("pid", tokens[0]);
			attributes.put("host", tokens[1]);

			zk.create(Path.CONTAINERS + "/" + this.getId(), attributes.toString().getBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

			Path.DEPLOYMENTS.verify(zk);
			zk.create(Path.DEPLOYMENTS + "/" + this.getId(), null,
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			LOG.info("Started container {} with attributes: {} ", this.getId(), attributes);

			watchDeployments();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			// todo: we may be in an inconsistent state if interrupted;
			// not sure what to do here
		}
		catch (KeeperException.NodeExistsException e) {
			// It is possible (but very unlikely) that an admin server is also
			// creating an /xd/deployments/server-id node
		}
		catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Asynchronously obtain a list of children under {@code /xd/deployments}.
	 *
	 * @see xdzk.ContainerServer.DeploymentPathCallback
	 */
	protected void watchDeployments() {
		getClient().getChildren(Path.DEPLOYMENTS.toString() + '/' + getId(),
				deploymentPathWatcher, deploymentPathCallback, null);
	}

	/**
	 * Return a set of deployments for this container server.
	 *
	 * @return read-only set of deployments
	 */
	public Set<String> getDeployments() {
		return deployments;
	}

	/**
	 * Watcher implementation that watches the {@code /xd/deployments} znode path.
	 */
	class DeploymentPathWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			watchDeployments();
		}
	}

	/**
	 * Callback implementation that is invoked to process the result of
	 * {@link ZooKeeper#getChildren} on the {@code /xd/deployments} znode.
	 */
	class DeploymentPathCallback implements AsyncCallback.ChildrenCallback {
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			LOG.debug(">>> path: {}, children: {}", path, children);
			Set <String> arrived = new HashSet<>();
			Set <String> departed = new HashSet<>();
			if (children == null) {
				children = Collections.emptyList();
			}

			for (String child : children) {
				if (!deployments.contains(child)) {
					arrived.add(child);
				}
			}

			Set<String> newPaths = Collections.unmodifiableSet(new HashSet<>(children));
			for (String child : deployments) {
				if (!newPaths.contains(child)) {
					departed.add(child);
				}
			}

			deployments = newPaths;

			LOG.info("New deployments:      {}", arrived);
			LOG.info("Departed deployments: {}", departed);
			LOG.info("All deployments:      {}", deployments);
		}
	}

	/**
	 * Start a container node. A ZooKeeper host:port may be optionally
	 * passed in as an argument. The default ZooKeeper host/port is
	 * {@code localhost:2181}.
	 *
	 * @param args command line arguments
	 *
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new ContainerServer(args.length == 1 ? args[0] : "localhost:2181").run();
	}

}
