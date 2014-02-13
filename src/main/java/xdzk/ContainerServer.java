package xdzk;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zk.node.Node;
import zk.node.NodeListener;
import zk.node.NodeListenerAdapter;

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
	 * {@link zk.node.NodeListener} implementation that handles deployment requests
	 * (and deployment removals) for this container.
	 */
	private final NodeListener deploymentListener = new DeploymentListener();

	/**
	 * Node used to track deployments for this container.
	 */
	// Marked as volatile because this reference is updated by the
	// ZK event dispatch thread and is read by public method getDeployments
	private volatile Node deployments;


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
	 * <p/>
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

			deployments = new Node(zk, Path.DEPLOYMENTS + "/" + this.getId());
			deployments.addListener(deploymentListener);

			LOG.info("Started container {} with attributes: {} ", this.getId(), attributes);
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
	 * Return a set of deployments for this container server.
	 *
	 * @return read-only set of deployments
	 */
	public Set<String> getDeployments() {
		return deployments.getChildren();
	}

	/**
	 * {@link zk.node.NodeListener} implementation that handles
	 * deployment additions and removals.
	 */
	class DeploymentListener extends NodeListenerAdapter {
		@Override
		public void onChildrenAdded(Set<String> children) {
			LOG.info("Deployments added: {}", children);
		}

		@Override
		public void onChildrenRemoved(Set<String> children) {
			LOG.info("Deployments removed: {}", children);
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
