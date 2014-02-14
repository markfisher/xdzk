package xdzk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xdzk.Path;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

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
	 * Cache of children under {@code /xd/deployments/[this-container-id]}.
	 */
	private final PathChildrenCache deployments;

	/**
	 * A {@link PathChildrenCacheListener} implementation that handles deployment
	 * requests (and deployment removals) for this container.
	 */
	private final DeploymentListener deploymentListener = new DeploymentListener();

	/**
	 * Server constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public ContainerServer(String hostPort) {
		super(hostPort);
		deployments = new PathChildrenCache(getClient(), Path.DEPLOYMENTS + "/" + this.getId(), false);
		deployments.getListenable().addListener(deploymentListener);
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Creates ephemeral node for this container and starts {@link #deployments} cache.
	 */
	@Override
	protected void onConnect(ConnectionState newState) {
		try {
			CuratorFramework client = getClient();

			if (client.checkExists().forPath(Path.CONTAINERS.toString()) == null) {
				client.create().creatingParentsIfNeeded().forPath(Path.CONTAINERS.toString());
			}

			Map<String, String> attributes = new HashMap<>();
			String mxBeanName = ManagementFactory.getRuntimeMXBean().getName();
			String tokens[] = mxBeanName.split("@");
			attributes.put("pid", tokens[0]);
			attributes.put("host", tokens[1]);

			client.create().withMode(CreateMode.EPHEMERAL).forPath(
					Path.CONTAINERS + "/" + this.getId(), attributes.toString().getBytes());

			deployments.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

			LOG.info("Started container {} with attributes: {} ", this.getId(), attributes);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Closes the {@link #deployments} cache.
	 */
	protected void onDisconnect(ConnectionState newState) {
		try {
			deployments.close();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Handle a new deployment.
	 *
	 * @param deployment name of deployment
	 */
	protected void onDeploymentAdded(String deployment) {
		LOG.info("Deployment added: {}", deployment);
	}

	/**
	 * Handle the removal of a deployment.
	 *
	 * @param deployment name of deployment
	 */
	protected void onDeploymentRemoved(String deployment) {
		LOG.info("Deployment removed: {}", deployment);
	}

	/**
	 * Strip path information from a string. For example,
	 * given an input of {@code /xd/path/location}, return
	 * {@code location}.
	 *
	 * @param path path string
	 *
	 * @return string with path stripped
	 */
	private String stripPath(String path) {
		// todo: error handling
		return path.substring(path.lastIndexOf('/') + 1);
	}

	class DeploymentListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			LOG.debug("Path cache event: {}", event);
			switch (event.getType()) {
				case INITIALIZED:
					// todo: when the cache is initialized the getInitialData
					// collection will contain all the children - instead of
					// issuing a deployment this should perhaps determine
					// if a deployment is required.

					// For now just (wrongly) assume that everything
					// should be deployed.
					for (ChildData data : event.getInitialData()) {
						onDeploymentAdded(stripPath(data.getPath()));
					}
					break;
				case CHILD_ADDED:
					onDeploymentAdded(stripPath(event.getData().getPath()));
					break;
				case CHILD_REMOVED:
					onDeploymentRemoved(stripPath(event.getData().getPath()));
					break;
				default:
					break;
			}
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
