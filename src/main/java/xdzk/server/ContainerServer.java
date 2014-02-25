/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xdzk.server;

import java.lang.management.ManagementFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xdzk.curator.Paths;

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
	 * A {@link PathChildrenCacheListener} implementation that handles deployment
	 * requests (and deployment removals) for this container.
	 */
	private final DeploymentListener deploymentListener = new DeploymentListener();

	/**
	 * Cache of children under the deployments path.
	 */
	private volatile PathChildrenCache deployments;

	/**
	 * Server constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public ContainerServer(String hostPort) {
		super(hostPort);
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

			Paths.ensurePath(client, Paths.DEPLOYMENTS);
			Paths.ensurePath(client, Paths.CONTAINERS);

			deployments = new PathChildrenCache(client, Paths.DEPLOYMENTS + "/" + this.getId(), false);
			deployments.getListenable().addListener(deploymentListener);

			String mxBeanName = ManagementFactory.getRuntimeMXBean().getName();
			String tokens[] = mxBeanName.split("@");
			StringBuilder builder = new StringBuilder()
					.append("pid=").append(tokens[0])
					.append(System.lineSeparator())
					.append("host=").append(tokens[1]);

			client.create().withMode(CreateMode.EPHEMERAL).forPath(
					Paths.CONTAINERS + "/" + this.getId(), builder.toString().getBytes("UTF-8"));

			deployments.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

			LOG.info("Started container {} with attributes: {} ", this.getId(), builder);
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
	@Override
	protected void onDisconnect(ConnectionState newState) {
		try {
			LOG.warn(">>> disconnected: {}", newState);
			deployments.getListenable().removeListener(deploymentListener);
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
						onDeploymentAdded(Paths.stripPath(data.getPath()));
					}
					break;
				case CHILD_ADDED:
					onDeploymentAdded(Paths.stripPath(event.getData().getPath()));
					break;
				case CHILD_REMOVED:
					onDeploymentRemoved(Paths.stripPath(event.getData().getPath()));
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
