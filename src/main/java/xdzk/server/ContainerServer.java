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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xdzk.core.MapBytesUtility;
import xdzk.core.Module;
import xdzk.core.ModuleRepository;
import xdzk.core.StubModuleRepository;
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
	 * Utility to convert maps to byte arrays.
	 */
	private final MapBytesUtility mapBytesUtility = new MapBytesUtility();

	/**
	 * Module repository.
	 */
	// TODO: make this pluggable
	private final ModuleRepository moduleRepository = new StubModuleRepository();

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

			deployments = new PathChildrenCache(client, Paths.DEPLOYMENTS + "/" + this.getId(), true);
			deployments.getListenable().addListener(deploymentListener);

			String mxBeanName = ManagementFactory.getRuntimeMXBean().getName();
			String tokens[] = mxBeanName.split("@");
			Map<String, String> map = new HashMap<String, String>();
			map.put("pid", tokens[0]);
			map.put("host", tokens[1]);

			client.create().withMode(CreateMode.EPHEMERAL).forPath(
					Paths.CONTAINERS + "/" + this.getId(), mapBytesUtility.toByteArray(map));

			deployments.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

			LOG.info("Started container {} with attributes: {} ", this.getId(), map);
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
	 * Event handler for new module deployments.
	 *
	 * @param client  curator client
	 * @param data    module data
	 */
	private void onChildAdded(CuratorFramework client, ChildData data) {
		Map<String, String> attributes = mapBytesUtility.toMap(data.getData());
		String streamName = attributes.get("stream");
		String moduleType = attributes.get("type");
		String moduleName = Paths.stripPath(data.getPath());

		LOG.info("Deploying module {} for stream {}", moduleName, streamName);

		Module module = moduleRepository.loadModule(moduleName, Module.Type.valueOf(moduleType.toUpperCase()));

		LOG.info("Loading module from {}", module.getUrl());

		// todo: this is where we load the module

		String path = Paths.createPath(Paths.STREAMS, streamName, moduleType, moduleName, getId());

		try {
			// this indicates that the container has deployed the module
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.forPath(path, mapBytesUtility.toByteArray(Collections.singletonMap("state", "deployed")));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (KeeperException.NodeExistsException e) {
			// todo: review, this should not happen
			LOG.info("Module for stream {} already deployed", moduleName, streamName);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Event handler for deployment removals.
	 *
	 * @param client  curator client
	 * @param data    module data
	 */
	private void onChildRemoved(CuratorFramework client, ChildData data) {
		// todo: implement
		LOG.info("Deployment removed: {}", Paths.stripPath(data.getPath()));
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
						onChildAdded(client, data);
					}
					break;
				case CHILD_ADDED:
					onChildAdded(client, event.getData());
					break;
				case CHILD_REMOVED:
					onChildRemoved(client, event.getData());
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
