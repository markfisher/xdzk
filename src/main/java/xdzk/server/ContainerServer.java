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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

import org.springframework.util.StringUtils;

import xdzk.core.MapBytesUtility;
import xdzk.core.Module;
import xdzk.core.ModuleRepository;
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
	private final MapBytesUtility mapBytesUtility;

	/**
	 * Module repository.
	 */
	private final ModuleRepository moduleRepository;

	/**
	 * The set of groups this container belongs to.
	 */
	private final Set<String> groups;

	/**
	 * Server constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public ContainerServer(String hostPort, String groups, MapBytesUtility mapBytesUtility,
			ModuleRepository moduleRepository) {
		super(hostPort);
		if (groups == null) {
			this.groups = Collections.emptySet();
		}
		else {
			Set<String> set = new HashSet<String>();
			Collections.addAll(set, StringUtils.tokenizeToStringArray(groups, ","));
			this.groups = Collections.unmodifiableSet(set);
		}
		this.mapBytesUtility = mapBytesUtility;
		this.moduleRepository = moduleRepository;
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

			deployments = new PathChildrenCache(client, Paths.build(Paths.DEPLOYMENTS, this.getId()), true);
			deployments.getListenable().addListener(deploymentListener);

			String mxBeanName = ManagementFactory.getRuntimeMXBean().getName();
			String tokens[] = mxBeanName.split("@");
			Map<String, String> map = new HashMap<String, String>();
			map.put("pid", tokens[0]);
			map.put("host", tokens[1]);

			StringBuilder builder = new StringBuilder();
			Iterator<String> iterator = groups.iterator();
			while (iterator.hasNext()) {
				builder.append(iterator.next());
				if (iterator.hasNext()) {
					builder.append(',');
				}
			}
			map.put("groups", builder.toString());

			client.create().withMode(CreateMode.EPHEMERAL).forPath(
					Paths.build(Paths.CONTAINERS, this.getId()),
					mapBytesUtility.toByteArray(map));

			deployments.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

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
		String deploymentPath = Paths.stripPath(data.getPath());
		String[] split = deploymentPath.split("\\.");
		String streamName = split[0];
		String moduleType = split[1];
		String moduleName = split[2];
		String moduleLabel = split[3];

		LOG.info("Deploying module '{}' for stream '{}'", moduleName, streamName);
		LOG.debug("streamName={}, moduleType={}, moduleName={}, moduleLabel={}", streamName, moduleType, moduleName, moduleLabel);

		Module module = moduleRepository.loadModule(moduleName, Module.Type.valueOf(moduleType.toUpperCase()));

		LOG.debug("Loading module from {}", module.getUrl());

		// todo: this is where we load the module

		String streamPath = Paths.build(Paths.STREAMS, streamName, moduleType,
				String.format("%s.%s", moduleName, moduleLabel), getId());

		try {
			// this indicates that the container has deployed the module
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.forPath(streamPath, mapBytesUtility.toByteArray(Collections.singletonMap("state", "deployed")));
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

}
