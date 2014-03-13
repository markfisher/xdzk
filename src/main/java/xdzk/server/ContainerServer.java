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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.StringUtils;

import xdzk.core.MapBytesUtility;
import xdzk.core.Module;
import xdzk.core.ModuleDescriptor;
import xdzk.core.ModuleRepository;
import xdzk.core.Stream;
import xdzk.core.StreamFactory;
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
	 * Watcher for modules deployed to this container under the {@link Paths#STREAMS}
	 * location.
	 */
	private final StreamModuleWatcher streamModuleWatcher = new StreamModuleWatcher();

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
	 * Stream factory.
	 */
	private final StreamFactory streamFactory;

	/**
	 * Map of deployed modules.
	 */
	private final Map<ModuleDescriptor.Key, ModuleDescriptor> mapDeployedModules =
			new ConcurrentHashMap<ModuleDescriptor.Key, ModuleDescriptor>();

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
		this.streamFactory = new StreamFactory(moduleRepository); // todo: this should be injected
	}

	/**
	 * Deploy the requested module.
	 * </p>
	 * TODO: this is a placeholder
	 *
	 * @param moduleDescriptor
	 */
	private void deployModule(ModuleDescriptor moduleDescriptor) {
		LOG.info("Deploying module {}", moduleDescriptor);
		mapDeployedModules.put(moduleDescriptor.newKey(), moduleDescriptor);
	}

	/**
	 * Undeploy the requested module.
	 * </p>
	 * TODO: this is a placeholder
	 *
	 * @param moduleLabel  module label
	 * @param moduleType   module type
	 */
	protected void undeployModule(String moduleLabel, String moduleType) {
		ModuleDescriptor.Key key = new ModuleDescriptor.Key(Module.Type.valueOf(moduleType.toUpperCase()), moduleLabel);
		ModuleDescriptor descriptor = mapDeployedModules.get(key);
		if (descriptor == null) {
			LOG.info("Module {} already undeployed", moduleLabel);
		}
		else {
			LOG.info("Undeploying module {}", descriptor);
			mapDeployedModules.remove(key);
		}
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

			// todo: modules should be undeployed
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
		String deployment = Paths.stripPath(data.getPath());
		String[] split = deployment.split("\\.");
		String streamName = split[0];
		String moduleType = split[1];
		String moduleName = split[2];
		String moduleLabel = split[3];

		LOG.info("Deploying module '{}' for stream '{}'", moduleName, streamName);
		LOG.debug("streamName={}, moduleType={}, moduleName={}, moduleLabel={}", streamName, moduleType, moduleName, moduleLabel);

		String streamPath = Paths.build(Paths.STREAMS, streamName, moduleType,
				String.format("%s.%s", moduleName, moduleLabel), getId());

		try {
			Stream stream = streamFactory.createStream(streamName,
					mapBytesUtility.toMap(client.getData().forPath(Paths.build(Paths.STREAMS, streamName))));

			deployModule(stream.getModuleDescriptor(moduleName, moduleType));

			// this indicates that the container has deployed the module
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.forPath(streamPath, mapBytesUtility.toByteArray(Collections.singletonMap("state", "deployed")));

			// set a watch on this module in the stream path; if the node
			// is deleted this indicates an undeployment
			client.getData().usingWatcher(streamModuleWatcher).forPath(streamPath);
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
		// todo: review
		// module undeployment only handled when removing the module in
		// the stream, not under /xd/deployments
	}

	/**
	 * Watcher for the modules deployed to this container under the {@link Paths#STREAMS}
	 * location. If the node is deleted, this container will undeploy the module.
	 */
	class StreamModuleWatcher implements CuratorWatcher {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void process(WatchedEvent event) throws Exception {
			if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
				// todo: we really need some kind of utility to do this path stuff
				String[] pathElements = event.getPath().split("\\/");
				String streamName = pathElements[2];
				String moduleType = pathElements[3];
				String moduleName = pathElements[4].split("\\.")[0];
				String moduleLabel = pathElements[4].split("\\.")[1];
				undeployModule(moduleLabel, moduleType);

				String deploymentPath = Paths.build(Paths.DEPLOYMENTS, getId(),
						String.format("%s.%s.%s.%s", streamName, moduleType, moduleName, moduleLabel));

				LOG.trace("Deleting path: {}",  deploymentPath);

				getClient().delete().forPath(deploymentPath);
			}
			else {
				// this watcher is only interested in deletes for the purposes
				// of undeploying modules; if any other change occurs the
				// watch needs to be reestablished
				getClient().getData().usingWatcher(this).forPath(event.getPath());
			}
		}
	}

	/**
	 * Listener for deployment requests for this container under {@link Paths#DEPLOYMENTS}.
	 */
	class DeploymentListener implements PathChildrenCacheListener {

		/**
		 * {@inheritDoc}
		 */
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
