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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.StringUtils;

import xdzk.cluster.ContainerRepository;
import xdzk.cluster.Container;
import xdzk.core.ModuleDescriptor;
import xdzk.core.ModuleRepository;
import xdzk.core.Stream;
import xdzk.core.StreamFactory;
import xdzk.curator.Paths;
import xdzk.curator.ChildPathIterator;
import xdzk.core.MapBytesUtility;

/**
 * Prototype implementation of an XD admin server that watches ZooKeeper
 * for Container arrivals and departures from the XD cluster. Each AdminServer
 * instance will attempt to request leadership, but at any given time only
 * one AdminServer instance in the cluster will have leadership status. Those
 * instances not elected will watch the {@link xdzk.curator.Paths#ADMIN} znode so that one
 * of them will take over leadership if the leader admin closes or crashes.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 */
public class AdminServer extends AbstractServer implements ContainerRepository {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);

	/**
	 * Cache of children under the containers path. This path is used to track
	 * containers in the cluster. Marked volatile because this reference is
	 * updated by the Curator event dispatch thread and read by public method
	 * {@link #getContainerPaths}.
	 */
	private volatile PathChildrenCache containers;

	/**
	 * Cache of children under the streams path. This path is used to track stream
	 * deployment requests. Marked volatile because this reference is written by
	 * the Curator thread that handles leader election and read by public
	 * method {@link #getStreamPaths}.
	 */
	private volatile PathChildrenCache streams;

	/**
	 * Leader selector to elect admin server that will handle stream
	 * deployment requests. Marked volatile because this reference is
	 * written and read by the Curator event dispatch threads - there
	 * is no guarantee that the same thread will do the reading and writing.
	 */
	private volatile LeaderSelector leaderSelector;

	/**
	 * Listener that is invoked when this admin server is elected leader.
	 */
	private final LeaderSelectorListener leaderListener = new LeaderListener();

	/**
	 * Converter from {@link ChildData} types to {@link xdzk.cluster.Container}.
	 */
	private final ContainerConverter containerConverter = new ContainerConverter();

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
	 * Server constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public AdminServer(String hostPort, MapBytesUtility mapBytesUtility, ModuleRepository moduleRepository) {
		super(hostPort);
		this.mapBytesUtility = mapBytesUtility;
		this.moduleRepository = moduleRepository;
		this.streamFactory = new StreamFactory(moduleRepository); // todo: use DI
	}

	/**
	 * Return the set of current container paths.
	 *
	 * @return read-only set of container paths
	 */
	public Set<String> getContainerPaths() {
		// todo: instead of returning a set here, perhaps
		// we can return Iterator<String>
		Set<String> containerSet = new HashSet<String>();
		if (containers != null) {
			for (ChildData child : containers.getCurrentData()) {
				containerSet.add(child.getPath());
			}
		}
		return Collections.unmodifiableSet(containerSet);
	}

	/**
	 * Return the set of current stream paths.
	 *
	 * @return read-only set of stream paths
	 */
	public Set<String> getStreamPaths() {
		// todo: instead of returning a set here, perhaps
		// we can return Iterator<String>
		Set<String> streamSet = new HashSet<String>();
		if (streams != null) {
			for (ChildData child : streams.getCurrentData()) {
				streamSet.add(child.getPath());
			}
		}
		return Collections.unmodifiableSet(streamSet);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<Container> getContainerIterator() {
		return new ChildPathIterator<Container>(containerConverter, containers);
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Upon connection, the admin server starts to listen for containers joining
	 * and leaving the cluster. It also attempts to take the leadership role
	 * for managing stream deployment requests.
	 */
	@Override
	protected void onConnect(ConnectionState newState) {
		LOG.info("Admin {} CONNECTED", this.getId());

		try {
			CuratorFramework client = getClient();

			Paths.ensurePath(client, Paths.DEPLOYMENTS);
			Paths.ensurePath(client, Paths.CONTAINERS);
			Paths.ensurePath(client, Paths.STREAMS);

			leaderSelector = new LeaderSelector(client, Paths.createPathWithNamespace(Paths.ADMIN), leaderListener);
			leaderSelector.setId(getId());
			leaderSelector.start();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Upon disconnect, the {@link #leaderSelector} is closed and the container
	 * listener is removed.
	 */
	@Override
	protected void onDisconnect(ConnectionState newState) {
		try {
			leaderSelector.close();
		}
		catch (IllegalStateException e) {
			// IllegalStateException is thrown if leaderSelector or
			// containers have already been closed
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Handle a stream deployment request. Upon completion of this
	 * method, the request for deployment is persisted. However, the
	 * actual deployment of the stream is executed in the background.
	 * <p>
	 * Implementation consists of writing the stream deployment
	 * request under the {@link xdzk.curator.Paths#STREAMS} znode.
	 *
	 * @param name        stream name
	 * @param definition  stream definition (pipe delimited list of modules)
	 */
	public void handleStreamDeployment(String name, String definition) {
		// TODO: improve parameter validation
		if (name == null) {
			throw new IllegalArgumentException("name must not be null");
		}
		if (definition == null) {
			throw new IllegalArgumentException("definition must not be null");
		}
		try {
			getClient().create().forPath(Paths.createPath(Paths.STREAMS, name),
					mapBytesUtility.toByteArray(Collections.singletonMap("definition", definition)));
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Converts a {@link org.apache.curator.framework.recipes.cache.ChildData} node
	 * to a {@link xdzk.cluster.Container}.
	 */
	public class ContainerConverter implements Converter<ChildData, Container> {

		@Override
		public Container convert(ChildData source) {
			// This converter will be invoked upon every iteration of the
			// iterator returned by getContainerIterator. While elegant,
			// this isn't exactly efficient. TODO - revisit
			return new Container(Paths.stripPath(source.getPath()), mapBytesUtility.toMap(source.getData()));
		}
	}

	/**
	 * Listener implementation that is invoked when containers are added/removed/modified.
	 */
	class ContainerListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			switch (event.getType()) {
				case CHILD_ADDED:
					LOG.info("Container added: {}", Paths.stripPath(event.getData().getPath()));
					break;
				case CHILD_UPDATED:
					LOG.info("Container updated: {}", Paths.stripPath(event.getData().getPath()));
					break;
				case CHILD_REMOVED:
					onChildLeft(client, event.getData());
					break;
				case CONNECTION_SUSPENDED:
					break;
				case CONNECTION_RECONNECTED:
					break;
				case CONNECTION_LOST:
					break;
				case INITIALIZED:
					break;
			}
		}

		/**
		 * Handle the departure of a container.
		 *
		 * @param client  curator client
		 * @param data    node data for the container that departed
		 */
		private void onChildLeft(CuratorFramework client, ChildData data) {
			// find all of the deployments for the container that left
			String container = Paths.stripPath(data.getPath());
			LOG.info("Container departed: {}", container);

			try {
				Map<String, Stream> streamMap = new HashMap<String, Stream>();
				List<String> deployments = client.getChildren().forPath(Paths.createPath(Paths.DEPLOYMENTS, container));
				for (String deployment : deployments) {
					String[] parts = deployment.split("\\.");
					String streamName = parts[0];
					String moduleType = parts[1];
					String moduleName = parts[2];
					String moduleLabel = parts[3];

					Stream stream = streamMap.get(streamName);
					if (stream == null) {
						stream = streamFactory.createStream(streamName, mapBytesUtility.toMap(
								client.getData().forPath(Paths.createPath(Paths.STREAMS, streamName))));
						streamMap.put(streamName, stream);
					}
					ModuleDescriptor moduleDescriptor = stream.getModuleDescriptor(moduleName, moduleType);
					if (moduleDescriptor.getCount() > 0) {
						// for now assume that just one redeployment is needed

						// todo: refactor duplicate code from StreamListener
						Iterator<Container> iterator = stream.getContainerMatcher()
								.match(moduleDescriptor, AdminServer.this).iterator();

						if (iterator.hasNext()) {
							Container targetContainer = iterator.next();
							String targetName = targetContainer.getName();

							LOG.info("Redeploying module {} for stream {} to container {}",
									moduleName, streamName, targetName);

							client.create().creatingParentsIfNeeded().forPath(
									Paths.createPath(Paths.DEPLOYMENTS, targetName,
											String.format("%s.%s.%s.%s", streamName, moduleType, moduleName, moduleLabel)));

							// todo: not going to bother verifying the redeployment for now
						}
						else {
							// uh oh
							LOG.warn("No containers available for redeployment of {} for stream {}", moduleName, streamName);
						}
					}
					else {
						StringBuilder builder = new StringBuilder();
						String group = moduleDescriptor.getGroup();
						builder.append("Module '").append(moduleName).append("' with label '")
								.append(moduleLabel).append("' is targeted to all containers");
						if (StringUtils.hasText(group)) {
							builder.append(" belonging to group '").append(group).append('\'');
						}
						builder.append("; it does not need to be redeployed");

						LOG.info(builder.toString());
					}
				}

				// remove the deployments from the departed container
				client.delete().deletingChildrenIfNeeded().forPath(Paths.createPath(Paths.DEPLOYMENTS, container));
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			catch (Exception e) {
				throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
			}
		}
	}

	/**
	 * Listener implementation that is invoked when this server becomes the leader.
	 */
	class LeaderListener extends LeaderSelectorListenerAdapter {

		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			LOG.info("Leader Admin {} is watching for stream deployment requests.", getId());
			PathChildrenCacheListener containerListener = new ContainerListener();

			PathChildrenCacheListener streamListener =
					new StreamListener(AdminServer.this, moduleRepository);

			try {
				containers = new PathChildrenCache(client, Paths.CONTAINERS, true);
				containers.getListenable().addListener(containerListener);
				containers.start();

				streams = new PathChildrenCache(client, Paths.STREAMS, true);
				streams.getListenable().addListener(streamListener);
				streams.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

				Thread.sleep(Long.MAX_VALUE);
			}
			catch (InterruptedException e) {
				LOG.info("Leadership canceled due to thread interrupt");
				Thread.currentThread().interrupt();
			}
			finally {
				streams.getListenable().removeListener(streamListener);
				streams.close();

				containers.getListenable().removeListener(containerListener);
				containers.close();
			}
		}
	}

}
