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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdzk.cluster.ContainerMatcher;
import xdzk.cluster.Container;
import xdzk.cluster.ContainerRepository;
import xdzk.core.MapBytesUtility;
import xdzk.core.Module;
import xdzk.core.ModuleRepository;
import xdzk.core.Stream;
import xdzk.core.StreamFactory;
import xdzk.curator.Paths;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Listener implementation that handles stream deployment requests.
 *
* @author Patrick Peralta
*/
public class StreamListener implements PathChildrenCacheListener {
	/**
	 * Logger.
	 */
	private final Logger LOG = LoggerFactory.getLogger(StreamListener.class);

	/**
	 * Provides access to the current container list.
	 */
	private final ContainerRepository containerRepository;

	/**
	 * Matches a deployment request to a container.
	 */
	private final ContainerMatcher matcher;

	/**
	 * Utility to convert maps to byte arrays.
	 */
	private final MapBytesUtility mapBytesUtility = new MapBytesUtility();

	/**
	 * Module repository.
	 */
	private final ModuleRepository moduleRepository;

	/**
	 * Stream factory.
	 */
	private final StreamFactory streamFactory;


	/**
	 * Construct a StreamListener.
	 *
	 * @param containerRepository admin server that this listener is attached to
	 */
	public StreamListener(ContainerRepository containerRepository, ContainerMatcher matcher,
						ModuleRepository moduleRepository) {
		this.containerRepository = containerRepository;
		this.matcher = matcher;
		this.moduleRepository = moduleRepository;
		this.streamFactory = new StreamFactory(moduleRepository);
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Handle child events for the {@link xdzk.curator.Paths#STREAMS} path.
	 */
	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		switch (event.getType()) {
			case CHILD_ADDED:
				onStreamAdded(client, event.getData());
				break;
			case CHILD_UPDATED:
				break;
			case CHILD_REMOVED:
				LOG.info("Stream removed: {}", Paths.stripPath(event.getData().getPath()));
				// todo: what to do when stream is removed?
			case CONNECTION_SUSPENDED:
				break;
			case CONNECTION_RECONNECTED:
				break;
			case CONNECTION_LOST:
				break;
			case INITIALIZED:
				// TODO!!
				// when this admin is first elected leader and there are
				// streams, it needs to verify that the streams have been
				// deployed
				for (ChildData childData : event.getInitialData()) {
					LOG.info("Existing stream: {}", Paths.stripPath(childData.getPath()));
				}
				break;
		}
	}

	/**
	 * Handle the creation of a new stream.
	 *
	 * @param client  curator client
	 * @param data    stream data
	 */
	private void onStreamAdded(CuratorFramework client, ChildData data) {
		String streamName = Paths.stripPath(data.getPath());
		Map<String, String> map = mapBytesUtility.toMap(data.getData());
		Stream stream = streamFactory.createStream(streamName, map.get("definition"), map);

		try {
			prepareStream(client, stream);
			deployStream(client, stream);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Prepare the new stream for deployment. This updates the ZooKeeper znode
	 * for the stream by adding the following under {@code /xd/streams/[stream-name]}:
	 * <ul>
	 *     <li>{@code .../source/[module-name]}</li>
	 *     <li>{@code .../processor0/[module-name]}</li>
	 *     <li>{@code .../processor1/[module-name]}</li>
	 *     <li>{@code .../sink/[module-name]}</li>
	 * </ul>
	 * The children of these nodes will be ephemeral nodes written by the containers
	 * that accept deployment of the modules.
	 *
	 * @param client  curator client
	 * @param stream  stream to be prepared
	 */
	private void prepareStream(CuratorFramework client, Stream stream) throws Exception {
		// todo: hacking in a "default" set of attributes for the module; these
		// should come from the stream deployment manifest
		Map<String, String> moduleAttributes = new HashMap<String, String>();
		moduleAttributes.put("count", "1");  // only deploy one instance of the module

		for (Iterator<Module> iterator = stream.getDeploymentOrderIterator(); iterator.hasNext();) {
			Module module = iterator.next();
			// TODO: the processors MUST be ordered! They are not right now
			String path = Paths.createPath(Paths.STREAMS, stream.getName(), module.getType().toString(), module.getName());

			try {
				client.create().creatingParentsIfNeeded().forPath(path, mapBytesUtility.toByteArray(moduleAttributes));
			}
			catch (KeeperException.NodeExistsException e) {
				// todo: this would be somewhat unexpected
				LOG.info("Path {} already exists", path);
			}
		}
	}

	/**
	 * Issue deployment requests for the modules of the given stream.
	 *
	 * @param client  curator client
	 * @param stream  stream to be deployed
	 *
	 * @throws Exception
	 */
	private void deployStream(CuratorFramework client, Stream stream) throws Exception {
		for (Iterator<Module> iterator = stream.getDeploymentOrderIterator(); iterator.hasNext();) {
			Module module = iterator.next();

			Container container = matcher.match(module, containerRepository);
			if (container == null) {
				LOG.warn("No container available to deploy module {}", module);
				return;
			}

			LOG.info("Deploying module '{}' to container: {}", module, container);

			String streamName = stream.getName();
			String moduleType = module.getType().toString();
			String moduleName = module.getName();
			String containerName = container.getName();

			Map<String, String> attributes = new HashMap<String, String>();
			attributes.put("stream", streamName);
			attributes.put("type", moduleType);

			try {
				client.create().creatingParentsIfNeeded().forPath(
						Paths.createPath(Paths.DEPLOYMENTS, containerName, moduleName),
						mapBytesUtility.toByteArray(attributes));

				String deploymentStatusPath =
						Paths.createPath(Paths.STREAMS, streamName, moduleType, moduleName, containerName);

				// ensure that the container accepts the module deployment before moving
				// on to the next deployment

				// todo: make timeout configurable
				long timeout = System.currentTimeMillis() + 5000;
				boolean deployed;
				do {
					Thread.sleep(10);
					deployed = client.checkExists().forPath(deploymentStatusPath) != null;
				}
				while (!deployed && System.currentTimeMillis() < timeout);

				if (!deployed) {
					// todo: if the container went away we should select another one to deploy to;
					// otherwise this reflects a bug in the container or some kind of network
					// error in which case the state of deployment is "unknown"
					throw new IllegalStateException(String.format("Deployment of module %s to container %s timed out",
							moduleName, containerName));
				}
			}
			catch (KeeperException.NodeExistsException e) {
				LOG.info("Module {} is already deployed to container {}", module, container);
			}
		}
	}

}
