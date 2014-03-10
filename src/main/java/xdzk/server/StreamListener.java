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

import xdzk.cluster.Container;
import xdzk.cluster.ContainerRepository;
import xdzk.core.MapBytesUtility;
import xdzk.core.ModuleDescriptor;
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
	 * Utility to convert maps to byte arrays.
	 */
	private final MapBytesUtility mapBytesUtility = new MapBytesUtility();

	/**
	 * Stream factory.
	 */
	private final StreamFactory streamFactory;


	/**
	 * Construct a StreamListener.
	 *
	 * @param containerRepository repository to obtain container data
	 * @param moduleRepository    repository to obtain module data
	 */
	public StreamListener(ContainerRepository containerRepository, ModuleRepository moduleRepository) {
		this.containerRepository = containerRepository;
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
				onChildAdded(client, event.getData());
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
	private void onChildAdded(CuratorFramework client, ChildData data) {
		String streamName = Paths.stripPath(data.getPath());
		Map<String, String> map = mapBytesUtility.toMap(data.getData());
		Stream stream = streamFactory.createStream(streamName, map);

		LOG.info("Deploying stream {}", stream);

		try {
			prepareStream(client, stream);
			deployStream(client, stream);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (Exception e) {
			throw e instanceof RuntimeException ? ((RuntimeException) e) : new RuntimeException(e);
		}
	}

	/**
	 * Prepare the new stream for deployment. This updates the ZooKeeper znode
	 * for the stream by adding the following under {@code /xd/streams/[stream-name]}:
	 * <ul>
	 *     <li>{@code .../source/[module-name.module-label]}</li>
	 *     <li>{@code .../processor/[module-name.module-label]}</li>
	 *     <li>{@code .../sink/[module-name.module-label]}</li>
	 * </ul>
	 * The children of these nodes will be ephemeral nodes written by the containers
	 * that accept deployment of the modules.
	 *
	 * @param client  curator client
	 * @param stream  stream to be prepared
	 */
	private void prepareStream(CuratorFramework client, Stream stream) throws Exception {
		for (Iterator<ModuleDescriptor> iterator = stream.getDeploymentOrderIterator(); iterator.hasNext();) {
			ModuleDescriptor module = iterator.next();
			String streamName = stream.getName();
			String moduleType = module.getModule().getType().toString();
			String moduleName = module.getModule().getName();
			String moduleLabel = module.getLabel();

			String path = Paths.build(Paths.STREAMS, streamName,
					moduleType, String.format("%s.%s", moduleName, moduleLabel));

			try {
				client.create().creatingParentsIfNeeded().forPath(path);
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
		for (Iterator<ModuleDescriptor> iterator = stream.getDeploymentOrderIterator(); iterator.hasNext();) {
			ModuleDescriptor module = iterator.next();
			String streamName = stream.getName();
			String moduleType = module.getModule().getType().toString();
			String moduleName = module.getModule().getName();
			String moduleLabel = module.getLabel();
			Map<Container, String> mapDeploymentStatus = new HashMap<Container, String>();

			for (Container container : stream.getContainerMatcher().match(module, containerRepository)) {
				String containerName = container.getName();
				try {
					client.create().creatingParentsIfNeeded().forPath(
							Paths.build(Paths.DEPLOYMENTS, containerName,
									String.format("%s.%s.%s.%s", streamName, moduleType, moduleName, moduleLabel)));

					mapDeploymentStatus.put(container,
							Paths.build(Paths.STREAMS, streamName, moduleType,
									String.format("%s.%s", moduleName, moduleLabel), containerName));
				}
				catch (KeeperException.NodeExistsException e) {
					LOG.info("Module {} is already deployed to container {}", module, container);
				}
			}

			// wait for all deployments to succeed
			// todo: make timeout configurable
			long timeout = System.currentTimeMillis() + 30000;
			do {
				for (Iterator<Map.Entry<Container, String>> iteratorStatus = mapDeploymentStatus.entrySet().iterator();
					 iteratorStatus.hasNext();) {
					Map.Entry<Container, String> entry = iteratorStatus.next();
					if (client.checkExists().forPath(entry.getValue()) != null) {
						iteratorStatus.remove();
					}
					Thread.sleep(10);
				}
			}
			while (!mapDeploymentStatus.isEmpty() && System.currentTimeMillis() < timeout);

			if (!mapDeploymentStatus.isEmpty()) {
				// todo: if the container went away we should select another one to deploy to;
				// otherwise this reflects a bug in the container or some kind of network
				// error in which case the state of deployment is "unknown"
				throw new IllegalStateException(String.format(
						"Deployment of module %s to the following containers timed out: %s",
						moduleName, mapDeploymentStatus.keySet()));
			}
		}
	}

}
