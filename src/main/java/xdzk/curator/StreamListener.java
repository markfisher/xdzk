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

package xdzk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdzk.ContainerMatcher;

/**
 * Listener implementation that handles stream deployment requests.
 *
* @author Patrick Peralta
*/
class StreamListener implements PathChildrenCacheListener {
	/**
	 * Logger.
	 */
	private final Logger LOG = LoggerFactory.getLogger(StreamListener.class);

	/**
	 * Provides access to the current container list.
	 */
	private final ContainerAware containerAware;

	/**
	 * Matches a deployment request to a container.
	 */
	private final ContainerMatcher matcher;

	/**
	 * Construct a StreamListener.
	 *
	 * @param containerAware admin server that this listener is attached to
	 */
	public StreamListener(ContainerAware containerAware, ContainerMatcher matcher) {
		this.containerAware = containerAware;
		this.matcher = matcher;

	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Handle child events for the {@link Paths#STREAMS} path.
	 */
	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		switch (event.getType()) {
			case CHILD_ADDED:
				deployStream(client, Paths.stripPath(event.getData().getPath()));
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
	 * Deploy a stream for the deployment request with the given stream name under {@link Paths#STREAMS}.
	 *
	 * @param client     curator client
	 * @param streamName the name of the stream to deploy
	 *
	 */
	private void deployStream(CuratorFramework client, String streamName) {
		LOG.info("Stream added: {}", streamName);
		try {
			deployModules(client, new String(client.getData().forPath(Paths.STREAMS + '/' + streamName)).split("\\|"));
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deploy the provided modules which have been parsed from a stream definition.
	 *
	 * @param client     curator client
	 * @param modules the modules to be deployed
	 */
	private void deployModules(CuratorFramework client, String[] modules) throws Exception {
		// start from the sink, so that startup order is reversed
		for (int i = modules.length - 1; i >= 0; i--) {
			deployModule(client, modules[i].trim());
		}
	}

	/**
	 * Deploy the provided module to a container based on the result of the {@link ContainerMatcher}.
	 *
	 * @param client     curator client
	 * @param module the name of the module to be deployed
	 */
	private void deployModule(CuratorFramework client, String module) throws Exception {
		Container container = matcher.match(module, containerAware.getContainerIterator());
		if (container == null) {
			LOG.info("No container available to deploy module {}", module);
			return;
		}

		LOG.info("Deploying module '{}' to container: {}", module, container);

		try {
		// todo: we need to specify whether container will contain the leading slash
//		client.create().creatingParentsIfNeeded().forPath(Paths.DEPLOYMENTS + '/' + container + '/' + module);
			client.create().creatingParentsIfNeeded().forPath(Paths.DEPLOYMENTS + container + '/' + module);
		}
		catch (KeeperException.NodeExistsException e) {
			LOG.info("Module {} is already deployed to container {}", module, container);
		}
	}

}
