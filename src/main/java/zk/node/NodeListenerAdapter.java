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

package zk.node;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdzk.ContainerServer;

/**
 * Implementation of {@link NodeListener} that simply logs.
 * May be subclassed when only a subset of the callback methods are needed.
 *
 * @author Mark Fisher
 */
public class NodeListenerAdapter implements NodeListener {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(ContainerServer.class);

	@Override
	public void onDataUpdated(byte[] oldData, byte[] newData) {
		LOG.info("Data updated: old={}, new={}", oldData, newData);
	}

	@Override
	public void onChildrenAdded(Set<String> children) {
		LOG.info("Children added: {}", children);
	}

	@Override
	public void onChildrenRemoved(Set<String> children) {
		LOG.info("Children removed: {}", children);
	}

}
