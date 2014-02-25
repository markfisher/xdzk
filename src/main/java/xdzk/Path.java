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

package xdzk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * Enumeration of ZooKeeper paths used by the system.
 *
 * @author Mark Fisher
 */
public enum Path {

	/**
	 * Base path where admin nodes compete for leadership.
	 */
	ADMIN("/admin", CreateMode.EPHEMERAL),

	/**
	 * Base path where container nodes register themselves.
	 */
	CONTAINERS("/containers", CreateMode.PERSISTENT),

	/**
	 * Base path where an admin node sends deployment requests.
	 */
	DEPLOYMENTS("/deployments", CreateMode.PERSISTENT),

	/**
	 * Base path where an admin node writes stream deployment
	 * requests.
	 */
	STREAMS("/streams", CreateMode.PERSISTENT);

	/**
	 * Root node for all XD nodes.
	 */
	private static final String ROOT = "/xd";

	/**
	 * Full path for ZooKeeper znode.
	 */
	private final String absolute;

	/**
	 * Create mode for znode path.
	 */
	private final CreateMode createMode;

	/**
	 * Creates a Path relative to the root, with the provided CreateMode.
	 *
	 * @param relative path relative to the root node
	 * @param createMode ZooKeeper createMode to use
	 */
	Path(String relative, CreateMode createMode) {
		this.absolute = ROOT + relative;
		this.createMode = createMode;
	}

	/**
	 * Verifies the existence of this path, else creates any proper subset of it as needed.
	 *
	 * @param zk the ZooKeeper instance
	 *
	 * @throws InterruptedException
	 */
	public void verify(ZooKeeper zk) throws InterruptedException {
		// Assert.notNull(zk, "ZooKeeper must not be null");
		try {
			if (zk.exists(this.absolute, false) == null) {
				String traversed = "/";
				String path = this.absolute;
				if (path.startsWith("/")) {
					path = path.substring(1);
				}
				String[] nodes = path.split("/");
				for (String node : nodes) {
					String current = traversed + node;
					if (zk.exists(current, false) == null) {
						zk.create(current, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, this.createMode);
					}
					traversed = current + "/";
				}
			}
		}
		catch (KeeperException.NodeExistsException e) {
			// Assume this means that another member of the cluster
			// is creating the same path
		}
		catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns the absolute path as a String.
	 */
	@Override
	public String toString() {
		return this.absolute;
	}

}
