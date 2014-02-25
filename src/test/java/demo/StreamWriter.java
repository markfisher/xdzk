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

package demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdzk.curator.Paths;

/**
 * @author Mark Fisher
 */
public class StreamWriter {

	private static final Logger LOG = LoggerFactory.getLogger(StreamWriter.class);

	public static void main(String[] args) throws Exception {
		ZooKeeper zk = new ZooKeeper("localhost:2181", 15000, new ZkWatcher());
		createStream(zk, "time2log", "time | log");
		Thread.sleep(3000);
		createStream(zk, "foo2bar", "foo | bar");
		Thread.sleep(3000);
		createStream(zk, "tcp2hdfs", "tcp | hdfs");
	}

	private static void createStream(ZooKeeper client, String name, String definition) {
		try {
			client.create(Paths.STREAMS + '/' + name, definition.getBytes("UTF-8"),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static class ZkWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			LOG.info("ZooKeeper event: " + event);
		}
	}

}
