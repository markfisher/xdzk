package demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdzk.AdminServerTest;
import xdzk.Path;

/**
 * @author Mark Fisher
 */
public class StreamWriter {

	private static final Logger LOG = LoggerFactory.getLogger(AdminServerTest.class);

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
			client.create(Path.STREAMS.toString() + '/' + name, definition.getBytes("UTF-8"),
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
