package zk.node;

import com.oracle.tools.deferred.Eventually;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xdzk.ZooKeeperStandalone;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.oracle.tools.deferred.DeferredHelper.eventually;
import static com.oracle.tools.deferred.DeferredHelper.invoking;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Peralta
 */
public class NodeTest {
	private static final Logger LOG = LoggerFactory.getLogger(NodeTest.class);

	private static final String CONNECT_STRING = "localhost:" + ZooKeeperStandalone.PORT;

	@BeforeClass
	public static void startZooKeeper() throws InterruptedException {
		ZooKeeperStandalone.start();
	}

	@AfterClass
	public static void stopZooKeeper() {
		ZooKeeperStandalone.stop();
	}

	/**
	 * Create a ZooKeeper client. The executing thread blocks until
	 * the connection is made to the server and the client is usable.
	 *
	 * @return a ZooKeeper client
	 *
	 * @throws InterruptedException
	 */
	protected ZooKeeper createClient() throws InterruptedException, TimeoutException {
		try {
			final CountDownLatch latch = new CountDownLatch(1);

			ZooKeeper client = new ZooKeeper(CONNECT_STRING, 2000, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					LOG.info("ZooKeeper event: {}", event);
					if (event.getState() == Event.KeeperState.SyncConnected) {
						latch.countDown();
					}
				}
			});
			if (latch.await(10, TimeUnit.SECONDS)) {
				return client;
			}
			else {
				throw new TimeoutException("Could not connect to ZooKeeper server");
			}
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * todo: double check method name and add doc
	 * @throws Exception
	 */
	@Test
	public void testNode() throws Exception {
		ZooKeeper client = createClient();
		String path = "/test";

		try {
			final Set<String> added = Collections.synchronizedSet(new HashSet<String>());
			final Set<String> removed = Collections.synchronizedSet(new HashSet<String>());
			Node node = new Node(client, path).init();
			node.addListener(new NodeListener() {
				@Override
				public void onChildrenAdded(Set<String> children) {
					added.addAll(children);
				}

				@Override
				public void onChildrenRemoved(Set<String> children) {
					removed.addAll(children);
				}
			});
			client.create(path + "/1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			client.create(path + "/2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			client.create(path + "/3", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			client.create(path + "/4", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			client.delete(path + "/2", -1);
			client.delete(path + "/4", -1);

			Eventually.assertThat(eventually(invoking(node).getChildren()), hasItems("1", "3"));
			assertThat(added, hasItems("1", "2", "3", "4"));
			assertThat(removed, hasItems("2", "4"));
		}
		finally {
			for (String s : client.getChildren(path, false)) {
				client.delete(path + '/' + s, -1);
			}
			client.delete(path, -1);
			client.close();
		}
	}
}
