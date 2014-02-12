package zk.election;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Election Demo:
 *
 * <ol>
 * <li>Start ZooKeeper on localhost with 2181 as client port.</li>
 * <li>Execute the ElectionDemo main method multiple times.</li>
 * <li>Each creates a Candidate and submits a Nomination for that Candidate.</li>
 * <li>One Candidate will be elected.</li>
 * <li>After 10 seconds that Candidate will resign.</li>
 * <li>Shortly after, another instance's Candidate will be elected.</li>
 * <li>The original Candidate's process will terminate.</li>
 * <li>That pattern will continue until all processes terminate and there is no leader.</li>
 * </ol>
 *
 * @author Mark Fisher
 */
public class ElectionDemo {

	private static final Logger LOG = LoggerFactory.getLogger(Nomination.class);

	private static class ShortTermCandidate implements Candidate {

		private final CountDownLatch latch;

		private ShortTermCandidate(int secondsToLead) {
			this.latch = new CountDownLatch(secondsToLead);
		}

		@Override
		public void elect() {
			LOG.info("Elected!");
			while (latch.getCount() > 0) {
				latch.countDown();
				LOG.info("Leading...");
				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
			LOG.info("Resigning!");
		}

		private void awaitResignation() throws InterruptedException {
			this.latch.await();
		}
	}

	public static void main(String[] args) throws IOException {
		Watcher zkWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				LOG.info("ZK event received: " + event);
			}
		};
		ZooKeeper client = new ZooKeeper("localhost:2181", 15000, zkWatcher);
		ShortTermCandidate candidate = new ShortTermCandidate(10);
		Nomination nomination = new Nomination(client, candidate, "/demo/leader");
		nomination.submit();
		try {
			candidate.awaitResignation();
			Thread.sleep(3000);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		LOG.info("Goodbye.");
	}

}
