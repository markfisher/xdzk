package xdzk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dumping ground for quick prototype testing (i.e. testing that has not
 * yet migrated to the formal testing package).
 *
 * @author Patrick Peralta
 */
public class Main {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	/**
	 * Main method.
	 *
	 * @param args program arguments
	 *
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		LOG.info(">>> Starting server");
		ZooKeeperEmbedded.start();
		LOG.info(">>> Started server");

		Thread.sleep(5000);

		LOG.info(">>> Stopping server");
		ZooKeeperEmbedded.stop();
		LOG.info(">>> Stopped server");

		Thread.sleep(10000);

		LOG.info(">>> Restarting server");
		ZooKeeperEmbedded.start();
		LOG.info(">>> Restarted server");

		Thread.sleep(5000);

		LOG.info(">>> Stopping server");
		ZooKeeperEmbedded.stop();
		LOG.info(">>> Stopped server");
	}
}
