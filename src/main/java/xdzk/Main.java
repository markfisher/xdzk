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
