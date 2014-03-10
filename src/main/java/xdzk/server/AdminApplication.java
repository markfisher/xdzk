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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import xdzk.core.MapBytesUtility;
import xdzk.core.ModuleRepository;
import xdzk.core.StubModuleRepository;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * Application context class for {@link AdminServer}.
 *
 * @author Patrick Peralta
 */
@Configuration
public class AdminApplication {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AdminApplication.class);

	/**
	 * The {@link ApplicationContext} for the admin server.
	 */
	private static volatile ApplicationContext applicationContext;

	/**
	 * Environment for this application.
	 */
	@Autowired
	Environment env;

	/**
	 * Bean definition for singleton instance of {@link MapBytesUtility}.
	 *
	 * @return new instance of MapBytesUtility
	 */
	@Bean
	public MapBytesUtility mapBytesUtility() {
		return new MapBytesUtility();
	}

	/**
	 * Bean definition for singleton instance of {@link ModuleRepository}.
	 *
	 * @return new instance of ModuleRepository
	 */
	@Bean
	public ModuleRepository moduleRepository() {
		return new StubModuleRepository();
	}

	/**
	 * Bean definition for singleton instance of {@link AdminServer}.
	 *
	 * @return new instance of AdminServer
	 */
	@Bean
	public AdminServer containerServer() {
		return new AdminServer(env.getProperty("zk", "localhost:2181"), mapBytesUtility(), moduleRepository());
	}

	/**
	 * Start an Admin server. A ZooKeeper host:port may be optionally
	 * passed in as an argument using flag "--zk" as in {@code --zk=localhost:9999}.
	 * The default ZooKeeper host/port is {@code localhost:2181}.
	 *
	 * @param args command line arguments
	 *
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		SpringApplication application = new SpringApplication(AdminApplication.class);
		application.addListeners(new AdminApplicationListener());
		application.run(args);
		Thread.sleep(Long.MAX_VALUE);
	}

	/**
	 * Application lifecycle listener.
	 */
	public static class AdminApplicationListener implements ApplicationListener<ContextRefreshedEvent> {

		@Override
		public void onApplicationEvent(ContextRefreshedEvent event) {
			LOG.trace("Application event: {}", event);

			// if the application context is used before refresh
			// an IllegalStateException is thrown; therefore
			// don't hold the application context until the first refresh
			if (applicationContext == null) {
				applicationContext = event.getApplicationContext();
			}
		}
	}

	/**
	 * Callable implementation that returns the known container paths.
	 */
	public static class CurrentContainers implements Callable<Collection<String>>, Serializable {
		private static final long serialVersionUID = 0L;

		@Override
		public Collection<String> call() throws Exception {
			try {
				return applicationContext == null
						? Collections.<String>emptyList()
						: applicationContext.getBean(AdminServer.class).getContainerPaths();
			}
			catch (RuntimeException e) {
				LOG.error("Error invoking method on application context",  e);
				throw e;
			}
		}
	}

}
