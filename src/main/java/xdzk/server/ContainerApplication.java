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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import xdzk.cluster.ContainerMatcher;
import xdzk.cluster.RandomContainerMatcher;
import xdzk.core.MapBytesUtility;
import xdzk.core.ModuleRepository;
import xdzk.core.StubModuleRepository;

/**
 * Application context class for {@link ContainerServer}.
 *
 * @author Patrick Peralta
 */
@Configuration
public class ContainerApplication {
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
	 * Bean definition for singleton instance of {@link ContainerMatcher}.
	 *
	 * @return new instance of ContainerMatcher
	 */
	@Bean
	public ContainerMatcher containerMatcher() {
		return new RandomContainerMatcher();
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
	 * Bean definition for singleton instance of {@link ContainerServer}.
	 *
	 * @return new instance of AdminServer
	 */
	@Bean
	public ContainerServer containerServer() {
		return new ContainerServer(env.getProperty("zk", "localhost:2181"), mapBytesUtility(), moduleRepository());
	}

	/**
	 * Start a Container server. A ZooKeeper host:port may be optionally
	 * passed in as an argument using flag "--zk" as in {@code --zk=localhost:9999}.
	 * The default ZooKeeper host/port is {@code localhost:2181}.
	 *
	 * @param args command line arguments
	 *
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		SpringApplication.run(ContainerApplication.class, args);
		Thread.sleep(Long.MAX_VALUE);
	}

}
