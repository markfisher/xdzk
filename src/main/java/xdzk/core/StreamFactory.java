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

package xdzk.core;

import java.util.Map;

/**
 * @author Patrick Peralta
 */
public class StreamFactory {
	private final ModuleRepository moduleRepository;

	public StreamFactory(ModuleRepository moduleRepository) {
		this.moduleRepository = moduleRepository;
	}

	public Stream createStream(String name, String dsl, Map<String, String> properties) {
		String[] modules = dsl.split("\\|");

		Stream.Builder builder = new Stream.Builder();
		builder.setName(name);
		builder.setProperties(properties);

		for (int i = 0; i < modules.length; i++) {
			Module module = moduleRepository.loadModule(modules[i].trim(),
					i == 0 ? Module.Type.SOURCE
							: i == modules.length - 1 ? Module.Type.SINK
							: Module.Type.PROCESSOR);

			builder.addModule(module);
		}

		return builder.build();
	}

}
