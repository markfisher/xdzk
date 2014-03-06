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

import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author Patrick Peralta
 */
public class StreamFactory {
	private final ModuleRepository moduleRepository;

	public StreamFactory(ModuleRepository moduleRepository) {
		this.moduleRepository = moduleRepository;
	}

	public Stream createStream(String name, Map<String, String> properties) {
		Assert.hasText(name, "Stream name is required");

		String definition = properties.get("definition");
		Assert.hasText(definition, "Stream deployment manifest requires a 'definition' property");

		String[] modules = definition.split("\\|");
		Stream.Builder builder = new Stream.Builder();
		builder.setName(name);
		if (properties != null) {
			builder.setProperties(properties);
		}

		for (int i = 0; i < modules.length; i++) {
			String moduleDefinition = modules[i].trim();
			String moduleName;
			String label;

			// TODO: naive parsing, the following formats are supported
			// source | processor | sink
			// source | p1: processor | p2: processor | sink
			// where p1 is the alias
			if (moduleDefinition.contains(":")) {
				String[] split = moduleDefinition.split("\\:");
				label = split[0].trim();
				moduleName = split[1].trim();
			}
			else {
				moduleName = moduleDefinition;
				label = String.format("%s-%d", moduleName, i);
			}

			Module module = moduleRepository.loadModule(moduleName,
					i == 0 ? Module.Type.SOURCE
							: i == modules.length - 1 ? Module.Type.SINK
							: Module.Type.PROCESSOR);

			builder.addModule(module, label);
		}

		return builder.build();
	}

}
