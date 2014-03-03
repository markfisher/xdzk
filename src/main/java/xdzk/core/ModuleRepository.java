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

/**
 * Interface definition for a repository that contains {@link Module modules}.
 *
 * @author Patrick Peralta
 */
public interface ModuleRepository {
	/**
	 * Load the requested module.
	 *
	 * @param name  module name
	 * @param type  module type
	 *
	 * @return the requested module
	 */
	Module loadModule(String name, Module.Type type);
}
