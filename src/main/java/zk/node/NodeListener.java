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

package zk.node;

import java.util.Set;

/**
 * Callback interface for receiving notifications of additions and
 * removals of node children as well as updates to node data.
 *
 * @see Node#addListener
 * @see Node#removeListener
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 */
public interface NodeListener {

	/**
	 * Invoked upon data being set on a {@link Node}.
	 *
	 * @param oldData previous data as byte array
	 * @param newData updated data as byte array
	 */
	void onDataUpdated(byte[] oldData, byte[] newData);

	/**
	 * Invoked upon the addition of children to a {@link Node}.
	 *
	 * @param children set of children added to a node
	 */
	void onChildrenAdded(Set<String> children);

	/**
	 * Invoked upon the removal of children from a {@link Node}.
	 *
	 * @param children set of children removed from a node
	 */
	void onChildrenRemoved(Set<String> children);

}
