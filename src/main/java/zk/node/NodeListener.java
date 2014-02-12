package zk.node;

import java.util.Set;

/**
 * Callback interface for receiving notifications of additions and
 * removals of node children.
 *
 * @see Node#addListener
 * @see Node#removeListener
 *
 * @author Patrick Peralta
 */
public interface NodeListener {
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
