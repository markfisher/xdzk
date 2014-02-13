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
