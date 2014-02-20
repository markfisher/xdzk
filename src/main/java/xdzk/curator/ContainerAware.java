package xdzk.curator;

import java.util.Iterator;

/**
 * Interface definition for an object with the ability to look up
 * the set of containers available in the cluster.
 *
 * @author Patrick Peralta
 */
public interface ContainerAware {
	/**
	 * Return an {@link Iterator} over the available containers.
	 *
	 * @return iterator over the available containers
	 */
	Iterator<Container> getContainerIterator();
}
