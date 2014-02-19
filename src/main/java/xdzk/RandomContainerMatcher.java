package xdzk;

import xdzk.curator.Container;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Implementation of the {@link ContainerMatcher} strategy that selects any one of the candidate containers at random.
 *
 * @author Mark Fisher
 */
public class RandomContainerMatcher implements ContainerMatcher {
	private static final Random RANDOM = new Random();

	/**
	 * Randomly selects one of the candidate containers.
	 */
	@Override
	public Container match(String module, Iterator<Container> candidates) {
		List<Container> containers = new ArrayList<>();
		while (candidates.hasNext()) {
			containers.add(candidates.next());
		}
		return containers.isEmpty() ? null : containers.get(RANDOM.nextInt(containers.size()));
	}

}
