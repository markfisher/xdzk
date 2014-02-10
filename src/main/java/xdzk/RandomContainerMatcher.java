package xdzk;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

/**
 * Implementation of the {@link ContainerMatcher} strategy that selects any one of the candidate containers at random.
 *
 * @author Mark Fisher
 */
public class RandomContainerMatcher implements ContainerMatcher {

	/**
	 * Randomly selects one of the candidate containers.
	 */
	@Override
	public String match(String module, Set<String> candidates) {
		if (candidates == null || candidates.size() == 0) {
			return null;
		}
		int i = new Random().nextInt(candidates.size());
		return new ArrayList<>(candidates).get(i);
	}

}
