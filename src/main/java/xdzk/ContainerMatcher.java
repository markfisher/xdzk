package xdzk;

import java.util.Set;

/**
 * Strategy interface for matching a ModuleDeploymentRequest to one of the candidate container nodes.
 *
 * @author Mark Fisher
 */
public interface ContainerMatcher {

	/**
	 * Matches the provided module against one of the candidate containers.
	 *
	 * @param moduleDeploymentRequest the module deployment request
	 * @param candidates set of containers to which a module could be deployed
	 * @return the matched container, or <code>null</code> if no suitable match
	 */
	// TODO: The module name should be replaced with ModuleDeploymentRequest which will
	// include the criteria for matching against container attributes as well as other
	// metadata extracted from the stream deployment manifest, such as the number of instances.
	// Also, the String return and collection element type should be Container instances
	// where ultimately matching will take Container attributes and metrics into consideration.
	String match(String module, Set<String> candidates);

}
