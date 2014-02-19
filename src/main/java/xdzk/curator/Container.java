package xdzk.curator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Domain object for an XD container. This object is typically constructed
 * from container data maintained in ZooKeeper.
 *
 * @author Patrick Peralta
 */
public class Container {
	/**
	 * Container name.
	 */
	private final String name;

	/**
	 * Container attributes.
	 */
	private final Map<String, String> attributes;

	/**
	 * Construct a Container object.
	 *
	 * @param name        container name
	 * @param attributes  container attributes
	 */
	public Container(String name, Map<String, String> attributes) {
		this.name = name;
		this.attributes = Collections.unmodifiableMap(new HashMap<String, String>(attributes));
	}

	/**
	 * Return the container name.
	 *
	 * @return container name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Return the container attributes.
	 *
	 * @return read-only map of container attributes
	 */
	public Map<String, String> getAttributes() {
		return attributes;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Container{" +
				"name='" + name + '\'' +
				", attributes=" + attributes +
				'}';
	}
}
