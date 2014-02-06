package xdzk;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prototype for a Container node that writes its attributes to an
 * ephemeral znode under {@code /xd/containers/}. The name of that
 * znode matches the UUID generated for this Container instance.
 *
 * @author Mark Fisher
 */
public class Container extends AbstractServer {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(Container.class);

	private final String id;

	public Container(String hostPort) {
		super(hostPort);
		this.id = UUID.randomUUID().toString();
	}

	@Override
	public void doStart() throws Exception {
		ZooKeeper zk = this.getClient();
		Path.CONTAINERS.verify(zk);
		Map<String, String> attributes = new HashMap<>();
		String mxbeanName = ManagementFactory.getRuntimeMXBean().getName();
		String tokens[] = mxbeanName.split("@");
		attributes.put("pid", tokens[0]);
		attributes.put("host", tokens[1]);
		zk.create(Path.CONTAINERS + "/" + id, attributes.toString().getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		LOG.info("started container [" + id + "] with attributes: " + attributes);
	}

	/**
	 * Start a Container node. A ZooKeeper host:port may be optionally
	 * passed in as an argument. The default ZooKeeper host/port is
	 * {@code localhost:2181}.
	 *
	 * @param args command line arguments
	 *
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Container container = new Container(args.length == 1 ? args[0] : "localhost:2181");
		container.run();
	}

}
