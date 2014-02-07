package xdzk;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

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

	/**
	 * Construct a Container.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public Container(String hostPort) {
		super(hostPort);
	}

	@Override
	public void doStart() throws Exception {
		ZooKeeper zk = this.getClient();
		Path.CONTAINERS.verify(zk);

		Map<String, String> attributes = new HashMap<>();
		String mxBeanName = ManagementFactory.getRuntimeMXBean().getName();
		String tokens[] = mxBeanName.split("@");
		attributes.put("pid", tokens[0]);
		attributes.put("host", tokens[1]);

		zk.create(Path.CONTAINERS + "/" + this.getId(), attributes.toString().getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		LOG.info("started container {} with attributes: {} ", this.getId(), attributes);
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
		new Container(args.length == 1 ? args[0] : "localhost:2181").run();
	}

}
