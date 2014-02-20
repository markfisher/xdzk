package xdzk;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zk.election.Candidate;
import zk.election.Nomination;
import zk.node.Node;
import zk.node.NodeListener;

/**
 * Prototype implementation of an XD admin server that watches ZooKeeper
 * for Container arrivals and departures from the XD cluster. Each AdminServer
 * instance will attempt to request leadership, but at any given time only
 * one AdminServer instance in the cluster will have leadership status. Those
 * instances not elected will watch the {@code /xd/admin} znode so that one
 * of them will take over leadership if the leader admin closes or crashes.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 */
public class AdminServer extends AbstractServer implements Candidate {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);

	/**
	 * Singleton instance of the Admin server.
	 */
	// Marked as volatile because this reference is updated by the
	// main thread and is read by the CurrentContainers callable.
	public static volatile AdminServer INSTANCE;

	/**
	 * Node used to track containers in the same cluster as this admin.
	 */
	// Marked as volatile because this reference is updated by the
	// ZK event dispatch thread and is read by public method getContainerPaths
	private volatile Node containers;

	/**
	 * {@link zk.node.NodeListener} implementation that handles container additions and removals.
	 */
	private final NodeListener containerListener = new ContainerListener();

	/**
	 * Node used to track stream deployment requests.
	 */
	// Marked as volatile because this reference is updated by the
	// ZK event dispatch thread and is read by public method getStreamPaths
	private volatile Node streams;

	/**
	 * {@link zk.node.NodeListener} implementation that handles stream additions and removals.
	 */
	private final NodeListener streamListener = new StreamListener();

	// TODO: make this pluggable
	private final ContainerMatcher containerMatcher = new RandomContainerMatcher();

	/**
	 * Admin constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public AdminServer(String hostPort) {
		super(hostPort);
	}

	/**
	 * Obtain the set of current container paths.
	 *
	 * @return read-only set of container paths
	 */
	public Set<String> getContainerPaths() {
		return containers.getChildren();
	}

	/**
	 * Obtain the set of current stream paths.
	 *
	 * @return read-only set of stream paths
	 */
	public Set<String> getStreamPaths() {
		return streams.getChildren();
	}

	/**
	 * Processes SyncConnected WatchedEvents from the ZooKeeper client.
	 * When received, requests leadership and establishes a watch for container nodes.
	 */
	@Override
	protected void onConnect(WatchedEvent event) {
		LOG.info("Admin {} CONNECTED", this.getId());
		ZooKeeper client = this.getClient();
		try {
			Path.CONTAINERS.verify(client);
			Path.STREAMS.verify(client);
			Path.DEPLOYMENTS.verify(client);
			containers = new Node(client, Path.CONTAINERS.toString());
			containers.addListener(containerListener);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			// todo: we may be in an inconsistent state if interrupted;
			// not sure what to do here, but returning before anything else
			return;
		}
		Nomination nomination = new Nomination(client, this, Path.ADMIN.toString());
		nomination.setData(this.getId().getBytes());
		nomination.submit();
	}

	/**
	 * Processes Disconnected WatchedEvents from the ZooKeeper client.
	 */
	@Override
	protected void onDisconnect(WatchedEvent event) {
		// TODO: stop watching for stream deployment requests if leader
		// or is what we do in StreamPathWatcher sufficient?
	}

	/**
	 * Callback method that is invoked when the current Admin server instance
	 * has been elected to take leadership.
	 */
	@Override
	public void lead() {
		LOG.info("Leader Admin {} is watching for stream deployment requests.", getId());
		// TODO: consider a resign() method and a latch
		try {
			streams = new Node(getClient(), Path.STREAMS.toString());
			streams.addListener(streamListener);
			// for now, we're just hanging out here to maintain leadership
			Thread.sleep(Long.MAX_VALUE);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		streams.removeListener(streamListener);
	}

	/**
	 * Handle a stream deployment request. Upon completion of this
	 * method, the request for deployment is persisted. However, the
	 * actual deployment of the stream is executed in the background.
	 * <p>
	 * Implementation consists of writing the stream deployment
	 * request under the {@link Path#STREAMS} znode.
	 *
	 * @param name        stream name
	 * @param definition  stream definition (pipe delimited list of modules)
	 */
	public void handleStreamDeployment(String name, String definition) {
		// TODO: improve parameter validation
		if (name == null) {
			throw new IllegalArgumentException("name must not be null");
		}
		if (definition == null) {
			throw new IllegalArgumentException("definition must not be null");
		}
		try {
			getClient().create(Path.STREAMS.toString() + '/' + name, definition.getBytes("UTF-8"),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		catch (KeeperException.NodeExistsException e) {
			// TODO: this could occur if the REST client issues the same request
			// multiple times; should this be ignored or should there be a
			// response indicating a duplicate request?
		}
		catch (KeeperException e) {
			throw new RuntimeException(e);
		}
		catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			// TODO: if the thread is interrupted we have no idea if the
			// request succeeded; what kind of response should be sent?
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deploy a stream for the deployment request with the given stream name under {@code /xd/streams}.
	 *
	 * @param streamName the name of the stream to deploy
	 *
	 * @see xdzk.AdminServer.StreamDeploymentCallback
	 */
	private void deployStream(String streamName) {
		Node streamNode = new Node(getClient(), Path.STREAMS.toString() + "/" + streamName);
		byte[] data = streamNode.getData();
		deployModules(new String(data).split("\\|"));
	}

	/**
	 * Deploy the provided modules which have been parsed from a stream definition.
	 *
	 * @param modules the modules to be deployed
	 */
	private void deployModules(String[] modules) {
		// start from the sink, so that startup order is reversed
		for (int i = modules.length - 1; i >= 0; i--) {
			deployModule(modules[i].trim());
		}
	}

	/**
	 * Deploy the provided module to a container based on the result of the {@link ContainerMatcher}.
	 *
	 * @param module the name of the module to be deployed
	 */
	private void deployModule(String module) {
		// Since the interface definition for ContainerMatcher changed to require
		// an Iterator<Container>, just going to skip ContainerMatcher usage
		// and simply select the first container path found.
		String container = getContainerPaths().iterator().next(); // TODO: REVISIT

		LOG.info("deploying module '{}' to container: {}", module, container);
		try {
			getClient().create(Path.DEPLOYMENTS + "/" + container + "/" + module, null,
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		catch (KeeperException.NodeExistsException e) {
			// TODO: what could cause a repeat deployment?
		}
		catch (KeeperException e) {
			throw new RuntimeException("failed to deploy module '" + module + "' to container: " + container, e);
		}
		catch (InterruptedException e) {
			// TODO: resubmit?
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * {@link zk.node.NodeListener} implementation that handles container additions and removals.
	 */
	class ContainerListener implements NodeListener {

		@Override
		public void onDataUpdated(byte[] oldData, byte[] newData) {
			LOG.info("Container data update: old={}, new={}", oldData, newData);
		}

		@Override
		public void onChildrenAdded(Set<String> children) {
			LOG.info("Containers added: {}", children);
		}

		@Override
		public void onChildrenRemoved(Set<String> children) {
			LOG.info("Containers removed: {}", children);
		}
	}

	/**
	 * {@link zk.node.NodeListener} implementation that handles stream deployment requests.
	 * <p/>
	 * When children are added, this listener initiates the handling of each stream deployment
	 * request, ultimately delegating to the container matcher to determine where each module of a
	 * stream should be deployed, then writing the module deployment requests into the
	 * {@code /xd/deployments} znode.
	 */
	class StreamListener implements NodeListener {

		@Override
		public void onDataUpdated(byte[] oldData, byte[] newData) {
			LOG.info("Stream data update: old={}, new={}", oldData, newData);
		}

		@Override
		public void onChildrenAdded(Set<String> children) {
			LOG.info("Streams added: {}", children);
			for (String streamName : children) {
				deployStream(streamName);
			}
		}

		@Override
		public void onChildrenRemoved(Set<String> children) {
			LOG.info("Streams removed: {}", children);
		}
	}

	/**
	 * Callable implementation that returns the known container paths.
	 */
	public static class CurrentContainers implements Callable<Collection<String>>, Serializable {
		private static final long serialVersionUID = 0L;

		@Override
		public Collection<String> call() throws Exception {
			return INSTANCE.getContainerPaths();
		}
	}

	/**
	 * Callable implementation that requests a stream deployment.
	 */
	public static class StreamDeploymentRequest implements Callable<Void>, Serializable {
		private static final long serialVersionUID = 0L;

		/**
		 * Stream name.
		 */
		private final String name;

		/**
		 * Stream definition.
		 */
		private final String definition;

		/**
		 * Construct a StreamDeploymentRequest.
		 *
		 * @param name        stream name
		 * @param definition  stream definition (pipe delimited list of modules)
		 */
		public StreamDeploymentRequest(String name, String definition) {
			this.name = name;
			this.definition = definition;
		}

		@Override
		public Void call() throws Exception {
			INSTANCE.handleStreamDeployment(name, definition);
			return null;
		}
	}

	/**
	 * Start an Admin server. A ZooKeeper host:port may be optionally
	 * passed in as an argument. The default ZooKeeper host/port is
	 * {@code localhost:2181}.
	 *
	 * @param args command line arguments
	 *
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		INSTANCE = new AdminServer(args.length == 1 ? args[0] : "localhost:2181");
		INSTANCE.run();
	}

}
