package xdzk;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class AdminServer extends AbstractServer {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);

	enum LeadershipStatus {
		REQUESTING,
		ELECTED,
		NOT_ELECTED
	}

	/**
	 * Current status of this AdminServer instance.
	 */
	private volatile LeadershipStatus status;

	/**
	 * Watcher instance that watches the {@code /xd/admin} znode path.
	 */
	private final AdminPathWatcher adminPathWatcher = new AdminPathWatcher();

	/**
	 * Callback instance that is invoked to process the result of an attempt
	 * to write the {@code /xd/admin} znode, i.e. a request to assume leadership.
	 */
	private final LeadershipRequestCallback leadershipRequestCallback = new LeadershipRequestCallback();

	/**
	 * Callback instance that is invoked to process the result of
	 * getting data from the {@code /xd/admin} znode.
	 */
	private final LeadershipCheckCallback leadershipCheckCallback = new LeadershipCheckCallback();

	/**
	 * Callback instance that is invoked to process the result of
	 * checking the existence of the {@code /xd/admin} znode.
	 */
	private final LeaderExistsCallback leaderExistsCallback = new LeaderExistsCallback();

	/**
	 * Watcher instance that watches the {@code /xd/container} znode path.
	 */
	private final ContainerPathWatcher containerPathWatcher = new ContainerPathWatcher();

	/**
	 * Callback instance that is invoked to process the result of
	 * {@link ZooKeeper#getChildren} on the {@code /xd/container} znode.
	 */
	private final ContainerPathCallback containerPathCallback = new ContainerPathCallback();

	/**
	 * Watcher instance that watches the {@code /xd/streams} znode path.
	 */
	private final StreamPathWatcher streamPathWatcher = new StreamPathWatcher();

	/**
	 * Callback instance that is invoked to process the result of
	 * {@link ZooKeeper#getChildren} on the {@code /xd/streams} znode.
	 */
	private final StreamPathCallback streamPathCallback = new StreamPathCallback();

	/**
	 * Callback instance that is invoked to process the result of
	 * {@link ZooKeeper#getData} on a child of {@code /xd/streams}.
	 */
	private final StreamDeploymentCallback streamDeploymentCallback = new StreamDeploymentCallback();

	// TODO: make this pluggable
	private final ContainerMatcher containerMatcher = new RandomContainerMatcher();

	/**
	 * Set of current container paths under {@code /xd/container}.
	 */
	// Marked as volatile because this reference is updated by the
	// ZK event dispatch thread and is read by public method getContainerPaths
	private volatile Set<String> containerPaths = Collections.emptySet();

	/**
	 * Set of current stream paths under {@code /xd/streams}.
	 */
	// Marked as volatile because this reference is updated by the
	// ZK event dispatch thread and is read by public method getStreamPaths
	private volatile Set<String> streamPaths = Collections.emptySet();


	/**
	 * Singleton instance of the Admin server.
	 */
	// Marked as volatile because this reference is updated by the
	// main thread and is read by the CurrentContainers callable.
	public static volatile AdminServer INSTANCE;


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
		return containerPaths;
	}

	/**
	 * Obtain the set of current stream paths.
	 *
	 * @return read-only set of stream paths
	 */
	public Set<String> getStreamPaths() {
		return streamPaths;
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
		catch (KeeperException | UnsupportedEncodingException e) {
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
	 * Start the Admin server.
	 *
	 * @throws InterruptedException
	 */
	@Override
	protected void doStart() throws InterruptedException {
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
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			// todo: we may be in an inconsistent state if interrupted;
			// not sure what to do here, but returning before anything else
			return;
		}
		requestLeadership();
		watchContainers();
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
	 * Asynchronously request leadership by attempting to write to {@code /xd/admin}.
	 *
	 * @see xdzk.AdminServer.LeadershipRequestCallback
	 */
	private void requestLeadership() {
		getClient().create(Path.ADMIN.toString(), this.getId().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, leadershipRequestCallback, null);
	}

	/**
	 * Asynchronously obtain a list of children under {@code /xd/containers}.
	 *
	 * @see xdzk.AdminServer.ContainerPathCallback
	 */
	protected void watchContainers() {
		getClient().getChildren(Path.CONTAINERS.toString(), containerPathWatcher, containerPathCallback, null);
	}

	/**
	 * Asynchronously obtain a list of children under {@code /xd/streams}.
	 *
	 * @see xdzk.AdminServer.StreamPathWatcher
	 * @see xdzk.AdminServer.StreamPathCallback
	 */
	private void watchStreamDeploymentRequests() {
		getClient().getChildren(Path.STREAMS.toString(), streamPathWatcher, streamPathCallback, null);
	}

	/**
	 * Callback method that is invoked when the current Admin server instance
	 * has been elected to take leadership.
	 */
	private void takeLeadership() {
		LOG.info("Leader Admin {} is watching for stream deployment requests.", getId());
		watchStreamDeploymentRequests();
	}

	/**
	 * Asynchronously attempt to get data from the {@code /xd/admin} znode.
	 *
	 * @see xdzk.AdminServer.LeadershipCheckCallback
	 */
	private void checkLeadership() {
		getClient().getData(Path.ADMIN.toString(), false, leadershipCheckCallback, null);
	}

	/**
	 * Asynchronously check for the existence of the {@code /xd/admin} znode.
	 *
	 * @see xdzk.AdminServer.AdminPathWatcher
	 * @see xdzk.AdminServer.LeaderExistsCallback
	 */
	private void leaderExists() {
		getClient().exists(Path.ADMIN.toString(), adminPathWatcher, leaderExistsCallback, null);
	}

	/**
	 * Deploy a stream for the deployment request with the given stream name under {@code /xd/streams}.
	 *
	 * @param streamName the name of the stream to deploy
	 *
	 * @see xdzk.AdminServer.StreamDeploymentCallback
	 */
	private void deployStream(String streamName) {
		getClient().getData(Path.STREAMS.toString() + "/" + streamName, false, streamDeploymentCallback, null);
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
		String container = this.containerMatcher.match(module, getContainerPaths());
		LOG.info("deploying module '{}' to container: {}", module, container);
		try {
			getClient().create(Path.DEPLOYMENTS + "/" + container + "/" + module, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
	 * Callback implementation that is invoked upon reading the {@code /xd/admin} znode.
	 */
	class LeadershipCheckCallback implements AsyncCallback.DataCallback {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkLeadership();
				return;
			case NONODE:
				requestLeadership();
				return;
			default:
				break;
			}
		}
	}

	/**
	 * Watcher implementation that watches the {@code /xd/admin} znode path.
	 */
	class AdminPathWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			if (EventType.NodeDeleted == event.getType()
					&& Path.ADMIN.toString().equals(event.getPath())) {
				requestLeadership();
			}
		}
	};

	/**
	 * Callback implementation that is invoked upon writing to the {@code /xd/admin} znode.
	 */
	class LeadershipRequestCallback implements AsyncCallback.StringCallback {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			LOG.info(">> LeaderCallback result: {}, {}, {}, {}", rc, path, ctx, name);
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkLeadership();
				return;
			case OK:
				status = LeadershipStatus.ELECTED;
				takeLeadership();
				break;
			case NODEEXISTS:
				status = LeadershipStatus.NOT_ELECTED;
				leaderExists();
				break;
			default:
				status = LeadershipStatus.NOT_ELECTED;
				LOG.error("Something went wrong requesting leadership.",
						KeeperException.create(Code.get(rc), path));
			}
			LOG.info("Admin {} status: {}", getId(), status);
		}
	}

	/**
	 * Callback implementation that is invoked when checking the
	 * existence of the {@code /xd/admin} znode.
	 */
	class LeaderExistsCallback implements AsyncCallback.StatCallback {

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				leaderExists();
				break;
			case OK:
				if (stat == null) {
					status = LeadershipStatus.REQUESTING;
					requestLeadership();
				}
				break;
			default:
				checkLeadership();
				break;
			}
		}
	}

	/**
	 * Watcher implementation that watches the {@code /xd/container} znode path.
	 */
	class ContainerPathWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			LOG.info(">> ContainerPathWatcher event: {}", event);
			watchContainers();
		}
	}

	/**
	 * Callback implementation that is invoked to process the result of
	 * {@link ZooKeeper#getChildren} on the {@code /xd/container} znode.
	 */
	class ContainerPathCallback implements AsyncCallback.ChildrenCallback {
		/**
		 * {@inheritDoc}
		 * <p>
		 * This callback updates the {@link #containerPaths} set with the latest
		 * known container paths.
		 */
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			LOG.info(">> ContainerPathCallback result: {}, {}, {}, {}", rc, path, ctx, children);

			Set<String> arrived = new HashSet<>();
			Set<String> departed = new HashSet<>();
			if (children == null) {
				children = Collections.emptyList();
			}

			for (String child : children) {
				if (!containerPaths.contains(child)) {
					arrived.add(child);
				}
			}

			Set<String> newPaths = Collections.unmodifiableSet(new HashSet<>(children));
			for (String child : containerPaths) {
				if (!newPaths.contains(child)) {
					departed.add(child);
				}
			}

			containerPaths = newPaths;

			// todo: consider a pluggable listener for new and departed containers

			LOG.info("New containers:      {}", arrived);
			LOG.info("Departed containers: {}", departed);
			LOG.info("All containers:      {}", containerPaths);
		}
	}

	/**
	 * Watcher implementation that watches the {@code /xd/streams} znode path.
	 */
	class StreamPathWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			LOG.info(">> StreamPathWatcher event: {}", event);
			if (LeadershipStatus.ELECTED == status) {
				watchStreamDeploymentRequests();
			}
		}
	}

	/**
	 * Callback implementation that is invoked to process the result of
	 * {@link ZooKeeper#getChildren} on the {@code /xd/streams} znode.
	 */
	class StreamPathCallback implements AsyncCallback.ChildrenCallback {
		/**
		 * {@inheritDoc}
		 * <p>
		 * This callback reads stream deployment requests and delegates
		 * to the container matcher to determine where each module of a
		 * stream should be deployed. It then writes the module deployment
		 * requests into the {@code /xd/deployments} znode.
		 */
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			LOG.info(">> StreamPathCallback result: {}, {}, {}, {}", rc, path, ctx, children);

			Set<String> arrived = new HashSet<>();
			Set<String> departed = new HashSet<>();
			if (children == null) {
				children = Collections.emptyList();
			}

			for (String child : children) {
				if (!streamPaths.contains(child)) {
					arrived.add(child);
				}
			}

			Set<String> newPaths = Collections.unmodifiableSet(new HashSet<>(children));
			for (String child : streamPaths) {
				if (!newPaths.contains(child)) {
					departed.add(child);
				}
			}

			streamPaths = newPaths;

			// todo: consider a pluggable listener for new and departed streams

			LOG.info("New streams:      {}", arrived);
			LOG.info("Departed streams: {}", departed);
			LOG.info("All streams:      {}", streamPaths);

			for (String streamName : arrived) {
				deployStream(streamName);
			}
		}
	}

	/**
	 * Callback implementation that is invoked upon reading the {@code /xd/streams} child
	 * znode for a given stream deployment request.
	 */
	class StreamDeploymentCallback implements AsyncCallback.DataCallback {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				// TODO: replace with Assert.isInstanceOf once we depend on Spring
				if (!(ctx instanceof String)) {
					throw new IllegalArgumentException("Expected stream name in context object, but received: " + ctx);
				}
				deployStream((String) ctx);
				return;
			case NONODE:
				// TODO: stream deployment request was deleted, ignore?
				return;
			default:
				deployModules(new String(data).split("\\|"));
				break;
			}
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
