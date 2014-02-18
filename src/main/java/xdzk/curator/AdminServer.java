package xdzk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xdzk.ContainerMatcher;
import xdzk.RandomContainerMatcher;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Prototype implementation of an XD admin server that watches ZooKeeper
 * for Container arrivals and departures from the XD cluster. Each AdminServer
 * instance will attempt to request leadership, but at any given time only
 * one AdminServer instance in the cluster will have leadership status. Those
 * instances not elected will watch the {@link Paths#ADMIN} znode so that one
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

	/**
	 * Singleton instance of the Admin server.
	 */
	// volatile because this reference is updated by the
	// main thread and is read by the CurrentContainers callable.
	public static volatile AdminServer INSTANCE;

	/**
	 * Cache of children under the containers path. This path is used to track
	 * containers in the cluster.
	 */
	// volatile because this reference is updated by the Curator event dispatch
    // thread and read by public method getContainerPaths
	private volatile PathChildrenCache containers;

	/**
	 * Listener implementation that handles container additions and removals.
	 */
	private final PathChildrenCacheListener containerListener = new ContainerListener();

	/**
	 * Cache of children under the streams path. This path is used to track stream
	 * deployment requests.
	 */
	// volatile because this reference is written by the Curator thread that
	// handles leader election and read by public method getStreamPaths
	// todo: perhaps this does not need to be a member since getStreamPaths
	// isn't invoked at the moment
	private volatile PathChildrenCache streams;

	/**
	 * Leader selector to elect admin server that will handle stream
	 * deployment requests.
	 */
	// volatile because this reference is written and read by the Curator
	// event dispatch threads - there is no guarantee that the same thread
	// will do the reading and writing
	private volatile LeaderSelector leaderSelector;

	/**
	 * Listener that is invoked when this admin server is elected leader.
	 */
	private final LeaderSelectorListener leaderListener = new LeaderListener();

	/**
	 * Container matcher used to match containers to module deployments.
	 */
	// TODO: make this pluggable
	private final ContainerMatcher containerMatcher = new RandomContainerMatcher();


	/**
	 * Server constructor.
	 *
	 * @param hostPort host name and port number in the format {@code host:port}.
	 */
	public AdminServer(String hostPort) {
		super(hostPort);
	}

	/**
	 * Return the set of current container paths.
	 *
	 * @return read-only set of container paths
	 */
	public Set<String> getContainerPaths() {
		// todo: instead of returning a set here, perhaps
		// we can return Iterator<String>
		Set<String> containerSet = new HashSet<>();
		for (ChildData child : containers.getCurrentData()) {
			containerSet.add(child.getPath());
		}
		return Collections.unmodifiableSet(containerSet);
	}

	/**
	 * Return the set of current stream paths.
	 *
	 * @return read-only set of stream paths
	 */
	public Set<String> getStreamPaths() {
		// todo: instead of returning a set here, perhaps
		// we can return Iterator<String>
		Set<String> streamSet = new HashSet<>();
		for (ChildData child : streams.getCurrentData()) {
			streamSet.add(child.getPath());
		}
		return Collections.unmodifiableSet(streamSet);
	}

	/**
	 * Return the configured {@link xdzk.ContainerMatcher}. This matcher
	 * is used to match a container to a module deployment request.
	 *
	 * @return container matcher for this admin server
	 */
	public ContainerMatcher getContainerMatcher() {
		return containerMatcher;
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Upon connection, the admin server starts to listen for containers joining
	 * and leaving the cluster. It also attempts to take the leadership role
	 * for managing stream deployment requests.
	 */
	@Override
	protected void onConnect(ConnectionState newState) {
		LOG.info("Admin {} CONNECTED", this.getId());

		try {
			CuratorFramework client = getClient();

			Paths.ensurePath(client, Paths.DEPLOYMENTS);
			Paths.ensurePath(client, Paths.CONTAINERS);
			Paths.ensurePath(client, Paths.STREAMS);

			containers = new PathChildrenCache(client, Paths.CONTAINERS, false);
			containers.getListenable().addListener(containerListener);
			containers.start();

			// todo: LeaderSelector does not make use of namespaces :(
			leaderSelector = new LeaderSelector(client, '/' + Paths.XD_NAMESPACE + '/' + Paths.ADMIN, leaderListener);
			leaderSelector.setId(getId());
			leaderSelector.start();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Upon disconnect, the {@link #leaderSelector} is closed and the container
	 * listener is removed.
	 */
	protected void onDisconnect(ConnectionState newState) {
		try {
			leaderSelector.close();

			containers.getListenable().removeListener(containerListener);
			containers.close();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Handle a stream deployment request. Upon completion of this
	 * method, the request for deployment is persisted. However, the
	 * actual deployment of the stream is executed in the background.
	 * <p>
	 * Implementation consists of writing the stream deployment
	 * request under the {@link xdzk.Path#STREAMS} znode.
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
			getClient().create().forPath(Paths.STREAMS + '/' + name, definition.getBytes("UTF-8"));
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


	/**
	 * Listener implementation that is invoked when containers are added/removed/modified.
	 */
	class ContainerListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			String child = Paths.stripPath(event.getData().getPath());
			switch (event.getType()) {
				case CHILD_ADDED:
					LOG.info("Container added: {}", child);
					break;
				case CHILD_UPDATED:
					LOG.info("Container updated: ", child);
					break;
				case CHILD_REMOVED:
					LOG.info("Container removed: {}", child);
				case CONNECTION_SUSPENDED:
					break;
				case CONNECTION_RECONNECTED:
					break;
				case CONNECTION_LOST:
					break;
				case INITIALIZED:
					break;
			}
		}
	}

	/**
	 * Listener implementation that is invoked when this server becomes the leader.
	 */
	class LeaderListener extends LeaderSelectorListenerAdapter {

		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			LOG.info("Leader Admin {} is watching for stream deployment requests.", getId());
			PathChildrenCacheListener streamListener = new StreamListener(AdminServer.this);

			try {
				streams = new PathChildrenCache(client, Paths.STREAMS, false);
				streams.getListenable().addListener(streamListener);
				streams.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

				Thread.sleep(Long.MAX_VALUE);
			}
			finally {
				streams.getListenable().removeListener(streamListener);
				streams.close();
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
