package zk.node;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdzk.ContainerServer;

/**
 * Implementation of {@link NodeListener} that simply logs.
 * May be subclassed when only a subset of the callback methods are needed.
 *
 * @author Mark Fisher
 */
public class NodeListenerAdapter implements NodeListener {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(ContainerServer.class);

	@Override
	public void onDataUpdated(byte[] oldData, byte[] newData) {
		LOG.info("Data updated: old={}, new={}", oldData, newData);
	}

	@Override
	public void onChildrenAdded(Set<String> children) {
		LOG.info("Children added: {}", children);
	}

	@Override
	public void onChildrenRemoved(Set<String> children) {
		LOG.info("Children removed: {}", children);
	}

}
