package xdzk.curator;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.springframework.core.convert.converter.Converter;

import java.util.Iterator;

/**
 * Iterator over {@link ChildData} instances that are managed by {@link PathChildrenCache}.
 * A {@link Converter} is used to convert the node data into a domain object.
 *
 * @param <T> the domain class type returned by this iterator
 *
 * @author Patrick Peralta
 */
public class ChildPathIterator<T> implements Iterator<T> {
	/**
	 * Converter from {@link ChildData} to domain class type {@link T}.
	 */
	private final Converter<ChildData, T> converter;

	/**
	 * The actual iterator over the {@link ChildData}.
	 */
	private final Iterator<ChildData> iterator;

	/**
	 * Construct a ChildPathIterator.
	 *
	 * @param converter  converter from node data to domain object
	 * @param cache      source for children nodes
	 */
	public ChildPathIterator(Converter<ChildData, T> converter, PathChildrenCache cache) {
		this.converter = converter;
		this.iterator = cache.getCurrentData().iterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T next() {
		return converter.convert(iterator.next());
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Implementation throws {@link UnsupportedOperationException} because
	 * removals are not supported.
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
