package zk.election;

/**
 * Interface for a candidate that can be nominated as a leader.
 *
 * @author Mark Fisher
 */
public interface Candidate {

	/**
	 * Method invoked if this candidate should assume leadership.
	 * Do not exit this method until relinquishing leadership.
	 */
	void lead();

}
