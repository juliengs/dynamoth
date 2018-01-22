package Dynamoth.Util.Message;

/**
 * Interface of a message handler.
 * An implementor of that method can register with a <code>Reactor</code>
 * @author <a href="mailto:jboula2@cs.mcgill.ca">Jean-Sebastien Boulanger</a>
 *
 */
public interface Handler {
		
	/**
	 * How to handle the message.
	 * @param msg
	 */
	public void handle(Message msg);
		
}
