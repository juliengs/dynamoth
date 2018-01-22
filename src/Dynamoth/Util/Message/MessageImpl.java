package Dynamoth.Util.Message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This is the base Message implementation
 * 
 * @author Jean-Sebastien Boulanger
 * @date Jan 17, 2006
 *
 */
public abstract class MessageImpl implements Message {

	private static final long serialVersionUID = 1L;

	public MessageImpl() { /* For externalizable */ }

	public String toString() { 
		String s = "Generic toString for MessageImpl : " + this.getClass().getName();
		return s;
	}

	/**
	 * @throws IOException Subclasses can throw exceptions.
	 */
	public void writeExternal(ObjectOutput out) throws IOException {
	}

	/**
	 * @throws IOException Subclasses can throw exceptions.
	 * @throws ClassNotFoundException Subclasses can throw exceptions.
	 */
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	}
}
