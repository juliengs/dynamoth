package Dynamoth.Mammoth.NetworkEngine;

import java.io.Serializable;

/**
 * NetworkMessage is the interface that messages of Mammoth that are sent over
 * the network have to respect. It is mainly used to encapsulate a serializable
 * and the sender of the message.
 * 
 * @version Nov 2, 2007
 * @author Dominik Zindel
 * 
 */
public interface NetworkMessage extends Serializable {

	/**
	 * This method gets the {@link NetworkEngineID} of the sender of the
	 * message.
	 * 
	 * @return The unique identifier of the originator of the message.
	 */
	NetworkEngineID getSender();

	/**
	 * This method gets the encapsulated serializable, that is the actual
	 * message.
	 * 
	 * @return A serializable representing the actual message sent over the
	 *         network.
	 */
	Serializable getSerializable();
}
