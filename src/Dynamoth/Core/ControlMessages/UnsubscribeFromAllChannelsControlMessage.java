package Dynamoth.Core.ControlMessages;

import Dynamoth.Core.Client.RPubClientId;

/**
 *
 * @author Julien Gascon-Samson
 */
public class UnsubscribeFromAllChannelsControlMessage extends ControlMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 822793896281234544L;
	
	private RPubClientId clientId;

	public UnsubscribeFromAllChannelsControlMessage(RPubClientId clientId) {
		this.clientId = clientId;
	}
	
	public RPubClientId getClientId() {
		return clientId;
	}
}
