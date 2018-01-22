package Dynamoth.Core.ControlMessages;

import Dynamoth.Core.Client.RPubClientId;

public class RemoveRPubClientControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6019478507997543331L;

	private RPubClientId clientId;
	
	protected RemoveRPubClientControlMessage() {
	}
	
	public RemoveRPubClientControlMessage(RPubClientId clientId) {
		this.clientId = clientId;
	}

	public RPubClientId getClientId() {
		return clientId;
	}
}
