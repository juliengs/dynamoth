package Dynamoth.Core.ControlMessages.Debug;

import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.ControlMessages.ControlMessage;

public class SubscribedToChannelControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6933702576727293926L;
	private RPubNetworkID networkId;

	public SubscribedToChannelControlMessage(RPubNetworkID networkId) {
		this.networkId = networkId;
	}

	public RPubNetworkID getNetworkId() {
		return networkId;
	}

	
}
