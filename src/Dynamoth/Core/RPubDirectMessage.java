package Dynamoth.Core;

import java.io.Serializable;

public class RPubDirectMessage extends RPubDataMessage {

	private static final long serialVersionUID = -7339461950787944590L;

	public RPubDirectMessage(RPubNetworkID sourceID, Serializable payload) {
		super(sourceID, payload);
	}

}
