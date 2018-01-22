package Dynamoth.Core;

public class RPubUnsubscribeMessage extends RPubSubscriptionMessage {

	private static final long serialVersionUID = -5949811207653527192L;

	public RPubUnsubscribeMessage(RPubNetworkID sourceID, RPubNetworkID targetID, String channelName) {
		super(sourceID, targetID, channelName);
	}
	
	public RPubUnsubscribeMessage(RPubNetworkID sourceID, RPubNetworkID targetID, String channelName, boolean infrastructure) {
		super(sourceID, targetID, channelName, infrastructure);
	}

}
