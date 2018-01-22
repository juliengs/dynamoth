package Dynamoth.Core;

public class RPubSubscribeMessage extends RPubSubscriptionMessage {

	private static final long serialVersionUID = -4282807049094745772L;

	public RPubSubscribeMessage(RPubNetworkID sourceID, RPubNetworkID targetID, String channelName) {
		super(sourceID, targetID, channelName);
	}
	
	public RPubSubscribeMessage(RPubNetworkID sourceID, RPubNetworkID targetID, String channelName, boolean infrastructure) {
		super(sourceID, targetID, channelName, infrastructure);
	}
}
