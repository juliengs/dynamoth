package Dynamoth.Core;

public class RPubSubscriptionMessage extends RPubMessage {

	private static final long serialVersionUID = -4364318241891601779L;

	private RPubNetworkID targetID = null;
	private String 		  channelName = "";
	
	private boolean infrastructure = false;

	public RPubSubscriptionMessage(RPubNetworkID sourceID, RPubNetworkID targetID, String channelName) {
		this(sourceID, targetID, channelName, false);
	}
	
	public RPubSubscriptionMessage(RPubNetworkID sourceID, RPubNetworkID targetID, String channelName, boolean infrastructure) {
		super(sourceID);
		this.targetID = targetID;
		this.channelName  = channelName;
		this.infrastructure = infrastructure;
	}
	
	/**
	 * The target of the subscription which is the network ID that must be registered/unregistered for the specified channel
	 * @return Target of the subscription
	 */
	public RPubNetworkID getTargetID() {
		return this.targetID;
	}

	/**
	 * Channel name for that subscription
	 * @return Channel name
	 */
	public String getChannelName() {
		return this.channelName;
	}

	/**
	 * An infrastructure subscription is a subscription which does not originate from the underlying game (Mammoth)
	 * It comes from within the internal RPub engine (ie moving a subscription from one server to the other)
	 * @return True if the subscription originates from the underlying game; otherwise False
	 */
	public boolean isInfrastructure() {
		return infrastructure;
	}
}
