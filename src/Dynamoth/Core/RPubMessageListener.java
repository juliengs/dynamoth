package Dynamoth.Core;

public interface RPubMessageListener {
	void messageReceived(String channelName, RPubMessage message, int rawMessageSize);
}
