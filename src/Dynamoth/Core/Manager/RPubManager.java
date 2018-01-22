package Dynamoth.Core.Manager;

import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Core.Client.RPubClient;
import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubSubscriptionMessage;

public interface RPubManager {

	/**
	 * Performs some initializations (connect to nodes, etc.)
	 */
	void initialize();
	
	/**
	 * Subscribes a given client to a given channel
	 * @param clientId Client
	 * @param channelName Channel
	 */
	void subscribeClientToChannel(NetworkEngineID clientId, String channelName);
	
	/**
	 * Subscribes a given client to a given channel
	 * @param clientId Client
	 * @param channelName Channel
	 * @param infrastructure Infrastructure mode
	 */
	void subscribeClientToChannel(NetworkEngineID clientId, String channelName, boolean infrastructure);
	
	/**
	 * Unsubscribes a given client from a given channel
	 * @param clientId Client
	 * @param channelName Channel
	 */
	void unsubscribeClientFromChannel(NetworkEngineID clientId, String channelName);
	
	/**
	 * Unsubscribes a given client from a given channel
	 * @param clientId Client
	 * @param channelName Channel
	 * @param infrastructure Infrastructure mode
	 */
	void unsubscribeClientFromChannel(NetworkEngineID clientId, String channelName, boolean infrastructure);
	
	/**
	 * Unsubscribes a given client from all channels on a given shard
	 * @param clientId Client
	 * @param shard Shard
	 */
	void unsubscribeClientFromAllChannels(NetworkEngineID clientId, RPubClient shard);
	
	/**
	 * Subscribe to basic channels such as broadcast
	 */
	void subscribeToBasicChannels();
	
	/**
	 * Publish to a networkid's private unicast channel
	 * 
	 * @param networkID
	 * @param message
	 */
	void publishToUnicast(NetworkEngineID networkID, RPubMessage message);
	
	/**
	 * Publish to the broadcast channel
	 * 
	 * @param message
	 */
	void publishToBroadcast(RPubMessage message);
	
	/**
	 * Create a channel
	 * 
	 * @param channelName
	 */
	void createChannel(String channelName);
	
	/**
	 * Publish to a given channel
	 * 
	 * @param channelName
	 * @param message
	 */
	void publishToChannel(String channelName, RPubMessage message);
	
	/**
	 * Publish to a networkid's private subscription channel
	 * 
	 * @param networkID
	 * @param message
	 */
	void publishToSubscriptionChannel(NetworkEngineID networkID, RPubSubscriptionMessage message);
}
