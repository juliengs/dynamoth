package Dynamoth.Core.Client;

import Dynamoth.Core.RPubMessage;

public interface RPubClient {
	
	/**
	 * Retrieves an unique identification number corresponding to this RPubClient instance
	 * @return Unique identification number
	 */
	RPubClientId getId();
	
	/**
	 * Connect to Redis
	 */
	void connect();
	
	/**
	 * Disconnect from Redis
	 */
	void disconnect();
	
	/**
	 * Determines if this client is connected
	 * @return TODO
	 */
	boolean isConnected();
	
	/**
	 * Create channel
	 */
	void createChannel(String channelName);
	
	/**
	 * Publish to channel
	 */
	void publishToChannel(String channelName, RPubMessage message);
	void publishToChannel(String channelName, String message);
	
	/**
	 * Subscribe to channel
	 */
	void subscribeToChannel(final String... channelName);
	
	/**
	 * Unsubscribe from channel
	 */
	void unsubscribeFromChannel(final String channelName);
}
