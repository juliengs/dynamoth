package Dynamoth.Core.Manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubMessageListener;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.RPubSubscribeMessage;
import Dynamoth.Core.RPubSubscriptionMessage;
import Dynamoth.Core.RPubUnsubscribeMessage;
import Dynamoth.Core.Client.RPubClient;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractRPubManager implements RPubManager {

	private RPubNetworkID networkID = null;
	private RPubMessageListener messageListener;
	
	private Set<String> currentSubscriptions = new HashSet<String>();
	
	public AbstractRPubManager(RPubNetworkID networkID, RPubMessageListener messageListener) {
		this.networkID = networkID;
		this.messageListener = messageListener;
	}
	
	public RPubNetworkID getNetworkID() {
		return this.networkID;
	}
	
	public RPubMessageListener getMessageListener() {
		return this.messageListener;
	}
	
	// ============================================
	// Obtaining channel names - can be overridden
	// ============================================

	/**
	 * Returns the name of the broadcast channel
	 * @return
	 */
	public static String getBroadcastChannelName() {
		return "broadcast";
	}
	
	/**
	 * Returns the name of the unicast channel for this network engine
	 * @return
	 */
	public String getUnicastChannelName() {
		return getUnicastChannelName(this.networkID);
	}
	
	/**
	 * Returns the name of the unicast channel for a given network engine
	 * @param networkID
	 * @return
	 */
	public static String getUnicastChannelName(NetworkEngineID networkID) {
		return "unicast" + networkID.hashCode();
	}
	
	/**
	 * Returns the name of the subscription channel for this network engine
	 * @return
	 */
	public String getSubscriptionChannelName() {
		return getSubscriptionChannelName(this.networkID);
	}
	
	/**
	 * Returns the name of the subscription channel for a given network engine
	 * @param networkID
	 * @return
	 */
	public static String getSubscriptionChannelName(NetworkEngineID networkID) {
		return "sub" + networkID.hashCode();
	}
	
	// ============================================
	// Obtaining appropriate shard(s)
	// *** MUST PROVIDE IMPLEMENTATION ***
	// ============================================
	
	// Shard index might not make sense...
	//public abstract void getShardIndex(String channelName);
	
	/**
	 * Get all shards that are to be used for publications for a given channel
	 * Typically, only one shard shall be returned; however, it might be the case that multiple shards
	 * correspond to the same channel.
	 * @param channelName
	 * @return
	 */
	public abstract RPubClient[] getPublicationShards(String channelName);
	
	/**
	 * Get all shards that are to be used for subscriptions for a given channel
	 * Typically, only one shard shall be returned; however, it might be the case that multiple shards
	 * correspond to the same channel.
	 * @param channelName
	 * @return
	 */
	public abstract RPubClient[] getSubscriptionShards(String channelName);
	
	/**
	 * Obtains a list of all currently active shards
	 * Useful shall one wish to register a channel to all active shards (ex: subscription channel)
	 * @return List of all currently active shards 
	 */
	public abstract RPubClient[] getAllActiveShards();
	
	// ============================================
	// Interface implementation
	// ============================================
	
	@Override
	public void subscribeToBasicChannels() {
		Map<RPubClient, List<String>> channelLists = new HashMap<RPubClient, List<String>>();
		
		// Get subscription channels for unicast, broadcast and subscription
		RPubClient[] clients = getAllActiveShards();
		for (RPubClient client : clients) {
			if (channelLists.containsKey(client) == false) {
				channelLists.put(client, new ArrayList<String>());
			}
			channelLists.get(client).add(getSubscriptionChannelName());
		}

		clients = getSubscriptionShards(getUnicastChannelName());
		for (RPubClient client : clients) {
			if (channelLists.containsKey(client) == false) {
				channelLists.put(client, new ArrayList<String>());
			}
			channelLists.get(client).add(getUnicastChannelName());
		}
		
		clients = getSubscriptionShards(getBroadcastChannelName());
		for (RPubClient client : clients) {
			if (channelLists.containsKey(client) == false) {
				channelLists.put(client, new ArrayList<String>());
			}
			channelLists.get(client).add(getBroadcastChannelName());
		}
		
		// Create the channels: private unicast and subscription. Broadcast is already created at some other location in the code.
		createChannel(getUnicastChannelName());
		createChannel(getSubscriptionChannelName());
		
		// Do all subscriptions
		for (Entry<RPubClient, List<String>> entry : channelLists.entrySet()) {
			entry.getKey().subscribeToChannel(entry.getValue().toArray(new String[] {}));
		}
	}
	
	@Override
	public void subscribeClientToChannel(NetworkEngineID clientId, String channelName) {
		// Send subscription to other end
		this.publishToSubscriptionChannel(clientId, new RPubSubscribeMessage((RPubNetworkID)(this.getNetworkID()), (RPubNetworkID)clientId, channelName));
		if (clientId.equals(this.getNetworkID()))
			this.currentSubscriptions.add(channelName);
	}
	
	@Override
	public void subscribeClientToChannel(NetworkEngineID clientId, String channelName, boolean infrastructure) {
		// Send subscription to other end
		this.publishToSubscriptionChannel(clientId, new RPubSubscribeMessage((RPubNetworkID)(this.getNetworkID()), (RPubNetworkID)clientId, channelName, infrastructure));
		if (clientId.equals(this.getNetworkID()))
			this.currentSubscriptions.add(channelName);
	}

	@Override
	public void unsubscribeClientFromChannel(NetworkEngineID clientId, String channelName) {
		// Send unsubscription to other end
		this.publishToSubscriptionChannel(clientId, new RPubUnsubscribeMessage((RPubNetworkID)(this.getNetworkID()), (RPubNetworkID)clientId, channelName));
		if (clientId.equals(this.getNetworkID()))
			this.currentSubscriptions.remove(channelName);
	}
	
	@Override
	public void unsubscribeClientFromChannel(NetworkEngineID clientId, String channelName, boolean infrastructure) {
		// Send unsubscription to other end
		this.publishToSubscriptionChannel(clientId, new RPubUnsubscribeMessage((RPubNetworkID)(this.getNetworkID()), (RPubNetworkID)clientId, channelName, infrastructure));
		if (clientId.equals(this.getNetworkID()))
			this.currentSubscriptions.remove(channelName);
	}
	
	@Override
	public void unsubscribeClientFromAllChannels(NetworkEngineID clientId, RPubClient shard) {
		// Send unsubscription to other end
		this.publishToShards(new RPubClient[] {shard}, getSubscriptionChannelName(clientId), new RPubUnsubscribeMessage((RPubNetworkID)(this.getNetworkID()), (RPubNetworkID)clientId, "", false));
		// Don't remove from list of subscriptions so that reconfiguration works
	}

	@Override
	public void publishToUnicast(NetworkEngineID networkID, RPubMessage message) {
		// Publish to unicast channel(s)
		publishToChannel(getUnicastChannelName(networkID), message);
	}

	@Override
	public void publishToBroadcast(RPubMessage message) {
		// Publish to broadcast channel(s)
		publishToChannel(getBroadcastChannelName(), message);
	}

	@Override
	public void createChannel(String channelName) {
		// Nothing to do under the default implementation.
		// However, implementers could provide additional behavior.
		// For instance, the Dynamoth RPub Manager could inform the LLAs of channel creations 
	}

	public void publishToShards(RPubClient[] shards, String channelName, RPubMessage message) {
		// Publish message to the specified channel on hosts specified in first parameter
		for (RPubClient client: shards) {
			if (prePublishToChannel(client, channelName, message)) {
				client.publishToChannel(channelName, message);
				postPublishToChannel(client, channelName, message);
			}
		}
	}
	
	@Override
	public void publishToChannel(String channelName, RPubMessage message) {
		// Publish message to the specified channel on all appropriate hosts
		publishToShards(this.getPublicationShards(channelName), channelName, message);
	}

	@Override
	public void publishToSubscriptionChannel(NetworkEngineID networkID, RPubSubscriptionMessage message) {
		// Publish to the subscription channel(s)
		String subChannelName = getSubscriptionChannelName(networkID);
		String channelToSubscribe = message.getChannelName();
		//System.out.println("Subscribing " + networkID.toString() + " to " + channelToSubscribe + " via " + subChannelName);
		//channelToSubscribe = subChannelName;
		//channelToSubscribe = "x";
		
		publishToShards(this.getSubscriptionShards(channelToSubscribe), subChannelName, message);
		/*
		if (message instanceof RPubSubscribeMessage)
			publishToShards(this.getSubscriptionShards(channelToSubscribe), subChannelName, message);
		else {
			// Temp: unsubscribe from all shards
			publishToShards(this.getAllActiveShards(), subChannelName, message);
		}
		*/
	}

	/**
	 * Optional method that can be called before publication to a given channel occurs
	 * (Allows implementors to make sure the client is connected for instance or reset
	 * some timeout to maintain some live connection)
	 * @param client RPub Client
	 * @param channelName Channel name
	 * @param message TODO
	 */
	protected boolean prePublishToChannel(RPubClient client, String channelName, RPubMessage message) {
		return true;
	}
	
	/**
	 * Optional method that can be called after publication to a given channel occurs
	 * @param client RPub Client
	 * @param channelName Channel name
	 * @param message TODO
	 */
	protected void postPublishToChannel(RPubClient client, String channelName, RPubMessage message) {
		
	}
	
	/**
	 * Obtains the list of current subscriptions (accross all servers)
	 * 
	 * @return List of all current subscriptions
	 */
	protected Set<String> getCurrentSubscriptions() {
		return this.currentSubscriptions;
	}
}
