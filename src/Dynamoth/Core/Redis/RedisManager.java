package Dynamoth.Core.Redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;

import Dynamoth.Client.Client;
import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubMessageListener;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Util.Collection.CollectionUtils;
import Dynamoth.Util.Properties.PropertyManager;

public class RedisManager {

	// NetworkEngineID
	private RPubNetworkID networkID =  null;
	
	// Redis nodes
	private List<String> redisHosts = new ArrayList<String>();
	// Redis clients
	private List<RedisClient> redisClients = new ArrayList<RedisClient>();
	
	public static AtomicInteger[] hashShardCount = new AtomicInteger[] {
		new AtomicInteger(0), new AtomicInteger(0),
		new AtomicInteger(0), new AtomicInteger(0),
		new AtomicInteger(0), new AtomicInteger(0),
		new AtomicInteger(0), new AtomicInteger(0)};
	
	public List<RedisClient> getRedisClients() {
		return redisClients;
	}

	private RPubMessageListener messageListener;
	
	// Connect to redis
	public RedisManager(RPubNetworkID networkID, RPubMessageListener messageListener) {
		this.networkID = networkID;
		this.messageListener = messageListener;
		
		// Read properties file
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		
		String rawServers = StringUtils.strip(
				props.getProperty("network.rpub.servers"));

		for (String server: rawServers.split(";")) {
			redisHosts.add(server);
		}
	}
	
	private String getJedisHostName(String server) {
		return server.split(":")[0];
	}
	
	private int getJedisHostPort(String server) {
		String[] serverInfo = server.split(":");
		if (serverInfo.length > 1) {
			// Hostname and port
			return Integer.parseInt(serverInfo[1]);
		}
		else {
			// Default port
			return 6379;
		}
	}
	
	public int getHashedShardIndex(String channelName) {
		// Get shard info
		
		int hashCode = channelName.hashCode();
		if (hashCode < 0)
			hashCode = -hashCode;
		
		int shard = hashCode % redisHosts.size();
		//hashShardCount[shard].incrementAndGet();
		
		//System.out.println("Sharding index: " + (hashCode % redisHosts.size()) );
		return shard;
		//return hashCode % (redisHosts.size()-1) + 1;
	}
	
	private RedisClient getHashedShard(String channelName) {
		return redisClients.get(getHashedShardIndex(channelName));
	}
	
	/**
	 * Create and connect all Jedis nodes
	 */
	public void initialize() {

		// Create and connect all Jedis nodes
		int index=0;
		for (String host: redisHosts) {
			RedisClient client = new RedisClient(networkID, index, 1, getJedisHostName(host), getJedisHostPort(host), messageListener, this);
			client.connect();
			redisClients.add(client);
			index++;
		}
	}
	
	/**
	 * Create a jedis channel
	 * @param channelName Name of channel
	 */
	public synchronized void createChannel(String channelName) {
		getHashedShard(channelName).createChannel(channelName);
	}
	
	public synchronized void publishToUnicast(NetworkEngineID networkID, RPubMessage message) {
		// Publish to one unicast channel
		//publishToChannel(getUnicastChannelName(networkID), message);
		
		// Publish to a random unicast channel
		int clientId = CollectionUtils.random.nextInt(redisClients.size());
		redisClients.get(clientId).publishToChannel(getUnicastChannelName(networkID), message);
	}
	
	public synchronized void publishToBroadcast(RPubMessage message) {
		// Single channel
		//publishToChannel(getBroadcastChannelName(), message);
		
		// Random channel
		int clientId = CollectionUtils.random.nextInt(redisClients.size());
		redisClients.get(clientId).publishToChannel(getBroadcastChannelName(), message);
	}
	
	public synchronized void publishToChannel(String channelName, String message) {
		// Single channel		
		//getHashedShard(channelName).publishToChannel(channelName, message);
		
		// Random channel
		int clientId = CollectionUtils.random.nextInt(redisClients.size());
		redisClients.get(clientId).publishToChannel(channelName, message);
	}
	
	public synchronized void publishToChannel(String channelName, RPubMessage message) {
		//System.out.println("Publishing to " + channelName + " on " + getHashedShardIndex(channelName));
		// Single channel
		//getHashedShard(channelName).publishToChannel(channelName, message);
		
		// Random channel
		int clientId = CollectionUtils.random.nextInt(redisClients.size());
		redisClients.get(clientId).publishToChannel(channelName, message);
	}
	
	public synchronized void publishToSubscriptionChannel(NetworkEngineID networkID, RPubMessage message) {
		//System.out.println("Publishing to Subscription channel (" + ((RPubSubscribeMessage)message).getChannelName() + ") for " + networkID);
		for (RedisClient client: redisClients) {
			client.publishToChannel(getSubscriptionChannelName(networkID), message);
		}
	}
	
	public synchronized void subscribeToBasicChannels() {
		// Get hashed shard for broadcast
		Map<RedisClient, List<String>> channelLists = new HashMap<RedisClient, List<String>>();
		
		for (RedisClient client: redisClients) {
			channelLists.put(client, new ArrayList<String>());
		}
		
		//channelLists.get(getHashedShard(getBroadcastChannelName())).add(getBroadcastChannelName());
		// Subscribe to only one unicast channel...
		//channelLists.get(getHashedShard(getUnicastChannelName())).add(getUnicastChannelName());
		
		for (RedisClient client: redisClients) {
			channelLists.get(client).add(getSubscriptionChannelName());
			// Subscribe to unicast channels on all hosts...
			channelLists.get(client).add(getUnicastChannelName());
			channelLists.get(client).add(getBroadcastChannelName());
		}
		
		// Do all subscriptions
		for (RedisClient client: redisClients) {
			client.subscribeToChannel(channelLists.get(client).toArray(new String[] {}));
		}
	}
	
	public synchronized void subscribeToBroadcast() {
		subscribeToChannel(getBroadcastChannelName());
	}
	
	public synchronized void subscribeToUnicast() {
		subscribeToChannel(getUnicastChannelName());
	}
	
	public synchronized void subscribeToSubscriptionChannel() {
		// Special case: all RedisClients should "subscribe" to this channel
		// which will only be used for subscriptions
		for (RedisClient client: redisClients) {
			client.subscribeToChannel(getSubscriptionChannelName());
		}
		//subscribeToChannel(getSubscriptionChannelName());
	}
	
	public synchronized void subscribeToChannel(final String channelName) { 
		//System.out.println("Subscribing to " + channelName + " on " + getHashedShardIndex(channelName));
		// Subscribe on one node
		//getHashedShard(channelName).subscribeToChannel(channelName);
		
		// Subscribe on all nodes
		for (RedisClient client: redisClients) {
			client.subscribeToChannel(channelName);
		}
	}
	
	public synchronized void unsubscribeFromChannel(String channelName) {
		//System.out.println("Unsubscribing to " + channelName + " on " + getHashedShardIndex(channelName));
		//getHashedShard(channelName).unsubscribeFromChannel(channelName);
		
		// Unsubscribe from all nodes
		for (RedisClient client: redisClients) {
			client.unsubscribeFromChannel(channelName);
		}
	}
	
	public String getBroadcastChannelName() {
		return "broadcast";
	}
	
	public String getUnicastChannelName() {
		return getUnicastChannelName(this.networkID);
	}
	
	public String getUnicastChannelName(NetworkEngineID networkID) {
		return "unicast" + networkID.hashCode();
	}
	
	public String getSubscriptionChannelName() {
		return getSubscriptionChannelName(networkID);
	}
	
	public String getSubscriptionChannelName(NetworkEngineID networkID) {
		return "sub" + networkID.hashCode();
	}
}
