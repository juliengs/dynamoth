package Dynamoth.Core.Manager;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import Dynamoth.Client.Client;
import Dynamoth.Core.RPubMessageListener;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.JedisRPubClient;
import Dynamoth.Core.Client.RPubClient;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Util.RPubUtil;
import Dynamoth.Util.Properties.PropertyManager;

public class HashedRPubManager extends AbstractRPubManager {

	// Redis nodes
	private List<String> redisHosts = new ArrayList<String>();
	// List of Redis clients
	private List<JedisRPubClient> redisClients = new ArrayList<JedisRPubClient>();
	
	public HashedRPubManager(RPubNetworkID networkID, RPubMessageListener messageListener) {
		super(networkID, messageListener);
		// Read properties file
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		
		String rawServers = StringUtils.strip(
				props.getProperty("network.rpub.servers"));

		for (String server: rawServers.split(";")) {
			redisHosts.add(server);
		}
	}
	
	public int getHashedShardIndex(String channelName) {
		// Get shard info
		// TODO: use the new ShardHasher...
		
		int hashCode = channelName.hashCode();
		if (hashCode < 0)
			hashCode = -hashCode;
		
		int shard = hashCode % redisHosts.size();
		//hashShardCount[shard].incrementAndGet();
		
		//System.out.println("Sharding index: " + (hashCode % redisHosts.size()) );
		return shard;
		//return hashCode % (redisHosts.size()-1) + 1;
	}
	
	private RPubClient getHashedShard(String channelName) {
		return this.redisClients.get(getHashedShardIndex(channelName));
	}

	@Override
	public void initialize() {
		// Create and connect all Jedis nodes
		for (String host: redisHosts) {
			JedisRPubClient client = new JedisRPubClient(this.getNetworkID(), RPubClientId.generate(), 1, RPubUtil.parseRPubHostName(host), RPubUtil.parseRPubHostPort(host), RPubUtil.parseRPubHostDomain(host), this.getMessageListener());
			client.connect();
			redisClients.add(client);
		}
	}

	@Override
	public RPubClient[] getPublicationShards(String channelName) {
		// Get the shards that shall be used for publication messages
		// Under the Hashed Model, the shard corresponding to -channelName- shall be used
		
		return new RPubClient[] { this.getHashedShard(channelName) };
	}

	@Override
	public RPubClient[] getSubscriptionShards(String channelName) {
		// Get the shards that shall be used for subscription messages
		// Under the Hashed Model, the shard corresponding to -channelName- shall be used
		// (same as publication shard)
		
		return new RPubClient[] { this.getHashedShard(channelName) };
	}

	@Override
	public RPubClient[] getAllActiveShards() {
		// Returns all active shards
		
		return this.redisClients.toArray(new RPubClient[] {}); 
	}

}
