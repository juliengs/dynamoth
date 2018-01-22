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
import Dynamoth.Util.Collection.CollectionUtils;
import Dynamoth.Util.Properties.PropertyManager;

public class FullyReplicatedRPubManager extends AbstractRPubManager {

	// Redis nodes
	private List<String> redisHosts = new ArrayList<String>();
	// List of Redis clients
	private List<JedisRPubClient> redisClients = new ArrayList<JedisRPubClient>();
	
	public FullyReplicatedRPubManager(RPubNetworkID networkID, RPubMessageListener messageListener) {
		super(networkID, messageListener);
		// Read properties file
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		
		String rawServers = StringUtils.strip(
				props.getProperty("network.rpub.servers"));

		for (String server: rawServers.split(";")) {
			redisHosts.add(server);
		}
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
		// Under the Fully Replicated Model, one random shard shall be chosen
		
		int clientId = CollectionUtils.random.nextInt(redisClients.size());
		return new RPubClient[] { this.redisClients.get(clientId) };
	}

	@Override
	public RPubClient[] getSubscriptionShards(String channelName) {
		// Get the shards that shall be used for subscription messages
		// Under the Fully Replicated Model, all shards shall be chosen
		
		return this.redisClients.toArray(new RPubClient[] {});
	}

	@Override
	public RPubClient[] getAllActiveShards() {
		// Returns all active shards
		
		return this.redisClients.toArray(new RPubClient[] {}); 
	}
	
}
