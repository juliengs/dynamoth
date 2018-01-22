package Dynamoth.Core.Manager;

import Dynamoth.Core.RPubMessageListener;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.JedisRPubClient;
import Dynamoth.Core.Client.RPubClient;
import Dynamoth.Core.Client.RPubClientId;

public class LLADynamothRPubManager extends AbstractRPubManager {

	private JedisRPubClient rpubClient = null;
	
	// Override those variables to hook to a "custom" jedis instance
	private RPubClientId localRPubClientId = new RPubClientId(1); // shouldn't matter cuz unused
	private String 		 localJedisHost    = "localhost";
	private int    		 localJedisPort    = 6379;
	
	private RPubMessageListener messageListener;
	
	public LLADynamothRPubManager(RPubNetworkID networkID,
			RPubMessageListener messageListener) {
		super(networkID, messageListener);

		this.messageListener = messageListener;
		
		// We don't create the Jedis object right away because we let the chance to users of the class
		// to override some parameters: jedis host, jedis port and rpubclientid to "hook" the LLA to another
		// jedis instance.
		
	}
	
	@Override
	public void initialize() {
		// Initialize the jedis connection
		rpubClient = new JedisRPubClient(getNetworkID(),
				this.localRPubClientId, 1,
				this.localJedisHost, this.localJedisPort, "",
				messageListener);
		
		// Connect jedis
		rpubClient.connect();
	}

	@Override
	public RPubClient[] getPublicationShards(String channelName) {
		// Return the only instance
		return new RPubClient[] {rpubClient};
	}

	@Override
	public RPubClient[] getSubscriptionShards(String channelName) {
		// Return the only instance
		return new RPubClient[] {rpubClient};
	}

	@Override
	public RPubClient[] getAllActiveShards() {
		// Return the only instance
		return new RPubClient[] {rpubClient};
	}

	public RPubClientId getLocalRPubClientId() {
		return localRPubClientId;
	}

	public void setLocalRPubClientId(RPubClientId localRPubClientId) {
		this.localRPubClientId = localRPubClientId;
	}

	public String getLocalJedisHost() {
		return localJedisHost;
	}

	public void setLocalJedisHost(String localJedisHost) {
		this.localJedisHost = localJedisHost;
	}

	public int getLocalJedisPort() {
		return localJedisPort;
	}

	public void setLocalJedisPort(int localJedisPort) {
		this.localJedisPort = localJedisPort;
	}

	public JedisRPubClient getRpubClient() {
		return rpubClient;
	}

}
