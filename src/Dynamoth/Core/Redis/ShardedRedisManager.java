package Dynamoth.Core.Redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import Dynamoth.Client.Client;
import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Core.Base64Coder;
import Dynamoth.Core.RPubBroadcastMessage;
import Dynamoth.Core.RPubDataMessage;
import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubMessageListener;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.RPubPublishMessage;
import Dynamoth.Core.RPubSubscribeMessage;
import Dynamoth.Core.RPubSubscriptionMessage;
import Dynamoth.Core.RPubUnsubscribeMessage;
import Dynamoth.Util.Properties.PropertyManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

@SuppressWarnings("unused")
public class ShardedRedisManager {
	
	// NetworkEngineID
	private RPubNetworkID networkID =  null;
	
	// Redis nodes
	private List<String> redisHosts = new ArrayList<String>();
	
	// Redis instance
	private ShardedJedis subscriberJedis = null; // SubscriberJedis who subscribes to topics 
	private ShardedJedis publisherJedis = null;  // PublisherJedis who publishes to topics
	
	// Pub sup listeners for sharded jedis / 1 thread per Jedis host
	private Map<Integer, JedisPubSub> pubSubListeners = new HashMap<Integer, JedisPubSub>();
	private Map<Integer, Boolean> subscriptionThreadsReady = new HashMap<Integer, Boolean>();

	private RPubMessageListener messageListener;
	
	// Connect to redis
	public ShardedRedisManager(RPubNetworkID networkID, RPubMessageListener messageListener) {
		this.networkID = networkID;
		this.messageListener = messageListener;
		
		// Read properties file
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		
		String rawServers = StringUtils.strip(
				props.getProperty("network.rpub.servers"));
		
		//this.listener = listener;

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
	
	private int getHashedShard(String channelName) {
		// Get shard info
		JedisShardInfo info = subscriberJedis.getShardInfo(channelName);
		return (info.getHost() + ":" + info.getPort()).hashCode();
	}
	
	/**
	 * Connect to Jedis nodes
	 */
	public void connect() {
		// Construct the list of shardinfo
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		for (String host: redisHosts) {
			shards.add(new JedisShardInfo(getJedisHostName(host), getJedisHostPort(host)));
		}
		
		//jedis = new ShardedJedis(shards);
		subscriberJedis = new ShardedJedis(shards);
		publisherJedis = new ShardedJedis(shards);
	}
	
	/**
	 * Create a jedis channel
	 * @param channelName Name of channel
	 */
	public synchronized void createChannel(String channelName) {
		// Nothing special to do to create a channel...
	}
	
	public synchronized void publishToUnicast(NetworkEngineID networkID, RPubMessage message) {
		publishToChannel(getUnicastChannelName(networkID), message);
	}
	
	public synchronized void publishToBroadcast(RPubMessage message) {
		publishToChannel(getBroadcastChannelName(), message);
	}
	
	public synchronized void publishToChannel(String channelName, RPubMessage message) {
	//	try {
			//publisherJedis.publish(channelName, toString(message));
	//	} catch (IOException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
	}
	
	public synchronized void subscribeToBroadcast() {
		subscribeToChannel(getBroadcastChannelName());
	}
	
	public synchronized void subscribeToUnicast() {
		subscribeToChannel(getUnicastChannelName());
	}
	
	public synchronized void subscribeToChannel(final String channelName) { 
		// Lookup our pubsub object on the map (based on the shard corresponding to the channel). It if doesn't exist,
		// create it and subscribe using our object on a new thread
		Jedis jedis = subscriberJedis.getShard(channelName);
		int hashedShard = getHashedShard(channelName);
		if (pubSubListeners.get(hashedShard) == null) {
			final PubSubListener newListener = new PubSubListener();
			synchronized(pubSubListeners) {
				pubSubListeners.put(hashedShard, newListener);
			}
			
			//System.out.println("Spawning Thread HashShard=" + hashedShard + " / tID=" + this.networkID + " / channelName=" + channelName);
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					// Subscribe to channel
					//subscriberJedis.subscribe(newListener, channelName);					
					
				}
			}).start();
		} else {
			//System.out.println("REUSING Thread HashShard=" + hashedShard + " / tID=" + Thread.currentThread().getId() + " / channelName=" + channelName);
			// Add to existing subscription
			JedisPubSub listener = pubSubListeners.get(hashedShard);
			Boolean ready = subscriptionThreadsReady.get(hashedShard);
			if (ready == null || ready == false) {
				// However we have to wait for the thread to be started properly...
				synchronized(listener) {
					try {
						listener.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			listener.subscribe(channelName);
		}
	}
	
	public synchronized void unsubscribeFromChannel(String channelName) {
		//subscriberJedis.unsubscribe(pubSubListeners.get(getHashedShard(channelName)), channelName);
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
	
	private class PubSubListener extends JedisPubSub {

		@Override
		public void onMessage(String channel, String message) {
			// Message received
			
			// Deal with it!: unserialize it
			RPubMessage msg = null;
			try {
				msg = (RPubMessage)fromString(message);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// If broadcast or publish message and source is 'Self', then don't consume it
			if (msg instanceof RPubBroadcastMessage || msg instanceof RPubPublishMessage) {
				if (msg.getSourceID().equals(networkID)) {
					return;
				}
			}
			
			// For debugging purposes, output the message
			String msgData = "";
			if (msg instanceof RPubDataMessage) {
				msgData = ((RPubDataMessage) msg).getPayload().toString();
			}
			else if (msg instanceof RPubSubscriptionMessage) {
				 msgData = ((RPubSubscriptionMessage)msg).getChannelName();
			}
			//System.out.println("(" + networkID.toString() + ") " + "Message received: " + msg.toString() + " (containing " + msgData + ")");
			
			// Check for type of message and react upon it
			if (msg instanceof RPubSubscribeMessage) {
				subscribeToChannel(((RPubSubscribeMessage)msg).getChannelName());
			}
			else if (msg instanceof RPubUnsubscribeMessage) {
				unsubscribeFromChannel(((RPubUnsubscribeMessage)msg).getChannelName());
			}
			else {
				messageListener.messageReceived(channel, msg, message.length());
			}
		}

		@Override
		public void onPMessage(String pattern, String channel, String message) {
			// Pattern message received
			
		}

		@Override
		public void onSubscribe(String channel, int subscribedChannels) {
			// A client subscribes
			//System.out.println("(" + networkID.toString() + ") " + "Subscribed: " + channel);
			Jedis jedis = subscriberJedis.getShard(channel);
			int hashedShard = getHashedShard(channel);
			Boolean ready;
			synchronized(subscriptionThreadsReady) {
				ready = subscriptionThreadsReady.get(hashedShard);
			}
			JedisPubSub listener;
			synchronized(pubSubListeners) {
				listener = pubSubListeners.get(hashedShard);
			}
			// DEBUG - get shard info
			//JedisShardInfo info = subscriberJedis.getShardInfo(channel);
			//System.out.println("Subscription:host=" + info.getHost() + ":" + info.getPort());
			if (ready == null || ready == false) {
				synchronized(listener) {
					subscriptionThreadsReady.put(hashedShard, true);
					listener.notifyAll();
				}
			}
			
		}

		@Override
		public void onUnsubscribe(String channel, int subscribedChannels) {
			// A client unsubscribes
			
		}

		@Override
		public void onPUnsubscribe(String pattern, int subscribedChannels) {
			// A client unsubscribes with a specific pattern
			
		}

		@Override
		public void onPSubscribe(String pattern, int subscribedChannels) {
			// A client subscribes with a specific pattern
			
		}

		
		
	}
	
    /** Read the object from Base64 string. */
    private static Object fromString( String s ) throws IOException ,
                                                        ClassNotFoundException {
        byte [] data = Base64Coder.decode( s );
        ObjectInputStream ois = new ObjectInputStream( 
                                        new ByteArrayInputStream(  data ) );
        Object o  = ois.readObject();
        ois.close();
        return o;
    }

    /** Write the object to a Base64 string. */
    private static String toString( Serializable o ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( o );
        oos.close();
        return new String( Base64Coder.encode( baos.toByteArray() ) );
    }
}
