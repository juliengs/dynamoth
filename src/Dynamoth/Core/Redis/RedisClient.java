package Dynamoth.Core.Redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;

public class RedisClient {

	public class Publication
	{
		private String channel = "";
		private String message = "";
		
		public Publication(String channel, String message) {
			this.channel = channel;
			this.message = message;
		}
		
		public String getChannel() {
			return channel;
		}
		
		public String getMessage() {
			return message;
		}
	}
	
	private int msgCount = 0;
	
	// NetworkEngineID
	private RPubNetworkID networkID =  null;
	
	// Redis instance
	private Jedis subscriberJedis = null; // SubscriberJedis who subscribes to topics 
	private Jedis[] publisherJedis = null;  // PublisherJedis who publishes to topics
	
	private RPubMessageListener messageListener;
	
	private PubSubListener pubSubListener = null;
	private boolean subscriptionThreadReady = false;

	private String jedisHostName; // Jedis node hostname
	private int jedisHostPort;    // Jedis node port

	private RedisManager redisManager;

	private int shardIndex;
	
	private int publisherCount;
	
	private Object publisherPipelineLock = new Object();
	private boolean publisherPipelineEmpty = true;
	private Pipeline publisherPipeline;
	private Thread publicationProcessorThread;
	
	private LinkedList<Publication> publisherPipelineQueue = new LinkedList<Publication>();
	
	private int publisherPipelineWaitDelay = 20; // in ms
	
	public static AtomicInteger recvCount = new AtomicInteger(0);
	public static AtomicInteger[] sentCount = null;
	public static Object timeSpentLock = new Object(); 
	public static double timeSpent = 0.0;
	public static Map<String,AtomicInteger> channelMsgCount = new HashMap<String,AtomicInteger>();
	public static AtomicInteger totalMsgCount = new AtomicInteger(0);
	
	public RedisClient(RPubNetworkID networkID, int shardIndex, int publisherCount, String jedisHostName, int jedisHostPort, RPubMessageListener messageListener, RedisManager redisManager) {
		this.networkID = networkID;
		this.shardIndex = shardIndex;
		this.publisherCount = publisherCount;
		this.jedisHostName = jedisHostName;
		this.jedisHostPort = jedisHostPort;
		this.messageListener = messageListener;
		this.redisManager = redisManager;
		
		sentCount = new AtomicInteger[publisherCount];
		for (int i=0; i<publisherCount; i++) {
			sentCount[i] = new AtomicInteger(0);
		}
	}

	/**
	 * Connect to the Jedis node
	 */
	public void connect() {
		// Connect subscriber, publisher
		subscriberJedis = new Jedis(jedisHostName, jedisHostPort, 0);
		publisherJedis = new Jedis[this.publisherCount];
		for (int i=0; i<this.publisherCount; i++) {
			publisherJedis[i] = new Jedis(jedisHostName, jedisHostPort, 0);
		}
		
		publisherPipeline = publisherJedis[0].pipelined();
		
		// Our publication processor thread
		publicationProcessorThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				// Every x ms, publish queued messages and recreate pipeline
				
				while (true) {
					try {
						Thread.sleep(publisherPipelineWaitDelay);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					//int runId = ProfilingManager.get().getTimingManager().startTiming("queuePublication");
					
					synchronized (publisherPipelineLock)
					{
						if (publisherPipelineEmpty == false) {
							// Enqueue all publications :-)
							Publication publication = publisherPipelineQueue.poll();
							int count = 0;
							while (publication != null && count < 100) {
								publisherPipeline.publish(publication.getChannel(), publication.getMessage());
								publication = publisherPipelineQueue.poll();
								//count++;
							}
							
							publisherPipeline.sync();
							publisherPipeline = publisherJedis[0].pipelined();
							if (count<100)
								publisherPipelineEmpty = true;
							//System.out.println("Sync'ed");
						}
					}
					
					/*double elapsed = ProfilingManager.get().getTimingManager().stopTiming("queuePublication", runId);
					synchronized(timeSpentLock) {
						timeSpent += elapsed;
					}*/
				}			
			}
		}, "RedisClient-PublicationProcessor");
		
		publicationProcessorThread.start();
		
	}
	
	/**
	 * Create a jedis channel
	 * @param channelName Name of channel
	 */
	public synchronized void createChannel(String channelName) {
		// Nothing special to do to create a channel...
	}
	
	public void publishToChannel(String channelName, String message) {
		queuePublication(channelName, message);
	}
	
	public void publishToChannel(String channelName, RPubMessage message) {
		try {
			queuePublication(channelName, toString(message));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void queuePublication(String channelName, String message) {
		
		//int runId = ProfilingManager.get().getTimingManager().startTiming("queuePublication");
		/*
		// Pick a random publisher jedis :-)
		int jedisIndex = CollectionUtils.random.nextInt(this.publisherCount);
		// Lock on that specific publisher
		synchronized (publisherJedis[jedisIndex]) {
			publisherJedis[jedisIndex].publish(channelName, message);
			sentCount[jedisIndex].incrementAndGet();
		}
		*/
		
		synchronized (channelMsgCount) {
			if (channelMsgCount.get(channelName) == null) {
				channelMsgCount.put(channelName, new AtomicInteger(1));
			} else {
				//channelMsgCount.get(channelName).incrementAndGet();					
				channelMsgCount.get(channelName).addAndGet(message.length()*2);
			}
			totalMsgCount.addAndGet(message.length()*2);
		}
		
		synchronized (publisherPipelineLock)
		{
			publisherPipelineQueue.add(new Publication(channelName, message));
			publisherPipelineEmpty = false;
		}
		
		/*double elapsed = ProfilingManager.get().getTimingManager().stopTiming("queuePublication", runId);
		synchronized(timeSpentLock) {
			timeSpent += elapsed;
		}*/
	}
	
	public synchronized void subscribeToChannel(final String... channelName) { 
		//System.out.println("Me [" + shardIndex + "] subscribing to " + channelName);
		// If our pubsub listener does not exist, create it on another thread; otherwise, reuse it (directly tell the pubsub to subscribe to the new channel)
		if (pubSubListener == null) {
			pubSubListener = new PubSubListener();
			
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					// Subscribe to channel
					synchronized (subscriberJedis) {
						// TODO: trap the exception and spawn a new subscriber jedis if it crashes?
						subscriberJedis.subscribe(pubSubListener, channelName);
					}					
				}
			}).start();
		} else {
			// Add to existing subscription
			if (subscriptionThreadReady == false) {
				// However we have to wait for the thread to be started properly...
				synchronized(pubSubListener) {
					try {
						pubSubListener.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			pubSubListener.subscribe(channelName);
		}
	}
	
	public synchronized void unsubscribeFromChannel(String channelName) {
		synchronized(subscriberJedis) {
			pubSubListener.unsubscribe(channelName);
			//System.out.println("Unsubscribing from " + channelName);
		}
	}
	
	private class PubSubListener extends JedisPubSub {

		@Override
		public void onMessage(String channel, String message) {
			/*
			synchronized (channelMsgCount) {
				if (channelMsgCount.get(channel) == null) {
					channelMsgCount.put(channel, new AtomicInteger(1));
				} else {
					channelMsgCount.get(channel).incrementAndGet();					
				}
			}
			*/
			
			// Message received
			if (message.startsWith("DBGDBG")) {
				System.out.println("DEBUG-MSG: " + shardIndex + " message");
				return;
			}
			
			// Increment our counter
			recvCount.incrementAndGet();
			
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
				int msgShardIndex = redisManager.getHashedShardIndex(((RPubSubscribeMessage)msg).getChannelName());
				//redisManager.subscribeToChannel(((RPubSubscribeMessage)msg).getChannelName());
				
				if ( msgShardIndex == shardIndex ) {
					//System.out.println("SubscriptionMsg / For " + ((RPubSubscribeMessage)msg).getChannelName() + "/ Same shard (Accepting) / " + ((RPubSubscribeMessage)msg).getChannelName());
					subscribeToChannel(((RPubSubscribeMessage)msg).getChannelName());
				}
				else {
					//System.out.println("SubscriptionMsg / For " + ((RPubSubscribeMessage)msg).getChannelName() + "/ Diff shard (Rejecting)");
					subscribeToChannel(((RPubSubscribeMessage)msg).getChannelName());
				}
				
			}
			else if (msg instanceof RPubUnsubscribeMessage) {
				int msgShardIndex = redisManager.getHashedShardIndex(((RPubUnsubscribeMessage)msg).getChannelName());
				//redisManager.unsubscribeFromChannel(((RPubUnsubscribeMessage)msg).getChannelName());
				
				if ( msgShardIndex == shardIndex ) {
					unsubscribeFromChannel(((RPubUnsubscribeMessage)msg).getChannelName());
				} else {
					unsubscribeFromChannel(((RPubUnsubscribeMessage)msg).getChannelName());
				}
				
			}
			else {
				msgCount++;
				//System.out.println("RedisClient[" + jedisHostName + ":" + jedisHostPort + "]: " + msgCount);
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
			if (subscriptionThreadReady == false) {
				synchronized(pubSubListener) {
					subscriptionThreadReady = true;
					
					// Subscribe to the sub-channel
					//System.out.println("SUB [" + shardIndex + "] to subscription channel / " + "sub" + networkID.hashCode());
					//this.subscribe("sub" + networkID.hashCode());
					
					pubSubListener.notifyAll();
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
