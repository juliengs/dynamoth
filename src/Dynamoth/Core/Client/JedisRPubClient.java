package Dynamoth.Core.Client;

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
import Dynamoth.Core.RPubUnsubscribeMessage;
import Dynamoth.Core.Game.Messages.RGameMoveMessage;
import Dynamoth.Core.RPubRawMessage;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;

public class JedisRPubClient implements RPubClient {

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
	private RPubMessageListener originalMessageListener;
	
	private PubSubListener pubSubListener = null;
	private boolean subscriptionThreadReady = false;

	private String jedisHostName; // Jedis node hostname
	private int jedisHostPort;    // Jedis node port
	
	private String jedisDomain; // Jedis domain

	private RPubClientId clientId;
	
	// DEBUG
	public int playerId = -1;
	
	private boolean connected = false;
	
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
	
	public JedisRPubClient(RPubNetworkID networkID, RPubClientId clientId, int publisherCount, String jedisHostName, int jedisHostPort, String jedisDomain, RPubMessageListener messageListener) {
		this.networkID = networkID;
		this.clientId = clientId;
		this.publisherCount = publisherCount;
		this.jedisHostName = jedisHostName;
		this.jedisHostPort = jedisHostPort;
		this.jedisDomain = jedisDomain;
		this.originalMessageListener = messageListener;
		
		if (RPubMessageDelayer.shouldEnable()) {
			this.messageListener = new RPubMessageDelayer(originalMessageListener, RPubMessageDelayer.localDelay(), networkID);
		} else {
			this.messageListener = originalMessageListener;
		}
		
		sentCount = new AtomicInteger[publisherCount];
		for (int i=0; i<publisherCount; i++) {
			sentCount[i] = new AtomicInteger(0);
		}
	}

	@Override
	public void connect() {
		try
		{
			// Connect subscriber, publisher
			subscriberJedis = new Jedis(jedisHostName, jedisHostPort, 0);
			subscriberJedis.auth("ouibsgkbvilbsgksu78272lisfkblb171bksbksv177282");
			publisherJedis = new Jedis[this.publisherCount];
			for (int i=0; i<this.publisherCount; i++) {
				publisherJedis[i] = new Jedis(jedisHostName, jedisHostPort, 0);
				publisherJedis[i].auth("ouibsgkbvilbsgksu78272lisfkblb171bksbksv177282");
			}
			
			publisherPipeline = publisherJedis[0].pipelined();
			
			// Mark as connected
			connected = true;
			
			// Our publication processor thread
			publicationProcessorThread = new Thread(new Runnable() {
				
				@Override
				public void run() {
					// Every x ms, publish queued messages and recreate pipeline
					
					while (connected) {
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
		catch (Exception ex) {
			System.out.println("*** Connection FAILED for " + jedisHostName + ":" + jedisHostPort + "!!!");
		}
	}

	@Override
	public void disconnect() {
		// Close connection
		
		// Mark as disconnected
		connected = false;
		
		// Wait for Publication Processor thread to join (end)
		try {
			publicationProcessorThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Close all jedises
		subscriberJedis.disconnect();
		for (int i=0; i<this.publisherCount; i++) {
			publisherJedis[i].disconnect();
		}
	}
	
	@Override
	public boolean isConnected() {
		return connected;
	}

	public String getJedisDomain() {
		return jedisDomain;
	}

	@Override
	public void createChannel(String channelName) {
		// Nothing special to do to create a channel (yet)...
	}

	@Override
	public void publishToChannel(String channelName, String message) {
		queuePublication(channelName, message);
	}
	
	@Override
	public void publishToChannel(String channelName, RPubMessage message) {
		
		// Hack: if our message is a raw message, then send only the payload
		// (do not serialize)
		if (message instanceof RPubRawMessage) {
			RPubRawMessage raw = (RPubRawMessage)message;
			queuePublication(channelName, raw.getPayload());
			return;
		}
		
		try {
			String messageAsString = toString(message);
			
			// Hack: perform padding for RGameMoveMessage instances
			if (message instanceof RPubDataMessage) {
				RPubDataMessage dataMessage = (RPubDataMessage)message;
				if (dataMessage.getPayload() instanceof RGameMoveMessage) {
					
					RGameMoveMessage moveMsg = (RGameMoveMessage)(dataMessage.getPayload());
					// Estimate current size in bytes
					int currentByteSize = messageAsString.length() / 2;
					// Pad delta
					if (RGameMoveMessage.PADDING_TOTAL_BYTES - currentByteSize > 0) {
						moveMsg.setPadding(RGameMoveMessage.PADDING_TOTAL_BYTES - currentByteSize);
					}
					
					// Regenerate message as string...
					messageAsString = toString(message);
					
					//System.out.println("MOVE-MESSAGE-SIZE: " + messageAsString.length());
				}
			}
			
			queuePublication(channelName, messageAsString);
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

	@Override
	public void subscribeToChannel(final String... channelName) {
		subscribeInternal(false, channelName);
	}
	
	/**
	 * Subscribe to pattern
	 */
	public void subscribeToPattern(final String... pattern) {
		subscribeInternal(true, pattern);
	}
	
	private void subscribeInternal(final boolean isPattern, final String... entities) {
		//System.out.println("Me [" + shardIndex + "] subscribing to " + channelName);
		// If our pubsub listener does not exist, create it on another thread; otherwise, reuse it (directly tell the pubsub to subscribe to the new channel)
		if (pubSubListener == null) {
			pubSubListener = new PubSubListener();
			
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					// Subscribe to channel
					synchronized (subscriberJedis) {
						try {
							// TODO: trap the exception and spawn a new subscriber jedis if it crashes?
							if (isPattern) {
								subscriberJedis.psubscribe(pubSubListener, entities);
							} else {
								subscriberJedis.subscribe(pubSubListener, entities);
							}
						}
						catch (Exception ex) {
							System.out.println("*** Connection FAILED for " + jedisHostName + ":" + jedisHostPort + "!!!");
						}
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
			// Add subscription to channel... This is done from within our pub-sub thread
			if (isPattern) {
				pubSubListener.psubscribe(entities);
			} else {
				pubSubListener.subscribe(entities);
			}
		}	
	}

	@Override
	public void unsubscribeFromChannel(final String channelName) {
		unsubscribeInternal(false, channelName);
	}
	
	public void unsubscribeFromAllChannels() {
		unsubscribeInternal(false);
	}
	
	/**
	 * Unsubscribe from pattern
	 */
	public void unsubscribeFromPattern(final String... pattern) {
		unsubscribeInternal(true, pattern);
	}
	
	private void unsubscribeInternal(final boolean isPattern, final String... entities) {
		synchronized(subscriberJedis) {
			if (isPattern) {
				//subscriberJedis.punsubscribe(pubSubListener, entities);	
			} else {
				pubSubListener.unsubscribe(entities);
			}
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
					
			// Increment our counter
			recvCount.incrementAndGet();
			
			// Deal with it!: unserialize it
			RPubMessage msg = null;
			try {
				msg = (RPubMessage)fromString(message);
			} catch (Exception e) {
				//e.printStackTrace();
				// If we can't parse, then instead create a raw data message
				msg = new RPubRawMessage(new RPubNetworkID(0), message);
			}
			
			// If broadcast or publish message and source is 'Self', then don't consume it
			// [Why not?]
			if (msg instanceof RPubBroadcastMessage || msg instanceof RPubPublishMessage) {
				if (msg.getSourceID().equals(networkID)) {
					//return;
					// Allow transmission...
				}
			}
			
			// Check for type of message and react upon it
			if (msg instanceof RPubSubscribeMessage) {
				// As a safeguard and to make sure that we don't subscribe to all channels
				// if psubscribe(*) is used, verify that the target match our engine
				RPubSubscribeMessage subscribeMessage = (RPubSubscribeMessage)msg;
				if (subscribeMessage.getTargetID().equals(networkID)) {
					subscribeToChannel(subscribeMessage.getChannelName());
				}
			}
			else if (msg instanceof RPubUnsubscribeMessage) {
				// As a safeguard and to make sure that we don't unsubscribe from all channels
				// if psubscribe(*) is used, verify that the target match our engine
				RPubUnsubscribeMessage unsubscribeMessage = (RPubUnsubscribeMessage)msg;
				if (unsubscribeMessage.getTargetID().equals(networkID)) {
					// If channel name is empty string, then unsubscribe from all channels
					if (unsubscribeMessage.getChannelName().equals("")) {
						unsubscribeFromAllChannels();
						System.out.println("** UNSUBSCRIBING FROM ALL CHANNELS! / shard=" + clientId.getId());
					} /* Unsubscribe from one channel */ else {
						unsubscribeFromChannel(unsubscribeMessage.getChannelName());
						System.out.println("** UNSUBSCRIBING FROM: " + unsubscribeMessage.getChannelName());
					}
				}
			}
			else {
				msgCount++;
				//System.out.println("RedisClient[" + jedisHostName + ":" + jedisHostPort + "]: " + msgCount);
			}
			
			// Queue the incoming message
			messageListener.messageReceived(channel, msg, message.length());
			//messageListener.messageReceived(channel, msg, message.length());
		}

		@Override
		public void onPMessage(String pattern, String channel, String message) {
			// Pattern message received
			onMessage(channel, message);
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
			onSubscribe(pattern, subscribedChannels);
		}

		
		
	}
	
    /** Read the object from Base64 string. */
    public static Object fromString( String s ) throws IOException ,
                                                        ClassNotFoundException {
        byte [] data = Base64Coder.decode( s );
        ObjectInputStream ois = new ObjectInputStream( 
                                        new ByteArrayInputStream(  data ) );
        Object o  = ois.readObject();
        ois.close();
        return o;
    }

    /** Write the object to a Base64 string. */
    public static String toString( Serializable o ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( o );
        oos.close();
        return new String( Base64Coder.encode( baos.toByteArray() ) );
    }

	@Override
	public RPubClientId getId() {
		return this.clientId;
	}
}
