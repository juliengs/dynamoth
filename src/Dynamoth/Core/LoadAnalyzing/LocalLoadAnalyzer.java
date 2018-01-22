package Dynamoth.Core.LoadAnalyzing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarProxy;
import org.hyperic.sigar.SigarProxyCache;

import Dynamoth.Client.Client;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.ChannelExistsException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchClientException;
import Dynamoth.Core.RPubBroadcastMessage;
import Dynamoth.Core.RPubConnectMessage;
import Dynamoth.Core.RPubDataMessage;
import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.RPubPublishMessage;
import Dynamoth.Core.RPubSubscribeMessage;
import Dynamoth.Core.RPubSubscriptionMessage;
import Dynamoth.Core.RPubUnsubscribeMessage;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.ControlMessages.ChangeChannelMappingControlMessage;
import Dynamoth.Core.ControlMessages.ControlMessage;
import Dynamoth.Core.ControlMessages.CreateChannelControlMessage;
import Dynamoth.Core.ControlMessages.LLAUpdateControlMessage;
import Dynamoth.Core.ControlMessages.UnsubscribeFromAllChannelsControlMessage;
import Dynamoth.Core.ControlMessages.UpdateRetentionRatiosControlMessage;
import Dynamoth.Core.Game.RConfig;
import Dynamoth.Core.LoadBalancing.CostModel.CostAnalyzer;
import Dynamoth.Core.Manager.AbstractRPubManager;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Core.Manager.LLADynamothRPubManager;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanId;
import Dynamoth.Core.Manager.Plan.PlanMapping;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.ShardsSelector.RandomShardSelector;
import Dynamoth.Core.Util.RPubUtil;
import Dynamoth.Core.Util.SigarUtil;
import Dynamoth.Util.Log.NetworkData;
import Dynamoth.Util.Message.Handler;
import Dynamoth.Util.Message.Message;
import Dynamoth.Util.Message.Reactor;
import Dynamoth.Util.Properties.PropertyManager;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LocalLoadAnalyzer {
	
	private RPubNetworkEngine engine = null;
	private RPubNetworkEngine llaEngine = null;
	private Reactor reactor = null;
	
	private Object engineLock = new Object();
	
	// RPub Client ID that this LLA represents
	private RPubClientId id = null;
	
	private HashMap<String, Channel> channels = new HashMap<String, Channel>();
	
	private HashMap<String, PlanId> lastUpdateSent = new HashMap<String, PlanId>();
	
	private Object channelsLock = new Object();
	
	private int startTime = (int) (System.currentTimeMillis() / 1000);
	
	private Long nextDump = Long.MIN_VALUE;
	
	// Sigar - for network logging (compare NetData VS computed data)
	private SigarProxy sigar;
	
	// NetData
	NetworkData netData = null;
	
	// LLA Update message stuff
	private int lastLLAUpdateTime = 0;
	
	// Timer to process LocalLoadAnalyzer stuff
	private Timer timer = new Timer();
	
	private Map<Integer, Long> byteInCounters = new HashMap<Integer, Long>();
	private Map<Integer, Long >byteOutCounters = new HashMap<Integer, Long>();
	
	// Output CSV
	private Writer csvWriter = null;
	
	// Correct/incorrect pub server message counters
	private int correctMessageCount   = 0;
	private int incorrectMessageCount = 0;
	private int forwardMessageCount = 0;
	
	// Debug
	private int messageCounterTime = -1;
	private int messageCounter = 0;
	
	// List of PlanMappings that changed in the latest plan. Upon receiving a publication for those
	// channels, a switch message will be issued.
	private List<PlanMapping> changingChannels = new ArrayList<PlanMapping>();

	// COST-ANALYZER STUFF
	
	// Retention ratios for all tiles
	private Map<String, Double> retentionRatios = new HashMap<String, Double>();
	private Object retentionRatiosLock = new Object();
	
	public LocalLoadAnalyzer(RPubClientId id, RPubNetworkEngine manager) {
		this.id = id;
		this.engine = manager;
	}
	
	public LocalLoadAnalyzer(RPubClientId id, final RPubNetworkEngine engine, final RPubNetworkEngine llaEngine, Reactor reactor) {
		this.id = id;
		this.engine = engine;
		this.llaEngine = llaEngine;
		this.reactor = reactor;
		
		// Read some props
		Properties props = PropertyManager
				.getProperties(Client.DEFAULT_CONFIG_FILE);

		String csvFileName = props.getProperty("network.rpub.localloadanalyzer.csv_dump_file", "LocalLoadAnalyzer_Output.csv");
		
		// Initialize CSV writer
		try {
			csvWriter = new BufferedWriter(new FileWriter(csvFileName, false));
			csvWriter.write("Channel;Time;sub;pub;sent;bytein;byteout;totalpub;totalsent;totalbytein;totalbyteout\n");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// The reactor should register a handler for the CreateChannelMessage
		reactor.register(CreateChannelControlMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				processCreateChannelControlMessage(msg);
			}
		});
		
		// Subscribe to UpdateRetentionRatiosControlMessage (Cost-Model)
		reactor.register(UpdateRetentionRatiosControlMessage.class, new Handler() {

			@Override
			public void handle(Message msg) {
				processUpdateRetentionRatiosControlMessage(msg);
				
			}
			
		});
		
		final RPubClientId ourId = id;
		reactor.register(UnsubscribeFromAllChannelsControlMessage.class, new Handler() {
			@Override
			public void handle(Message msg) {
				UnsubscribeFromAllChannelsControlMessage unsubscribeMessage = (UnsubscribeFromAllChannelsControlMessage)(msg);
				if (unsubscribeMessage.getClientId().getId() == ourId.getId()) {
					unregisterAllSubscriptions();
					System.out.println("************* UNREG ALL *************");
				}
			}
			
		});
		
		// The NE registers for incoming low-level messages
		llaEngine.registerLowLevelListener(new RPubNetworkEngine.LowLevelListener() {
			
			@Override
			public void messageReceived(String channelName, RPubMessage message, int rawMessageSize) {
				processLowLevelMessage(channelName, message, rawMessageSize);
				
			}
		});
		
		// If we are using wildcard channel subscription, then subscribe to * pattern on llaEngine
		if (DynamothRPubManager.WILDCARD_CHANNEL_SUBSCRIPTION) {
			// Subscribe to wildcard-* channel
			LLADynamothRPubManager manager = (LLADynamothRPubManager)(this.llaEngine.getRPubManager());
			manager.getRpubClient().subscribeToPattern("*");
		}
		
		// Setup sigar
		SigarUtil.setupSigarNatives();
		setupSigar();
		
		// Subscribe to the loadanalyzer-channel channel
		// Subscribe to the lla-channel (for testing purposes)
		try {
			this.engine.subscribeChannel("loadanalyzer-channel", this.engine.getId());
			Thread.sleep(700);
			this.engine.createChannel("lla-channel");
			Thread.sleep(700);
			this.engine.subscribeChannel("lla-channel", this.llaEngine.getId());
			// Subscribe to plan-push-channel-lla for the lazy version of Dynamoth
			this.engine.subscribeChannel("plan-push-channel-lla", this.engine.getId());
			Thread.sleep(700);
			//Subscribe to cost-analyzer-update-channel
			this.engine.subscribeChannel("cost-analyzer-update-channel", this.engine.getId());
		} catch (NoSuchChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ChannelExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Scheduling TIMER...");
		
		// Schedule timer (to send LLA Update messages)
		timer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				// Invoke the method
				processTimer();
			}
		}, 1000, 1000);
		
		// Sigar timer
		new Timer().scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				
				// Pack sigar stuff into the message
				Long[] m;
				try {
					m = netData.getMetric();
					
					long totalrx = m[0];
			        long totaltx = m[1];
			        synchronized(netData) {
			        	// Increment byteIn and byteOut at current time
			        	int timeSlice = getCurrentTimeSlice();
			        	if (byteInCounters.get(timeSlice) == null)
			        		byteInCounters.put(timeSlice, 0L);
			        	if (byteOutCounters.get(timeSlice) == null)
			        		byteOutCounters.put(timeSlice, 0L);
			        	byteInCounters.put( timeSlice, byteInCounters.get(timeSlice) + totalrx );
			        	byteOutCounters.put( timeSlice, byteOutCounters.get(timeSlice) + totaltx );
			        }
					
				} catch (SigarException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, 50, 50);
	}
	
	private void setupSigar() {
		Sigar sigarImpl = new Sigar();
		
		try {
			netData = new NetworkData(sigarImpl);
		} catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			netData = null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			netData = null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			netData = null;
		}
		
		this.sigar = 
			SigarProxyCache.newInstance(sigarImpl, 100);
	}
	
	private void processTimer() {
		// Prepare LLA Update control message
		LLAUpdateControlMessage message = prepareLLAUpdateMessage();

		//System.out.println("Sending LLAUpdateMessage...");
		
		synchronized (engineLock) {
			// Send our LLA update message
			try {
				engine.send("loadbalancer-channel", message);
			} catch (ClosedChannelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchChannelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Clear old stats
		synchronized (channelsLock) {
			for (Map.Entry<String, Channel> channelEntry: this.channels.entrySet()) {
				Channel channel = channelEntry.getValue();
				channel.clearSliceStats(100);
			}
		}
		
		// Check to see if we need to send keepalive messages
		sendKeepAliveMessages();
		
	}
	
	/**
	 * If needed, send keepalive messages (DynAvail).
	 * Such messages will be sent only if subscribers did not receive any
	 * publication in the last X seconds.
	 */
	private void sendKeepAliveMessages() {
		
		Set<RPubNetworkID> allSubscribers = new HashSet<RPubNetworkID>();
		Set<RPubNetworkID> activeSubscribers = new HashSet<RPubNetworkID>();
		
		// Map of subscribers -> one of their subscription topic
		Map<RPubNetworkID, String> subscribersTopics = new HashMap<RPubNetworkID, String>();
		
		int window = 3;
		
		synchronized (channelsLock) {
			
			// Iterate through the channels
			for (Map.Entry<String,Channel> channelEntry: this.channels.entrySet()) {
				// Get the channel
				Channel channel = channelEntry.getValue();
				
				int lastTime = channel.getLastTime()-1;
				
				// Iterate only if an appropriate last time is defined
				for (int t=lastTime; t>-1 && t>lastTime-window; t--) {
					// Retrieve our slicestats object
					SliceStats sliceStats = channel.getSliceStats(t);
					if (sliceStats == null)
						continue;
					
					// For every subscriber
					for (RPubNetworkID subscriber: sliceStats.getSubscriberList()) {
						// Add it to allSubscribers
						allSubscribers.add(subscriber);
						
						// If not in subscribersTopic, then put the current topic
						if (subscribersTopics.containsKey(subscriber) == false) {
							subscribersTopics.put(subscriber, channel.getChannelName());
						}
						
						// If pubCount > 0, then add it to activeSubscribers
						if (sliceStats.getPublicationStats().getPublications() > 0) {
							activeSubscribers.add(subscriber);
						}
					}
				}
			}
		}
		
		// Compute stale subscribers
		Set<RPubNetworkID> staleSubscribers = new HashSet<RPubNetworkID>(allSubscribers);
		staleSubscribers.removeAll(activeSubscribers);
	
		// Print stale subscribers
		for (RPubNetworkID subscriber: staleSubscribers) {
			System.out.println("Stale: " + subscriber.getId().toString());
			/*
			try {
				// Send him a dummy keepalive packet on one of his channels :-)
				this.engine.send(subscribersTopics.get(subscriber), new ControlMessage());
			} catch (IOException ex) {
				Logger.getLogger(LocalLoadAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
			} catch (NoSuchChannelException ex) {
				Logger.getLogger(LocalLoadAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
			}
			*/
		}
	}
	
	public Channel getAndCreateChannel(String channelName) {
		Channel channel = channels.get(channelName);
		if (channel == null) {
			channel = new Channel(channelName);
			channels.put(channelName, channel);
		}
		return channel;
	}
	
	public int getCurrentTimeSlice() {
		return (int) (System.currentTimeMillis() / 1000) - this.startTime;
	}
	
	public LLAUpdateControlMessage prepareLLAUpdateMessage() {
		LLAUpdateControlMessage message = new LLAUpdateControlMessage(this.id);
		
		// Get current time slice
		int currentTimeSlice = getCurrentTimeSlice();
		
		// Dbg- out RConfig print tile array
		int[][] subscribers = new int[100][100];
		int subscribersCount = 0;
		int[][] lastTimes = new int[100][100];
		int[][] publishers = new int[100][100];
		int[][] sentMsg = new int[100][100];
		Set<RPubNetworkID> publisherList = new HashSet<RPubNetworkID>();
		int publishersCount = 0;
		int altPublishersCount = 0;
		// Dbg [Continued] - number of unique subscribers for tiles, print every subscriber who is subscribed to multiple tiles
		Set<RPubNetworkID> uniqueSubscribers = new HashSet<RPubNetworkID>();
		Set<RPubNetworkID> multipleSubscribers = new HashSet<RPubNetworkID>();
		
		// Lock
		synchronized (channelsLock) {
			
			// Iterate through the channels
			//System.out.println("---Begin Channel Dump---");
			for (Map.Entry<String,Channel> channelEntry: this.channels.entrySet()) {
				// Get the channel
				String channelName = channelEntry.getKey();
				Channel channel = channelEntry.getValue();
				//System.out.println("PACKING CHANNEL+ " + channelEntry.getKey());
				
				// Ask the channel to pack itself to our message only if an appropriate last time is defined
				if (channel.getLastTime() > -1) {
					//System.out.println("Packing from: " + this.lastLLAUpdateTime + " to " + currentTimeSlice + " | channel=" + channelEntry.getKey());
					channel.packToUpdateMessage(message, this.lastLLAUpdateTime, currentTimeSlice);
		
					int lastTime = channel.getLastTime()-1;
					SliceStats sliceStats = channel.getSliceStats(lastTime);
					if (sliceStats == null)
						continue;
					
					// If channel is tile something
					if (channelName.startsWith("tile_")) {
						try {
							int tileX = Integer.parseInt(channelName.split("_")[1]);
							int tileY = Integer.parseInt(channelName.split("_")[2]);
							subscribers[tileX][tileY] = sliceStats.getSubscribers();
							subscribersCount += subscribers[tileX][tileY];
							lastTimes[tileX][tileY] = lastTime;
							publishers[tileX][tileY] = sliceStats.getPublishers().size();
							if (tileX >= RConfig.getTileCountX() || tileY >= RConfig.getTileCountY() ||
									tileX < 0 || tileY < 0) {
								//System.out.println("+++SUB_IN_TILE (" + tileX + "," + tileY + ")");
							}
							publishersCount += publishers[tileX][tileY];
							altPublishersCount += sliceStats.getPublicationStats().getPublications(); 
							publisherList.addAll(sliceStats.getPublishers().keySet());
							//System.out.println("Tile(" + tileX + ";" + tileY + ") => " + subscribers[tileX][tileY]);
							// For all subscribers - iterate
							for (RPubNetworkID sub: sliceStats.getSubscriberList()) {
								if (uniqueSubscribers.contains(sub)) {
									multipleSubscribers.add(sub);
								} else {
									uniqueSubscribers.add(sub);
								}
							}
							
						} catch (NumberFormatException nfe) {
							// Do nothing :-)
							//System.out.println("Warning: Unparseable X/Y! " + channelName);
						}
					}
				}
			}
			
			// Print the array for subscribers
			RConfig.printTileMap(subscribers);
			System.out.println("Total Tile Subscribers Count: " + subscribersCount);
			System.out.println("Total Tile UNIQUE Subscribers: " + uniqueSubscribers.size());
			System.out.print("Multiple Subscribers: ");
			for (RPubNetworkID sub : multipleSubscribers ) {
				System.out.print(sub.getId().toString());
			}
			System.out.print("\n");
			System.out.print("Unique Subscribers: ");
			for (RPubNetworkID sub : uniqueSubscribers ) {
				System.out.print(sub.toString());
			}
			System.out.print("\n");
			
			//RConfig.printTileMap(publishers);
		//	System.out.println("Total Tile Publishers Count: " + publishersCount);
		//	System.out.println("Alt Tile Publishers Count: " + altPublishersCount);
			/*
			System.out.print("Pubs: ");
			for (RPubNetworkID pub : publisherList ) {
				System.out.print(pub.toString());
			}
			System.out.print("\n");
			*/
			
			// Dump correct and incorrect
			System.out.println("Correct message count=" + this.correctMessageCount);
			System.out.println("Incorrect message count=" + this.incorrectMessageCount);
			System.out.println("Forward message count=" + this.forwardMessageCount);
			
		
			System.out.println("---End Channel Dump---");
			
		}
		
		// Pack bytein and byteout in our LLAUpdmessage
		synchronized (netData) {
			for (int i=this.lastLLAUpdateTime; i<currentTimeSlice; i++)
			{
				if (this.byteInCounters.get(i) != null) 
					message.putLocalByteIn(i, this.byteInCounters.get(i));
				if (this.byteOutCounters.get(i) != null) {
					message.putLocalByteOut(i, this.byteOutCounters.get(i));
					System.out.println("ByteOutCounter: " + this.byteOutCounters.get(i));
				}
			}
		}
		
		// Set our last LLA update time
		this.lastLLAUpdateTime = currentTimeSlice;
		
		return message;
	}

	public void dump(int sliceCount) {
		synchronized ( this.channelsLock ) {
			System.out.println("*** DUMPING ***");
			// For all channels
			for (Map.Entry<String,Channel> entry : this.channels.entrySet()) {
				//if (entry.getKey().equals("__announce__") == false)
				//	continue;
				System.out.println("Channel: " + entry.getKey());
				// Print the slices
				for (int i=getCurrentTimeSlice(); i>getCurrentTimeSlice()-sliceCount; i--) {
					if (i<0)
						continue;
					entry.getValue().initializeSliceStats(i);
					SliceStats stats = entry.getValue().getSliceStats(i);
					if (stats == null) {
						System.out.println("...T=" + i + " (Null)");
					} else {
						System.out.println("...T=" + i + " | " + "sub=" + stats.getSubscribers() + " | " + "pub=" + stats.getPublicationStats().getPublications() + " | " + "sent=" + stats.getPublicationStats().getSentMessages());
					}
				}
			}
			System.out.println("*** END OF DUMP ***");
		}
	}
	
	/*
	public void dumpToCSVFile(int sliceCount) {
		synchronized ( this.channelsLock ) {
			System.out.println("*** DUMPING TO CSV FILE ***");
			// For all channels
			for (Map.Entry<String,Channel> entry : this.channels.entrySet()) {
				System.out.println("Channel: " + entry.getKey());
				// Print the slices
				for (int i=getCurrentTimeSlice(); i>getCurrentTimeSlice()-sliceCount; i--) {
					// Skip events prior to time 0
					if (i<0)
						continue;
					
					entry.getValue().initializeSliceStats(i);
					SliceStats stats = entry.getValue().getSliceStats(i);
					if (stats == null) {
						// Do nothing
					} else {
						// Channel;Time;sub;pub;sent;bytein;byteout;totalpub;totalsent;totalbytein;totalbyteout
						try {
							csvWriter.write(entry.getKey() + ";" + i + ";" + 
									stats.getSubscribers() + ";" + stats.getPublications() + ";" + stats.getSentMessages() + ";" +
									stats.getByteIn() + ";" + stats.getByteOut() + ";" + 
									stats.getCumulativePublications() + ";" + stats.getCumulativeSentMessages() + ";" + 
									stats.getCumulativeByteIn() + ";" + stats.getCumulativeByteOut() + "\n");
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			
			try {
				csvWriter.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	*/

	private void createAndRegisterChannel(String channelName) {
		// Create a new "channel" instance
		// TODO - do it only if this channel is registered on the Redis server attached to this LLA
		boolean isNewChannel = true;
		synchronized ( channelsLock ) {
			if (channels.get(channelName) != null) {
				// Channel already existed
				//isNewChannel = false;
				isNewChannel = true;
			}
			getAndCreateChannel(channelName);
		}
		
		if (isNewChannel) {
			//System.out.println("LocalLoadAnalyzer:Register:" + channelName);
			
			// Subscribe to that channel so that we receive a copy of the messages sent through it
			// The bandwidth impact should be minimal because the LLA is located on the same node as the Redis server
			// Subscribe only if we are not using wildcard channel subscription
			if (DynamothRPubManager.WILDCARD_CHANNEL_SUBSCRIPTION == false) {
				try {
					this.llaEngine.subscribeChannel(channelName, this.llaEngine.getId());
				} catch (NoSuchChannelException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private void registerSubscriberForChannel(RPubNetworkID subscriber, String subChannelName) {
		synchronized ( channelsLock ) {
			// Obtain (or initialize) channel
			Channel channel = getAndCreateChannel(subChannelName);
			
			// Initialize and obtain the time slice
			int time = getCurrentTimeSlice();
			channel.initializeSliceStats(time);
			SliceStats stats = channel.getSliceStats(time);
			
			//System.out.println("### ADDING SUBSCRIBER TO " + subChannelName + " ###");
			// If msg is subscription: increment subscriber
			if ( stats.getSubscriberList().add(subscriber) ) {
				// Increment subscriber count for this channel +1 
				// NOTE: this will probably removed as we can simply use the size of the subscription
				// list to determine the number of subscribers
				stats.incrementSubscribers(1);
			}
			if(subChannelName.startsWith("dynamic"))
				System.out.println("register:" + stats.getSubscriberList().size());
		}
	}
	
	private void unregisterSubscriberForChannel(RPubNetworkID subscriber, String subChannelName) {
		synchronized ( channelsLock ) {
			// Obtain (or initialize) channel
			Channel channel = getAndCreateChannel(subChannelName);
			
			// Initialize and obtain the time slice
			int time = getCurrentTimeSlice();
			channel.initializeSliceStats(time);
			SliceStats stats = channel.getSliceStats(time);
			
			//System.out.println("*** REMOVING SUBSCRIBER FROM " + subChannelName + " ***");
			
			// Remove new subscriber from list of subscribers
			if ( stats.getSubscriberList().remove(subscriber) ) {
				// Decrement subscriber count for this channel -1
				// NOTE: this will probably removed as we can simply use the size of the subscription
				// list to determine the number of subscribers
				stats.incrementSubscribers(-1);
			}
			if(subChannelName.startsWith("dynamic"))
				System.out.println("unregister:" + stats.getSubscriberList().size());
		}
	}
	
	/**
	 * Unregisters all subscriptions accross all channels.
	 * Useful for DynAvail where we "fake" failures...
	 */
	private void unregisterAllSubscriptions() {
		synchronized ( channelsLock ) {
			for (Map.Entry<String,Channel> channelEntry: channels.entrySet()) {
				Channel channel = channelEntry.getValue();
				// Initialize and obtain the time slice
				int time = getCurrentTimeSlice();
				channel.initializeSliceStats(time);
				SliceStats stats = channel.getSliceStats(time);
				stats.setSubscribers(0);
				stats.getSubscriberList().clear();
			}
		}
	}
	
	/**
	 * Process create channel control message
	 * @param msg Create channel control message
	 */
	private void processCreateChannelControlMessage(Message msg) {
		CreateChannelControlMessage createChannelMessage = (CreateChannelControlMessage)msg;
		createAndRegisterChannel(createChannelMessage.getChannelName());
	}
	
	private void processUpdateRetentionRatiosControlMessage(Message msg) {
		System.out.println("GOT UPDATE RETENTION MSG!!!");
		UpdateRetentionRatiosControlMessage updateRetentionRatiosMessage = (UpdateRetentionRatiosControlMessage)msg;
		synchronized(retentionRatiosLock) {
			this.retentionRatios = updateRetentionRatiosMessage.getRetentionRatios();
		}
	}

	private void processLowLevelMessage(String channelName, RPubMessage message, int rawMessageSize) {
		// To be safe, prevent any concurrency issue
		synchronized ( channelsLock ) {
		
			//#System.out.println("### LOW LEVEL: " + message.getClass().toString() + "###");
			
			// * Subscription of a network ID to a given channel *
			// Subscription or unsubscription -> increment or decrement the number of subscribers
			// for the channel which is the target of the subscription
			if (message instanceof RPubSubscriptionMessage) {
				RPubSubscriptionMessage subMessage = (RPubSubscriptionMessage)message;
				
				//System.out.println("New subscription: targetID=" + subMessage.getTargetID().toString() + " to " + subMessage.getChannelName());
				
				// If the subscriber IS NOT the LLA (we don't count the LLA as a subscriber because it outputs
				// data trough the loopback interface)
				// TODO: if the LLA IS NOT on the same machine as the corresponding Redis host, then
				// we HAVE TO take the subscription into account because it will generate real traffic
				if (subMessage.getTargetID().equals(this.llaEngine.getId()) == false) {
					// Different engine, so we can process
				
					// Get channel name for this subscription
					String subChannelName = subMessage.getChannelName();
					
					// Make sure the channel exists and is registered so that we can monitor it!
					createAndRegisterChannel(subChannelName);
					
					// Call appropriate helper
					if (message instanceof RPubSubscribeMessage) {
						registerSubscriberForChannel(subMessage.getTargetID(), subChannelName);
					} else if (message instanceof RPubUnsubscribeMessage) {
						unregisterSubscriberForChannel(subMessage.getTargetID(), subChannelName);
					}
				
				} else {
					// Same engine -> don't process
					// (this branch should be removed)
				}
				
			} else if (message instanceof RPubBroadcastMessage) {
				// Check if the payload is a RPubConnectMessage
				RPubBroadcastMessage broadcastMessage = (RPubBroadcastMessage)message;
				if (broadcastMessage.getPayload() instanceof RPubConnectMessage) {
					// Because of how broadcast, private unicast and subscription channels are handled in RPub,
					// we will not be informed of subscriptions regarding those channels.
					// We are intercepting connect messages (at a low-level) to detect new nodes. Upon receiving
					// a connection message from a node, we will automatically create and subscribe to these new
					// channels and we will mark the new node as being registered to those channels.
					//System.out.println("############## Got RPUB Connect Msg #############");
					RPubConnectMessage connectMessage = (RPubConnectMessage)(broadcastMessage.getPayload());
					// Register broadcast, private unicast and subscription
					createAndRegisterChannel(AbstractRPubManager.getBroadcastChannelName());
					createAndRegisterChannel(AbstractRPubManager.getUnicastChannelName(connectMessage.getSourceID()));
					createAndRegisterChannel(AbstractRPubManager.getSubscriptionChannelName(connectMessage.getSourceID()));
					// Mark the node as a subscriber
					registerSubscriberForChannel(message.getSourceID(), AbstractRPubManager.getBroadcastChannelName());
					registerSubscriberForChannel(message.getSourceID(), AbstractRPubManager.getUnicastChannelName(connectMessage.getSourceID()));
					registerSubscriberForChannel(message.getSourceID(), AbstractRPubManager.getSubscriptionChannelName(connectMessage.getSourceID()));
				}
			} else if (message instanceof RPubPublishMessage) {
				RPubPublishMessage publishMessage = (RPubPublishMessage)message;
				
				// Check if payload is UpdateRetentionRatiosControlMessage
				/*
				if (publishMessage.getPayload() instanceof UpdateRetentionRatiosControlMessage) {
					processUpdateRetentionRatiosControlMessage((UpdateRetentionRatiosControlMessage)(publishMessage.getPayload()));
				}*/
				
			}
			
			// In all cases, increase the number of publications and sent messages
			// Retrieve the channel or create it if it doesn't exist
			Channel channel = getAndCreateChannel(channelName);

			// Initialize the time slice
			int time = getCurrentTimeSlice();
			channel.initializeSliceStats(time);
			SliceStats stats = channel.getSliceStats(time);
			
			// Increment publications and sent (which is 1*subscribers) globally
			stats.getPublicationStats().incrementPublications(1);
			int sentMsg = 1 * stats.getSubscribers();
			stats.getPublicationStats().incrementSentMessages(sentMsg);
			
			// Increment publications and sent for the current publisher. Add the publisher
			// to the list of publishers if not there.
			if (stats.getPublishers().containsKey(message.getSourceID()) == false ) {
				stats.getPublishers().put(message.getSourceID(), new SliceStatsPublicationCounter());
			}
			SliceStatsPublicationCounter publicationStats = stats.getPublishers().get(message.getSourceID());
			// Set publications, sentMessages for publisher
			publicationStats.incrementPublications(1);
			publicationStats.incrementSentMessages(sentMsg);
			
			// Cumulative
			stats.incrementCumulativePublications(1);
			stats.incrementCumulativeSentMessages(sentMsg);
			
			// Calc bytein and byteout
			int byteIn = rawMessageSize;
			int byteOut = stats.getSubscribers() * rawMessageSize;
			
			// Don't increment byteIn if channel is loadbalancer-channel because those messages are issued from the localhost
			// TODO: if the Redis host we are bound to IS NOT LOCATED on the same machine as this LLA, then we HAVE TO
			// increment byteIn !
			if (channelName.equals("loadbalancer-channel") == false) {					
				stats.getPublicationStats().incrementByteIn(byteIn);
				stats.incrementCumulativeByteIn(byteIn);
				
				// Set byteIn for publisher
				publicationStats.incrementByteIn(byteIn);
				
				//System.out.println("channel= " + channelName + " | sub= " + stats.getSubscribers() + " | byteIn += " + rawMessageSize);
			}
			stats.getPublicationStats().incrementByteOut(byteOut);					
			stats.incrementCumulativeByteOut(byteOut);
			
			// Set byteOut for publisher
			publicationStats.incrementByteOut(byteOut);

			// Forward publication if needed
			if (!message.isForward()) {
				if (DynamothRPubManager.LAZY_PLAN_PROPAGATION){
					forwardPublicationWithReplication(channelName, message);
					//forwardPublication(channelName, message);
				}
			} else {
				this.forwardMessageCount++;
			}
			
			// COSTANALYZER - if channel starts with tile_ AND doesn't end with "_L"
			// if channel has retention ratio info of 1.0 then retain all, otherwise
			// filter using our random generator.
			if (shouldForwardToLowChannel(channelName)) {
				double retentionRatio = getRetentionRatio(channelName);
				
				// FORWARD (make it simple - same server - but could it work on a different server? Maybe!)
				// Do not set forwarding flag because it is not a "Dynamoth" forwarding
				// TODO: COMPARE A DIRECT SUBSCRIPTION AGAINST FORWARDING
				
				if (RPubUtil.getRandom().nextDouble() < retentionRatio) {
					// Forward message
					
					RPubDataMessage dataMessage = (RPubDataMessage) message;
					try {
						this.engine.send(channelName + "_L", dataMessage.getPayload());
					} catch (ClosedChannelException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (NoSuchChannelException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
			
			// DEBUGGING STUFF			
			// Timing stats output for debugging only
			if (channelName.startsWith("tile_2_0")
					|| channelName.startsWith("replication-test")) {
				if (messageCounterTime != time) {
					messageCounterTime = time;
					System.out.println("Publication count - processLowLevelMessage - for time unit " + messageCounterTime + ": " + messageCounter);
					messageCounter = 0;
				}
				messageCounter++;
			}
			
			// DEBUG: if msg is publication and contains a RGameMoveMessage, then unicast a "reply" message to the source
			// This reply message will allow the source to measure the time
			/*
			if (message instanceof RPubPublishMessage ) {
				RPubPublishMessage publishMessage = (RPubPublishMessage)message;
				if (publishMessage.getPayload() instanceof RGameMoveMessage) {
					RGameMoveMessage moveMessage = (RGameMoveMessage)(publishMessage.getPayload());
					int hash = moveMessage.hashCode();
					PerformanceReplyMessage perfMessage = new PerformanceReplyMessage(hash);
					// Unicast to source
					//engine.send(publishMessage, object)
				}
			}*/
			
			// Dump if we shall dump / DISABLED
			if (System.currentTimeMillis() > nextDump) {
				//dumpToCSVFile(10);
				nextDump = System.currentTimeMillis() + 10000;
			}
		}
	}
	
	/**
	 * Forward publication to appropriate shard, if needed.
	 * WARNING: WILL NOT WORK WITH CHANNEL REPLICATION!
	 * @param channelName
	 * @param message
	 */
	private void forwardPublication(String channelName, RPubMessage message) {
		synchronized (engineLock) {
			Plan plan = getPlan();
			if (plan.isCorrectShard(this.id, channelName)) {
				this.correctMessageCount++;
				
				// Forward using previous plan: to shard defined in past plan if it changed
				// We must look at the planHistory in the Dynamoth RPub Manager
				List<Plan> planHistory = getPlanHistory();
				if (planHistory.size()>1) {
					
					// Take the previous plan
					Plan previousPlan = planHistory.get(planHistory.size()-2);
					
					// Forward to first shard defined in previousPlan if it is different from our shardid!
					RPubClientId previousShard = previousPlan.getMapping(channelName).getShards()[0];
					if (this.id.equals(previousShard) == false) {

						DynamothRPubManager dynamoth = (DynamothRPubManager) (this.engine.getRPubManager());
						
						// Forward
						if (message instanceof RPubDataMessage) {
							RPubDataMessage dataMessage = (RPubDataMessage) message;
							dataMessage.setForward(true);
							dynamoth.publishToShards(dynamoth.buildShards(new RPubClientId[] {previousShard}), channelName, dataMessage);
						}
						
						// Ask old subscribers to switch to new plan
						PlanMapping mapping = new PlanMappingImpl((PlanMappingImpl)(plan.getMapping(channelName)));
						ChangeChannelMappingControlMessage changeMessage = new ChangeChannelMappingControlMessage(channelName, mapping, id);
						System.out.println("******** ASKING TO SWITCH ***********");
						
						dynamoth.publishToShards(dynamoth.buildShards(new RPubClientId[] {previousShard}), channelName, new RPubPublishMessage((RPubNetworkID) this.engine.getId(), changeMessage));
						
					}
					
				}
				
				
			} else {
				this.incorrectMessageCount++;
				
				// CAUSES A SEVERE BOTTLENECK! WHAT SHOULD WE DO ??
				// Forward using current plan: to shard defined in current plan
				if (message instanceof RPubDataMessage) {
					RPubDataMessage dataMessage = (RPubDataMessage) message;
					try {
						engine.send(channelName, dataMessage.getPayload(), true);
					} catch (ClosedChannelException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (NoSuchChannelException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				// Ask all subscribers to switch
				// Get the mapping as defined in Dynamoth's plan
				PlanMapping mapping = new PlanMappingImpl((PlanMappingImpl)(plan.getMapping(channelName)));
				ChangeChannelMappingControlMessage changeMessage = new ChangeChannelMappingControlMessage(channelName, mapping, id);
				
				// Send the publication through this lla engine so that it is sent to (old) subscribers,
				// which are subscribers still bound to THIS server
				
				try {
					this.llaEngine.send(channelName, changeMessage);
				} catch (ClosedChannelException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NoSuchChannelException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// Ask this specific publisher to switch - we do this because it might happen that a given
				// publisher is not a subscriber the given channel - in that case, he would not receive the
				// request to switch.
				
				try {
					this.engine.send(message.getSourceID(), changeMessage);
				} catch (ClosedChannelException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NoSuchClientException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}
	
	private void forwardPublicationWithReplication(String channelName, RPubMessage message) {
		synchronized(engineLock) {

			//No need to forward unsubscribes
			if(message instanceof RPubUnsubscribeMessage) {
				//System.out.println(message + channelName);
				return;
			}
			
			//If it is a subscription message get the actual channelName
			if(message instanceof RPubSubscriptionMessage) {
				channelName = ((RPubSubscriptionMessage) message).getChannelName();
			}
			if(channelName.startsWith("plan-push-channel-lla"))
				return;
			
			//System.out.println(message + channelName);

			//Get history
			List<Plan> planHistory = getPlanHistory();
			
			//If more than one plan && message has plan id and the planid is equal or bigger than 0 than we can do forwarding
			if(planHistory.size() > 1 && message.getPlanID() != null && message.getPlanID().getId() >= 0  && message.getPlanID().getId() < planHistory.size())  {

				Set<RPubClientId> previousShards = new HashSet<RPubClientId>();
				
				Plan previousPlan = planHistory.get(planHistory.size()-2);
					
				// Forward to first shard defined in previousPlan if it is different from our shardid!
				previousShards.addAll(Arrays.asList(previousPlan.getMapping(channelName).getShards()));
				
				DynamothRPubManager dynamoth = (DynamothRPubManager) (this.engine.getRPubManager());
	
				//Get current plan
				PlanMapping channelPlan = getPlan().getMapping(channelName);
				//get clients plan
				PlanMapping clientPlan = planHistory.get(message.getPlanID().getId()).getMapping(channelName); // get client plan from message
				
				//Get the set of servers that received this message
				Set<RPubClientId> serversAlreadyReceived = new HashSet<RPubClientId>();
				serversAlreadyReceived.add(this.id);
				if(clientPlan.getStrategy() == PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED) {
					serversAlreadyReceived.addAll(Arrays.asList(clientPlan.getShards()));
				}
				
				boolean isForwarder = this.id.equals(chooseForwarder(serversAlreadyReceived));
				//if clients plan != current plan

				if(channelPlan.getPlanId().equals(clientPlan.getPlanId())) {
					this.correctMessageCount++;
				} else if(clientPlan.getPlanId().compareTo(channelPlan.getPlanId()) == -1){
					this.incorrectMessageCount++;
					//Get all servers this server has to forward the message to
					Set<RPubClientId> forwardList = getForwardList(channelPlan, serversAlreadyReceived);
					
					//Get this servers split of that list
					List<RPubClientId> forwardTo = getForwardListSplit(new ArrayList<RPubClientId>(forwardList), serversAlreadyReceived);
					
					if (message instanceof RPubDataMessage && !forwardTo.isEmpty()) {
						//System.out.println("forwarding message");
						RPubDataMessage dataMessage = (RPubDataMessage) message;
						dataMessage.setForward(true);
						//dynamoth.publishToShards(dynamoth.buildShards(forwardTo.toArray(new RPubClientId[]{})), channelName, dataMessage);
					}
					
					//send update message
					ChangeChannelMappingControlMessage changeMessage = new ChangeChannelMappingControlMessage(channelName, channelPlan, id);
					if(isForwarder) {
						//send update
						//System.out.println(clientPlan.getPlanId().getId());
						//System.out.println("update message");
						RPubPublishMessage pubChangeMessage = new RPubPublishMessage((RPubNetworkID) this.engine.getId(), changeMessage);
						pubChangeMessage.setForward(true);
						if(!channelPlan.getPlanId().equals(lastUpdateSent.get(channelName))) {
							//System.out.println("inform everyone");
							lastUpdateSent.put(channelName, channelPlan.getPlanId());
							dynamoth.publishToShards(dynamoth.buildShards(previousShards.toArray(new RPubClientId[]{})), channelName, new RPubPublishMessage((RPubNetworkID) this.engine.getId(), changeMessage));
						}
						try {
							this.engine.send(message.getSourceID(), changeMessage);
						} catch (ClosedChannelException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (NoSuchClientException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				previousShards.removeAll(serversAlreadyReceived);

				//send to previous
				if(isForwarder && !previousShards.isEmpty() && message instanceof RPubDataMessage) {
					//System.out.println("Send to previous");
					RPubDataMessage dataMessage = (RPubDataMessage) message;
					dataMessage.setForward(true);
					//dynamoth.publishToShards(dynamoth.buildShards(previousShards.toArray(new RPubClientId[]{})), channelName, dataMessage);
				}
			}
		}
	}

	private Set<RPubClientId> getForwardList(PlanMapping channelPlan, Set<RPubClientId> serversAlreadyReceived) {
		Set<RPubClientId> forwardList = new HashSet<RPubClientId>();

		//Determine the forward list
		if(channelPlan.getStrategy() == PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED
				|| channelPlan.getStrategy() == PlanMappingStrategy.DEFAULT_STRATEGY) {
			//If one of the servers already received the message then we don't need to forward
			boolean mustForward = true;
			for(RPubClientId c : channelPlan.getShards()) {
				if(serversAlreadyReceived.contains(c)) {
					mustForward = false;
					break;
				}
			}
			if(mustForward) {
				forwardList.add(RandomShardSelector.INSTANCE.selector(channelPlan, 0)[0]);
			}
		} else if(channelPlan.getStrategy() == PlanMappingStrategy.SUBSCRIBERS_FULLY_CONNECTED) {
			forwardList.addAll(Arrays.asList(channelPlan.getShards()));
			forwardList.removeAll(serversAlreadyReceived);
		}
		return forwardList;
	}

	private RPubClientId chooseForwarder(Set<RPubClientId> serversAlreadyReceived) {
		RPubClientId chosen = null;
		for(RPubClientId f : serversAlreadyReceived) {
			if(chosen == null) {
				chosen = f;
			} else if(f.compareTo(chosen) < 0) {
				chosen = f;
			}
		}
		return chosen;
	}

	private List<RPubClientId> getForwardListSplit(List<RPubClientId> forwardList, Set<RPubClientId> serversAlreadyReceived) {
		List<RPubClientId> forwarders = new ArrayList<RPubClientId>(serversAlreadyReceived);
		Collections.sort(forwardList);
		Collections.sort(forwarders);
		int numAlreadyReceived = forwarders.size();
		List<RPubClientId> forwardListSplit = new ArrayList<RPubClientId>();
		for(int i = 0; i < forwardList.size(); i++) {
			RPubClientId forwarder = forwarders.get(i % numAlreadyReceived);
			if(this.id.equals(forwarder)) {
				forwardListSplit.add(forwardList.get(i));
			}
		}
		return forwardListSplit;
	}
	
	private Plan getPlan() {
		return ((DynamothRPubManager) this.engine.getRPubManager()).getCurrentPlan();	
	}
	
	private List<Plan> getPlanHistory() {
		return ((DynamothRPubManager) this.engine.getRPubManager()).getPlanHistory();
	}
	
	private double getRetentionRatio(String channelName) {
		synchronized(retentionRatiosLock) {
			Double ratio = this.retentionRatios.get(channelName);
			if (ratio==null)
				return 1.0;
			else
				return ratio;
		}
	}

	/*
	 * Return true (forward) if cost analyzer is enabled and channel is "tile_" (but does not end with "_L")
	 */
	private boolean shouldForwardToLowChannel(String channelName) {
		return ( CostAnalyzer.shouldEnable() && channelName.startsWith("tile_") && (channelName.endsWith("_L") == false) );
	}
}
