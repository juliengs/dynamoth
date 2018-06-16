package Dynamoth.Core.Manager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import Dynamoth.Client.Client;
import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Core.Availability.AvailabilityConfiguration;
import Dynamoth.Core.Availability.FailureDetector;
import Dynamoth.Core.Availability.FailureListener;
import Dynamoth.Core.Availability.PastPublication;
import Dynamoth.Core.RPubDataMessage;
import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubMessageListener;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.RPubPublishMessage;
import Dynamoth.Core.RPubSubscribeMessage;
import Dynamoth.Core.RPubSubscriptionMessage;
import Dynamoth.Core.RPubUnsubscribeMessage;
import Dynamoth.Core.Client.JedisRPubClient;
import Dynamoth.Core.Client.RPubClient;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.ControlMessages.AddRPubClientControlMessage;
import Dynamoth.Core.ControlMessages.ChangeChannelMappingControlMessage;
import Dynamoth.Core.ControlMessages.ChangePlanControlMessage;
import Dynamoth.Core.ControlMessages.ControlMessage;
import Dynamoth.Core.ControlMessages.CreateChannelControlMessage;
import Dynamoth.Core.ControlMessages.RemoveRPubClientControlMessage;
import Dynamoth.Core.ControlMessages.UnsubscribeFromAllChannelsControlMessage;
import Dynamoth.Core.Game.RConfig;
import Dynamoth.Core.Game.Messages.RGameMoveMessage;
import Dynamoth.Core.LoadAnalyzing.AbstractResponseTimeTracker;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanDiff;
import Dynamoth.Core.Manager.Plan.PlanDiffImpl;
import Dynamoth.Core.Manager.Plan.PlanId;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.Util.RPubHostInfo;
import Dynamoth.Core.Util.ShardHasher;
import Dynamoth.Util.Properties.PropertyManager;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DynamothRPubManager extends AbstractRPubManager {
	
	public static boolean WILDCARD_CHANNEL_SUBSCRIPTION = true;
	public static boolean LAZY_PLAN_PROPAGATION = true;
	public boolean DebugForceRerouteTileMessages = false;
		
	/**
	 * Map of all rpub clients:
	 * - client ID
	 * - jedis RPub client instance
	 * Not all clients might be active at the same time!
	 * When we receive appropriate control messages, we might add/remove clients from this list.
	 */
	public Map<RPubClientId, JedisRPubClient> rpubClients = new HashMap<RPubClientId, JedisRPubClient>();
	
	/**
	 * Current plan
	 * Perhaps eventually we should keep a list of all plans?
	 */
	private Plan currentPlan = null;
	
	/**
	 * History of past (unexpired) plans
	 */
	private List<Plan> planHistory = new ArrayList<Plan>();
	
	/**
	 * Lock object that should be locked when manipulating / using the current plan
	 * Prevents subscriptions while the plan is being changed
	 */
	private Object planChangeLock = new Object();
	
	/**
	 * List of channels that we are currently subscribed to
	 */
	private Set<String> currentSubscriptions = new HashSet<String>();
	
	/**
	 * List of past publications (for availability support, if enabled)
	 */
	private List<PastPublication> pastPublications = new LinkedList<PastPublication>();
	private Object pastPublicationsLock = new Object();
	
	/**
	 * Past received messages so that we can eliminate duplicates (up to a certain extent...)
	 */
	private LinkedHashSet<Integer> pastIncomingPublications = new LinkedHashSet<Integer>();
	private Object pastIncomingPublicationsLock = new Object();
	
	/**
	 * Failure detector
	 */
	private FailureDetector failureDetector = null;

	/**
	 * Server that is currently fake-failed
	 */
	private RPubClientId fakeFailedServer = null;
	
	/**
	 * Plan corresponding to the server that is currently fake-failed
	 */
	private Plan fakeFailedPlan = null;
	
	// --- Reconfiguration stuff
	/**
	 * Failed server that triggered a reconfiguration
	 */
	private RPubClientId reconfigureFailedServer = null;
	/**
	 * Plan that was active when the reconfiguration process was kicked-in
	 */
	private Plan reconfigureFailedPlan = null;
	
	public static final int STATE_BEFORE_FAILURE = 0;
	public static final int STATE_FAILURE_UNDETECTED = 1;
	public static final int STATE_FAILURE_DETECTED = 2;
	public static final int STATE_SUBSCRIPTIONS_REESTABLISHED = 3;
	public static final int STATE_AFTER_RECOVERY = 4;
	
	private int failureState = STATE_BEFORE_FAILURE;
	
	
	/**
	 * Only one failure is permitted 
	 */
	
	/**
	 * Our RPub message listener which invokes the 'real' listener. This listener
	 * is also able to intercept some messages.
	 */
	RPubMessageListener customListener = null;

	public DynamothRPubManager(RPubNetworkID networkID,
			RPubMessageListener messageListener) {
		super(networkID, messageListener);
		// Read properties file
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		
		// Load initial servers (rpub clients will connect to those servers when the app is starting)
		String rawServers = StringUtils.strip(
				props.getProperty("network.rpub.dynamoth.initial_servers"));
		
		// Create a custom message listener to be able to intercept plan change requests (hack!)
		// We also need to be able to intercept incoming RPub subscription/unsubscription messages
		// We do so because we have to maintain an internal list of subscriptions to be able to switch
		// subscriptions from one node to the other when needed.
		// 2014-09-02: our custom listener will also be used to intercept incoming messages to measure
		// the response time, using the ResponseTimeTracker. Such measurements will work only if
		// multiple clients are run on the same machine.
		// Also use it to feed messages to the Failure Detector!
		customListener = new RPubMessageListener() {
			
			@Override
			public void messageReceived(String channelName, RPubMessage message, int rawMessageSize) {
				
				// If message already exists, then don't do anything! Skip message processing...
				synchronized(pastIncomingPublicationsLock) {
					if (pastIncomingPublications.contains(message.getMessageID())) {
						return;
					}
				}
				
				// Checker whether we have a data message
				if (message instanceof RPubDataMessage) {
					// Get the payload
					Serializable payload = ((RPubDataMessage)message).getPayload();
					
					// If the payload is an instanceof ControlMessage
					if (payload instanceof ControlMessage) {
						// Call our handler
						processControlMessage((ControlMessage)payload);
					}
				} else if (message instanceof RPubSubscriptionMessage) {
					// We have a subscription message - process it
					processSubscriptionMessage((RPubSubscriptionMessage)message);
				}
				
				// Notify the tracker of the incoming message. The tracker will update it's stats.
				AbstractResponseTimeTracker.getInstance().addIncomingMessage(message, channelName);
				
				// Failure detector
				if (failureDetector != null)
					failureDetector.feedMessage(message);
				
				// Put message in past incoming messages
				synchronized(pastIncomingPublicationsLock) {
					pastIncomingPublications.add(message.getMessageID());
					// If size > THRESHOLD remove one
					if (pastIncomingPublications.size() > 10000) {
						pastIncomingPublications.iterator().remove();
					}
				}
				
				// Redirect message to the original listener
				getMessageListener().messageReceived(channelName, message, rawMessageSize);
				
			}
		};
		
		// Prepare initial jedis clients
		for (String server: rawServers.split(";")) {
			// Build host info
			RPubHostInfo hostInfo = new RPubHostInfo(server);
			
			// Create our jedis instance
			JedisRPubClient client = new JedisRPubClient(this.getNetworkID(), hostInfo.getClientId(), 1, hostInfo.getHostName(), hostInfo.getPort(), hostInfo.getDomain(), customListener);
			// Add it
			rpubClients.put(hostInfo.getClientId(), client);
		}
		
		// Create the default plan
		PlanImpl plan = new PlanImpl(new PlanId(0));
		plan.setMapping("track-info", new PlanMappingImpl(new PlanId(0), "track-info", new RPubClientId(2)));
		//plan.setMapping("replication-test-default", new PlanMappingImpl(new PlanId(0), "replication-test-default", new RPubClientId[]{new RPubClientId(0)}, PlanMappingStrategy.DEFAULT_STRATEGY));
		//plan.setMapping("replication-test-pfc", new PlanMappingImpl(new PlanId(0), "replication-test-pfc", new RPubClientId[]{new RPubClientId(0), new RPubClientId(1), new RPubClientId(2)}, PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED));
		//plan.setMapping("replication-test-sfc", new PlanMappingImpl(new PlanId(0), "replication-test-sfc", new RPubClientId[]{new RPubClientId(0), new RPubClientId(1), new RPubClientId(2)}, PlanMappingStrategy.SUBSCRIBERS_FULLY_CONNECTED));
		//plan.setMapping("replication-test-dynamic", new PlanMappingImpl(new PlanId(0), "replication-test-dynamic", new RPubClientId[]{new RPubClientId(0), new RPubClientId(1), new RPubClientId(2)}, PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED));
		//plan.setMapping("replication-test-dynamic", new PlanMappingImpl(new PlanId(0), "replication-test-dynamic", new RPubClientId[]{new RPubClientId(0)}, PlanMappingStrategy.DEFAULT_STRATEGY));
		plan.setMapping("tile_0_0DUMMY", new PlanMappingImpl(new PlanId(0), "tile_0_0DUMMY", new RPubClientId(1)));
		
		// Set all tile_i_j_B to RPubClientId1
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				//plan.setMapping("tile_" + i + "_" + j + "|A", new PlanMappingImpl(new PlanId(0), "tile_" + i + "_" + j + "|A", new RPubClientId(0)));
				//plan.setMapping("tile_" + i + "_" + j + "|B", new PlanMappingImpl(new PlanId(0), "tile_" + i + "_" + j + "|B", new RPubClientId(1)));
				/*if (i==0 && j==0) {
					// No change... keep one tile in (0,0)
				} else {
					plan.setMapping("tile_" + i + "_" + j + "", new PlanMappingImpl(new PlanId(0), "tile_" + i + "_" + j + "", new RPubClientId(1)));
				}*/
				/*plan.setMapping("tile_" + i + "_" + j, new PlanMappingImpl(new PlanId(0), "tile_" + i + "_" + j, new RPubClientId[] {
						new RPubClientId(0),  new RPubClientId(1)
						}, PlanMappingStrategy.DYNAWAN_ROUTING));*/
				
				// Use the shard hasher to randomize the initial assignment
				RPubClient[] shards = getAllActiveShards();
				// Add them to a list except "2" and the one that failed
				List<RPubClientId> shardIds = new LinkedList<RPubClientId>();
				for (RPubClient shard : shards) {
					if (shard.getId().getId() != 2) {
						shardIds.add(shard.getId());
					}
				}

				String channel = "tile_" + i + "_" + j + "";
				RPubClientId shardId = ShardHasher.HashShard(channel, shardIds.toArray(new RPubClientId[] {}));
				// Override if i<4... to put more clients on shard #1
				/*if (i<4) {
					shardId = new RPubClientId(1);
				}*/
				//shardId = new RPubClientId(1);
				//plan.setMapping(channel, new PlanMappingImpl(new PlanId(0), channel, shardId));
			}
		}
		
		// Enable WAN Replication for tile_0_0
		
		/*plan.setMapping("tile_0_0", new PlanMappingImpl(new PlanId(0), "tile_0_0", new RPubClientId[] {
			new RPubClientId(0), new RPubClientId(3)
			}, PlanMappingStrategy.DYNAWAN_ROUTING));
		*/
		
		
		
		currentPlan = plan;
		fakeFailedPlan = new PlanImpl((PlanImpl)currentPlan);

		// Put the default plan in the history
		this.planHistory.add(plan);
		
		// Setup failure detector
		//setupFailureDetector();
	}
	
	/**
	 * Add or remove the channel to our list of current subscriptions
	 * @param subscriptionMessage RPub subscription message
	 */
	private void processSubscriptionMessage(RPubSubscriptionMessage subscriptionMessage) {
		// If we are in infrastructure mode, then registrations are not modified!
		if (subscriptionMessage.isInfrastructure())
			return;
	
		synchronized(planChangeLock) {
			
			if (subscriptionMessage instanceof RPubSubscribeMessage) {
				// Register subscription to channel
				System.out.println("AddToCurrentSubscriptions->" + subscriptionMessage.getChannelName());
				this.currentSubscriptions.add(subscriptionMessage.getChannelName());
			} else if (subscriptionMessage instanceof RPubUnsubscribeMessage) {
				// Remove channel subscription
				System.out.println("RemoveFromCurrentSubscriptions->" + subscriptionMessage.getChannelName());
				this.currentSubscriptions.remove(subscriptionMessage.getChannelName());
			}
			
		}
	}
	
	private void processUnsubscribeAllChannelsControlMessage(UnsubscribeFromAllChannelsControlMessage unsubscribeMessage) {
		RPubClient shard = this.rpubClients.get(unsubscribeMessage.getClientId());
		System.err.println(AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()) + " ### UNSUBSCRIBING ALL CHANNELS FOR THIS SHARD!");
		unsubscribeFromAllChannels(shard);
	}
	
	/**
	 * Process an incoming RPub control message
	 * (dispatches to other method)
	 * @param controlMessage RPub control message
	 */
	private void processControlMessage(ControlMessage controlMessage) {
		if (controlMessage instanceof ChangePlanControlMessage) {
			processChangePlanControlMessage((ChangePlanControlMessage)controlMessage);
		} else if (controlMessage instanceof AddRPubClientControlMessage) {
			processAddRPubClientControlMessage((AddRPubClientControlMessage)controlMessage);
		} else if (controlMessage instanceof RemoveRPubClientControlMessage) {
			processRemoveRPubClientControlMessage((RemoveRPubClientControlMessage)controlMessage);
		} else if (controlMessage instanceof ChangeChannelMappingControlMessage) {
			processChangeChannelMappingControlMessage((ChangeChannelMappingControlMessage) controlMessage);
		} else if (controlMessage instanceof UnsubscribeFromAllChannelsControlMessage) {
			processUnsubscribeAllChannelsControlMessage((UnsubscribeFromAllChannelsControlMessage)controlMessage);
		}
	}
	
	public void applyPlan(Plan newPlan) {
		// Time-stamp plan
		newPlan.setTime((int)(Math.round(System.currentTimeMillis() / 1000.0)));
		
		synchronized (planChangeLock) {
			
			// Compute planDiff
			PlanDiff planDiff = new PlanDiffImpl(this.currentPlan, newPlan, this.hashCode());
			
			Plan oldPlan = this.currentPlan;
			
			// Put the new plan in the plan history
			this.planHistory.add(newPlan);
			
			// Switch to new plan
			setCurrentPlan(newPlan);
			
			
			// Connect new channels
			// For each shard...
			for (RPubClientId clientId: planDiff.getShards()) {
				// For each subscription channel
				for (String channel: planDiff.getOwnSubscriptions(clientId, currentSubscriptions)) {
					// Create channel just-in-case
					createChannel(channel);
					System.out.println("Subscribing to " + channel + " on " + clientId.getId());
					// Subscription is performed on 'current' shard with infrastructure mode
					subscribeClientToChannel(getNetworkID(), channel, true);
				}
				
			}
			
			// Switch to old plan
			setCurrentPlan(oldPlan);
			
			// TODO: FIND OUT WHY UNSUBS ARE NOT WORKING PROPERLY
			// HINT: if the channel was not defined in the previous plan,
			// then it is possible that the unsub cannot be sent!
			// We should assume plan0.
			// 2014-05-27: Should have been fixed, LoadBalancer auto-adds info for all unknown channels
			// and the new plan contains those info. 
			
			// Disconnect old channels
			// For each shard...
			for (RPubClientId clientId: planDiff.getShards()) {
				// For each unsubscription channel
				for (String channel: planDiff.getOwnUnsubscriptions(clientId, currentSubscriptions)) {
					// Unsubscription is performed on 'current' shard with infrastructure mode
					unsubscribeClientFromChannel(getNetworkID(), channel, true);
					System.out.println("Unsubscribing to " + channel + " on " + clientId.getId());

				}
				
			}
			
			// Switch again to new plan
			setCurrentPlan(newPlan);
			
			System.out.println("Applying plan " + this.currentPlan);
			
			// DEBUG: inform some listener that we changed the plan
			//publishToChannel("dynamoth-debug", new RPubPublishMessage(getNetworkID(), new PlanAppliedControlMessage()));
			
			// DEBUG: print the planId of all current mappings
			/*
			System.out.println("---Begin printing planId of all mappings---");
			for (String channel: currentPlan.getAllChannels()) {
				System.out.println("   " + channel + " (" + currentPlan.getMapping(channel).getPlanId().getId() + ")");
			}
			System.out.println("---End printing planId of all mappings---");
			*/

			System.out.println("---Begin printing channels---");
			for (RPubClientId clientId: currentPlan.getAllShards()) {
				for (String channel: currentPlan.getClientChannels(clientId)) {
					System.out.println("   " + channel + " -> RPubClientId" + clientId.getId());
				}
			}
			System.out.println("---End printing channels---");
		}
	}
	
	/**
	 * Apply a partial plan
	 * @param controlMessage
	 */
	private void processChangeChannelMappingControlMessage(ChangeChannelMappingControlMessage controlMessage) {
		synchronized (planChangeLock) {
			
			//if (controlMessage.getChannel().startsWith("tile_"))
			//	System.out.println("Change channel mapping request (src=RPubClientId" + controlMessage.getSourceClientId().getId() + "): " + controlMessage.getChannel() + "->" + controlMessage.getMapping().getShards()[0].getId());
			
			// Create a dummy new plan based on the existing plan to reuse our plan change algo
			PlanImpl plan = new PlanImpl((PlanImpl) currentPlan);
			plan.setMapping(controlMessage.getChannel(), controlMessage.getMapping());
			
			//Update the plan id of the overall plan
			PlanId newPlanId = new PlanId(Math.max(controlMessage.getMapping().getPlanId().getId(), plan.getPlanId().getId()));
			plan.setPlanId(newPlanId);

			// Apply it
			applyPlan(plan);
		}
	}
	
	/**
	 * Apply a new plan
	 */
	private void processChangePlanControlMessage(ChangePlanControlMessage changePlanControlMessage) {
		
		applyPlan(changePlanControlMessage.getNewPlan());
	}
	
	private void processAddRPubClientControlMessage(AddRPubClientControlMessage message) {
		// Add the new rpub client
		JedisRPubClient client = new JedisRPubClient(this.getNetworkID(), message.getClientId(), 1, message.getHostName(), message.getHostPort(), "", customListener);
		rpubClients.put(message.getClientId(), client);
		// Connect it ** SHOULD BE REMOVED ** because we will only connect when it's needed
		//client.connect();
		System.out.println("Adding new RPubClient " + message.getClientId());
	}
	
	private void processRemoveRPubClientControlMessage(RemoveRPubClientControlMessage message) {
		// Disconnect the rpub client if it was connected
		if (rpubClients.get(message.getClientId()).isConnected()) {
			rpubClients.get(message.getClientId()).disconnect();
		}
		// Remove the rpub client
		rpubClients.remove(message.getClientId());
		System.out.println("Removing RPubClient " + message.getClientId());
	}
	
	public Plan getCurrentPlan() {
		return this.currentPlan;
	}
	
	public void setCurrentPlan(Plan currentPlan) {
		this.currentPlan = currentPlan;
	}
	
	public List<Plan> getPlanHistory() {
		return this.planHistory;
	}
	
	public RPubClientId getHashedShardId(String channelName) {
		// Get shard info
		
		int hashCode = channelName.hashCode();
		if (hashCode < 0)
			hashCode = -hashCode;
		
		int shard = hashCode % rpubClients.size();
		//hashShardCount[shard].incrementAndGet();
		
		System.out.println("Sharding index: " + shard );

		return new RPubClientId(shard);
		//return hashCode % (redisHosts.size()-1) + 1;
	}
	
	private RPubClient getHashedShard(String channelName) {
		return this.rpubClients.get(getHashedShardId(channelName));
	}

	@Override
	public void initialize() {
		// Create and connect all initially-registered Jedis nodes
		for (Map.Entry<RPubClientId,JedisRPubClient> entry: this.rpubClients.entrySet() ) {
			entry.getValue().connect();
		}
		
		// Subscribe ourself to the plan push channel
		//publishToSubscriptionChannel(getNetworkID(), new RPubSubscribeMessage((RPubNetworkID)(getNetworkID()), "plan-push-channel"));
	}

	@Override
	public void createChannel(String channelName) {
		
		synchronized (planChangeLock) {
		
			// Publish the channel creation to the LLAs
			// Obtain all the RPub clients that are handling 'channelName'
			// Publish the CreateChannelControlMessage to them over the LoadAnalyzer channel.
			//for (RPubClient client: this.getPublicationShards(channelName)) {
			for (RPubClient client: buildShards(currentPlan.getMapping(channelName).getShards())) {
				client.publishToChannel("loadanalyzer-channel", new RPubPublishMessage(this.getNetworkID(), new CreateChannelControlMessage(channelName)));
			}
			
		}
	}

	@Override
	public RPubClient[] getPublicationShards(String channelName) {
		// Get the shards that shall be used for publication messages
		// Under the Dynamoth Model, the current shard corresponding to -channelName- shall be used
		
		synchronized (planChangeLock) {
						
			if (DebugForceRerouteTileMessages && channelName.startsWith("tile_")) {
				return buildShards(new RPubClientId[] {new RPubClientId(1)});
			}
			PlanMapping mapping = currentPlan.getMapping(channelName);
			RPubClientId[] shards = mapping.getShards();

			/*
			if (channelName.equals("tile_0_0")) {
				System.out.print("PubToShards: ");
				for (RPubClientId id: mapping.getStrategy().selectPublicationShards(mapping, this.hashCode())) {
					System.out.print(id.getId() + ",");
				}
				System.out.print("\n");
			}
			*/
			return buildShards(mapping.getStrategy().selectPublicationShards(mapping, this.hashCode()));
		
		}
	}

	@Override
	public RPubClient[] getSubscriptionShards(String channelName) {
		// Get the shards that shall be used for subscription messages
		// Under the Dynamoth Model, the shard corresponding to -channelName- shall be used
		// (same as publication shard)
		
		synchronized (planChangeLock) {
			
			PlanMapping mapping = currentPlan.getMapping(channelName);
			RPubClientId[] shards = mapping.getShards();

			//return new RPubClient[] { this.getHashedShard(channelName) };
			if (channelName.startsWith("tile_")) {
				RPubClient[] bs = buildShards(mapping.getStrategy().selectSubscriptionShards(mapping, this.hashCode()));
				System.out.println("getSubscriptionShards [" + channelName + "]: " + bs.length + " | " + bs[0].getId().getId());
				
			}
			return buildShards(mapping.getStrategy().selectSubscriptionShards(mapping, this.hashCode()));
		
		}
	}

	@Override
	public RPubClient[] getAllActiveShards() {
		// Returns all active shards
		
		return this.rpubClients.values().toArray(new RPubClient[] {}); 
	}
	
	public RPubClient[] buildShards(RPubClientId[] shardIDs) {
		RPubClient[] clients = new RPubClient[shardIDs.length];
		for (int i=0; i<shardIDs.length; i++) {
			clients[i] = this.rpubClients.get(shardIDs[i]);
		}
		return clients;
	}

	// Override prePublishToChannel and postPublishToChannel so that we can make sure the client
	// is connected before issuing the message
	@Override
	protected boolean prePublishToChannel(RPubClient client, String channelName, RPubMessage message) {
		// Ensure client is connected
		if (client.isConnected() == false) {
			client.connect();
		}
		String actualChannelName = channelName;
		if(message instanceof RPubSubscriptionMessage) {
			actualChannelName = ((RPubSubscriptionMessage) message).getChannelName();
		}
		PlanId channelPlanId = currentPlan.getMapping(actualChannelName).getPlanId();
		message.setPlanID(channelPlanId);
		// If message is publication and contains a RGameMoveMessage, then record the hash and time in a
		// global hash table
		if (message instanceof RPubPublishMessage ) {
			RPubPublishMessage publishMessage = (RPubPublishMessage)message;
			if (publishMessage.getPayload() instanceof RGameMoveMessage) { 
				
				// For failure handling code, remove the following line because we don't always collect
				AbstractResponseTimeTracker.getInstance().addOutgoingMessage(message, channelName);
								
				// Initiate response time collection if one of the 3 following is true:
				// * Collect response times while failed
				// * No server is currently failing (between fake failure time and until recovery)
				// * The channel is NOT associated to the failing server
				//if (AvailabilityConfiguration.getInstance().isCollectResponseTimesWhileFailed() || fakeFailedServer == null || isFakeFailedChannel(channelName) == false) {
				
				// Should be collecting response times in the current state?
				if (AvailabilityConfiguration.getInstance().isCollectingResponseTimes(failureState)) {
				
					// Should we collect only from "failed" channels? Or from all?
					if (AvailabilityConfiguration.getInstance().isCollectResponseTimesOnlyFromFailedChannels() == false /* Always collect, otherwise, make sure it is a failed channel */ ||
							isChannelMappedToShard(channelName, fakeFailedPlan, new RPubClientId(1))) {
					
						// Put in global response time tracker
						//AbstractResponseTimeTracker.getInstance().addOutgoingMessage(message, channelName);
					
					}
				}
			}
		}
		
		// Set message's source domain
		message.setSourceDomain(System.getProperty("ec2.region", ""));
		
		// Set message's rpub domain
		message.setRpubServerDomain(((JedisRPubClient) client).getJedisDomain());
		
		// Set message's rpub server that processed it
		message.setRpubServer(client.getId());
		
		// Put message in "publication history"... so that in case of failure, it will be re-sent
		// (only for tile_ channels... this setting should go in the appropriate settings file!)
		if (message instanceof RPubPublishMessage && channelName.startsWith("tile_")) {
			synchronized(pastPublicationsLock) {
				int currentTime = AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime());
				pastPublications.add(new PastPublication(channelName, message, currentTime));

				// If we are reconfiguring, and the channel itself was subject to a reconfiguration (!new)
				// then prevent the publication... Once reconfiguration has ended, then
				// process past publications.
				if (reconfigureFailedServer != null && isReconfiguringChannel(channelName)) {
					if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.PLAYBACK_ORDERED) {
						// Playing in orderded mode - return false since we want the publication to be prevented from being played!
						return false;
						
					} else if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.PLAYBACK_CONCURRENT) {
						// Playing in concurrent mode - pop it from the past publication queue since we want to send it right away!
						pastPublications.remove(pastPublications.size()-1);
					}
				}
			}
			
			// Clear old past publications.
			// Maybe doing it at this point is not the best idea...
			// Ideally transform this into a loop at some point...
			clearOldPastPublications();
		}
		
		return true;
	}

	@Override
	protected void postPublishToChannel(RPubClient client, String channelName, RPubMessage message) {
		
	}

	@Override
	public void publishToSubscriptionChannel(NetworkEngineID networkID,
			RPubSubscriptionMessage message) {
		// TODO Auto-generated method stub
		synchronized (planChangeLock) {
			PlanId channelPlanId = currentPlan.getMapping(message.getChannelName()).getPlanId();
			message.setPlanID(channelPlanId);
			super.publishToSubscriptionChannel(networkID, message);
		}
	}
	
	/**
	 * Setups the failure detector.
	 * (Check whether it should be enabled...)
	 */
	private void setupFailureDetector() {
		failureDetector = new FailureDetector();
		
		failureDetector.addFailureListener(new FailureListener() {
			@Override
			public void failureDetected(RPubClientId clientId) {
			
				// Failure was detected!
				// Proceed to reconfigure
				System.out.println("-->NewlyFailedServer: " + clientId.getId());
				
				// Wait a bit... suspend the transmission of new publications
				reconfigureFailedServer = clientId;
				reconfigureFailedPlan = new PlanImpl((PlanImpl)currentPlan);
				
				failureState = STATE_FAILURE_DETECTED;
				
				// Remap all [tile] channels using hashing.
				// This is done by modifying the current plan
				reconfigureFailedSubscriptions(clientId);
				
				failureState = STATE_AFTER_RECOVERY;
				
				failureDetector.resetServerMonitoring();
				
			}
		});
	}
	
	/**
	 * Clears old past publications.
	 * Peek the first one and pop it as long as the elapsed time is > pastPublicationsTime.
	 */
	private void clearOldPastPublications() {
		while (true) { /* Dangerous :-) */
			// Lock, then peek the first one
			synchronized(pastPublicationsLock) {
				// If empty then break right away
				if (pastPublications.isEmpty())
					break;
				
				// Peek the first one
				PastPublication pastPublication = pastPublications.get(0);
				int currentTime = AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime());
				int duration = currentTime - pastPublication.getTime();
				// If duration > threshold then pop the first item... and keep looping
				if (duration > AvailabilityConfiguration.getInstance().getPastPublicationsTime()) {
					// Pop it
					pastPublications.remove(0);
				} else {
					// Break loop -- no more publications should be removed...
					break;
				}
			}
		}
	}
	
	/**
	 * Reconfigure all subscriptions following server failure.
	 */
	private void reconfigureFailedSubscriptions(RPubClientId failedServer) {
				
		// Go through all subscriptions *for that server* and move them using hashing
		// TODO: Warning - might be thread-unsafe
		Set<String> subscriptions = new HashSet<String>(this.getCurrentSubscriptions());
		for (String subscription: subscriptions) {
			// If starts by tile (!)
			if (subscription.startsWith("tile")) {
				// Look in the plan
				PlanMapping mapping = this.currentPlan.getMapping(subscription);
				// If mapping corresponds to failedServer, with no replication
				// TODO: implement replicated case
				if (mapping.getShards().length == 1 && mapping.getShards()[0].equals(failedServer)) {
					
					if (subscription.equals("tile_0_0")) {
						//System.out.println("INCORRECT RECONF FOR TILE00: oldShard="mapping.getShards()[0].getId());
					}
					
					// Use the ShardHasher to obtain the correct shard
					RPubClient[] shards = getAllActiveShards();
					// Add them to a list except "2" and the one that failed
					List<RPubClientId> shardIds = new LinkedList<RPubClientId>();
					for (RPubClient shard : shards) {
						if (shard.getId().getId() != 2 && shard.getId().getId() != failedServer.getId()) {
							shardIds.add(shard.getId());
						}
					}

					RPubClientId shardId = ShardHasher.HashShard(subscription, shardIds.toArray(new RPubClientId[] {}));
					
					// Alter local plan! Do this by faking a change plan control mapping request
					PlanMapping newMapping = new PlanMappingImpl(this.getCurrentPlan().getPlanId(), subscription, shardId);

					processChangeChannelMappingControlMessage(new ChangeChannelMappingControlMessage(subscription, newMapping, failedServer));
				}
			}
		}
		
		try {
			// ARTIFICIALLY WAIT AND THEN PLAYBACK OLD PUBLICATIONS
			Thread.sleep(1500);
		} catch (InterruptedException ex) {
			Logger.getLogger(DynamothRPubManager.class.getName()).log(Level.SEVERE, null, ex);
		}
		
		fakeFailedServer = null;
		failureState = STATE_SUBSCRIPTIONS_REESTABLISHED;
		
		
		
		// If we are not replaying old publications, then bail out right away
		if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.NO_PLAYBACK) {
			reconfigureFailedServer = null;
			reconfigureFailedPlan = null;	
			return;
		}
		
		// Start playback
		System.out.println("STARTING PUBLICATION REPLAYING.........");
		// Print last publication ID
		synchronized(pastPublicationsLock) {
			if (pastPublications.size() > 0) {
				RGameMoveMessage moveMsg = (RGameMoveMessage) ( ((RPubPublishMessage)(pastPublications.get(0).getMessage())).getPayload() );
				System.out.println(AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()) + " WILL FIRST REPLAY MESSAGE ID: " + moveMsg.getMessageId());
				moveMsg = (RGameMoveMessage) ( ((RPubPublishMessage)(pastPublications.get(pastPublications.size()-1).getMessage())).getPayload() );
				System.out.println(AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()) + " WILL LAST REPLAY MESSAGE ID: " + moveMsg.getMessageId());
				
			}
		}
		
		// Playback old publications
		int currentTime = AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime());
		boolean published = false;
		while (true) { // Not really an infinite loop
			published = false;
			
			synchronized(pastPublicationsLock) {
				// If no more publications, then disable reconfiguration mode and break the infinite loop
				// Publishers will then be allowed to publish and publications will not be blocked anymore
				if (pastPublications.size() == 0) {
					reconfigureFailedServer = null;
					reconfigureFailedPlan = null;	
					System.out.println(AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()) + " REPLAYED ALL PUBLICATIONS.........");
					break;
				}
				
				//System.out.println("A-Replaying publications, remaining=" + pastPublications.size());
				
				// Attempts to get the first past publication (at this point in the code there should be at least one)
				PastPublication pastPublication = null;
				
				if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.PLAYBACK_ORDERED) {
					pastPublication = pastPublications.get(0);
				} else if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.PLAYBACK_CONCURRENT) {
					pastPublication = pastPublications.get(pastPublications.size()-1);
				}
				
				//System.out.println("B-Channel=" + pastPublication.getChannel() + " Shard=" + reconfigureFailedPlan.getMapping(pastPublication.getChannel()).getShards()[0].getId());
				if ( isReconfiguringChannel(pastPublication.getChannel()) ) {
					// Only if time matches...
					if (currentTime - pastPublication.getTime() < AvailabilityConfiguration.getInstance().getPastPublicationsTime()) {
						
						// Resend it according to the new plan
						RPubClient[] shards = getPublicationShards(pastPublication.getChannel());
						//System.out.println("C-Should publish to shard " + shards[0].getId().getId());
						for (RPubClient shard: shards) {
							shard.publishToChannel(pastPublication.getChannel(), pastPublication.getMessage());
							//System.out.println("D-Publishing to shard " + shard.getId().getId());
						}
				
						System.out.println(AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()) + "          PUBLISHED");
						published = true;

					} else {
						System.out.println(AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()) + "          SKIPPED");
					}
				}
				
				//Remove this past publication from the list since it has been sent
				// Note: not adequate since it will also remove publications for other servers which might be needed if they do fail!
				if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.PLAYBACK_ORDERED) {
					pastPublications.remove(0);
				} else if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.PLAYBACK_CONCURRENT) {
					pastPublications.remove(pastPublications.size()-1);
				}
				
			}
			
			if (published) {
				int waitTime = 0;
				if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.PLAYBACK_ORDERED) {
					waitTime = AvailabilityConfiguration.getInstance().getPlaybackOrderedWaitTime();
				} else if (AvailabilityConfiguration.getInstance().getPlaybackMode() == AvailabilityConfiguration.PlaybackMode.PLAYBACK_CONCURRENT) {
					waitTime = AvailabilityConfiguration.getInstance().getPlaybackConcurrentWaitTime();
				}


				try {
					Thread.sleep(waitTime);
				} catch (InterruptedException ex) {
					Logger.getLogger(DynamothRPubManager.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
			
		}
	}
	
	private boolean isChannelMappedToShard(String channel, Plan plan, RPubClientId shard) {
		return (plan.getMapping(channel) != null && plan.getMapping(channel).getShards()[0].getId() == shard.getId() );
	}
	
	private boolean isFakeFailedChannel(String channel) {
		return isChannelMappedToShard(channel, fakeFailedPlan, fakeFailedServer);
	}
	
	/**
	 * While reconfiguration is taking place, returns whether a given channel is being reconfigured.
	 * A reconfigured channel is a channel associated to the failing server.
	 * @param channel Channel
	 * @return True if the channel was associated to the failing server.
	 */
	private boolean isReconfiguringChannel(String channel) {
		return isChannelMappedToShard(channel, reconfigureFailedPlan, reconfigureFailedServer);
	}
	
	/**
	 * Unsubscribe form all channels for a given shard.
	 * Redis allows us to do so.
	 * 
	 * THIS IS ONLY USED TO SIMULATE A SERVER DISCONNECTION (CRASH)!
	 * 
	 * @param client Shard to unsubscribe from
	 */
	public void unsubscribeFromAllChannels(RPubClient shard) {
		fakeFailedServer = shard.getId();
		failureState = STATE_FAILURE_UNDETECTED;
		this.unsubscribeClientFromAllChannels(this.getNetworkID(), shard);
	}
	
}
