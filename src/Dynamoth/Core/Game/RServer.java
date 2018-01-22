package Dynamoth.Core.Game;

import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.ChannelExistsException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchClientException;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.Client.JedisRPubClient;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.ControlMessages.ChangePlanControlMessage;
import Dynamoth.Core.ControlMessages.LoadBalancerStatsControlMessage;
import Dynamoth.Core.ControlMessages.TrackInfoControlMessage;
import Dynamoth.Core.ControlMessages.Debug.PlanAppliedControlMessage;
import Dynamoth.Core.ControlMessages.Debug.RPlayerCrashedControlMessage;
import Dynamoth.Core.ControlMessages.Debug.SubscribedToChannelControlMessage;
import Dynamoth.Core.ControlMessages.UnsubscribeFromAllChannelsControlMessage;
import Dynamoth.Core.Game.Messages.RGameAcquirePlayerMessage;
import Dynamoth.Core.Game.Messages.RGameActivateMessage;
import Dynamoth.Core.Game.Messages.RGameUpdateFlockInfoMessage;
import Dynamoth.Core.Game.Messages.RGameUpdatePlayerInfoMessage;
import Dynamoth.Core.Manager.AbstractRPubManager;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Core.Manager.Plan.PlanId;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Util.Message.Handler;
import Dynamoth.Util.Message.Message;
import Dynamoth.Util.Message.Reactor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Very primitive RGame server
 *  
 * @author Julien Gascon-Samson
 *
 */
public class RServer {

	private static final int LOAD_RADIO_COUNT = 8;
	private static final int MAX_LATENCY_OUTPUT = 600;
	private static final int LATENCY_INCREMENT_OUTPUT = 50;
	
	private RPubNetworkEngine engine;
	private Reactor reactor;
	
	private int playerIdLimit = 0;
	
	private int nextPlayerId = 0;
	private Object nextPlayerIdLock = new Object();
	
	private AtomicInteger planAppliedCounter = new AtomicInteger(0);
	private AtomicInteger subscribedToChannelCounter = new AtomicInteger(0);
	
	private Queue<RGameAcquirePlayerMessage> nextPlayerDeliveryQueue = new LinkedList<RGameAcquirePlayerMessage>();
	
	private HashSet<Integer> subscribedAuthorizedSet = new HashSet<Integer>();
	private HashSet<Integer> subscribedConfirmedSet = new HashSet<Integer>();
	
	private long startTime = 0;
	
	private int outputCurrentTime = 0;
	private int outputSum = 0;
	private int outputCount = 0;
	
	// List of all response time measurements to compute percentile
	private List<Integer> responseTimes = new ArrayList<Integer>();
	private Object responseTimesLock = new Object();
	
	private int moveMessageCount = 0;
	private Map<Integer, Integer> latenciesUnder = new HashMap<Integer, Integer>();
	
	private AtomicLong totalInMessage = new AtomicLong(0);
	private AtomicLong totalOutMessage = new AtomicLong(0);
	private AtomicLong computedByteIn = new AtomicLong(0);
	private AtomicLong computedByteOut = new AtomicLong(0);
	private AtomicLong measuredByteIn = new AtomicLong(0);
	private AtomicLong measuredByteOut = new AtomicLong(0);
	private AtomicBoolean rebalancingTriggered = new AtomicBoolean(false);
	private AtomicInteger hostCount = new AtomicInteger(1);
	private double[] loadRatios = new double[] {};
	private Object loadRatiosLock = new Object();
	
	private AtomicInteger disabledPlayerCount = new AtomicInteger(0);
	
	private AtomicInteger flockingActivePlayerCount = new AtomicInteger(0);
	
	
	
	// Output file
	private Writer outputFileWriter = null;
	
	public RServer() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// Create and launch server
		for (int i=0; i<8; i++)
			for (int j=0; j<8; j++)
				System.out.print("tile_" + i + "_" + j + ":1 ");
		RServer server = new RServer();
		server.launch();
	}
	
	public void launch() {
		// Connect to the network
		startTime = System.nanoTime();
		try {
			outputFileWriter = new FileWriter("RServer_ResponseTime.log");
			outputFileWriter.write("Time" + ";" + "PlayerCount" + ";" + "TotalInMessage" + ";" + "TotalOutMessage" + ";" + "TotalOutMessage/50" + ";"  + "ComputedByteIn" + ";"  + "ComputedByteOut" + ";"  + "MeasuredByteIn" + ";"  + "MeasuredByteOut" + ";");
			for (int i=0; i<LOAD_RADIO_COUNT; i++) {
				outputFileWriter.write("LoadRatio" + i + ";");
			}
			outputFileWriter.write("LoadRatio" + ";");
			for (int i=LATENCY_INCREMENT_OUTPUT; i<=MAX_LATENCY_OUTPUT; i+=LATENCY_INCREMENT_OUTPUT) {
				outputFileWriter.write("ResponseTimeUnder" + i + ";");
			}
			outputFileWriter.write("AvgResponseTime" + ";" + "PercentileResponseTime75" + ";" + "PercentileResponseTime95" + ";" + "RebalancingTriggered*1000" + ";" + "HostCount*125" + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		engine = new RPubNetworkEngine();
    	try {
			engine.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AlreadyConnectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	   
    	
    	// Reactor
    	reactor = new Reactor("RServerReactor", engine);
    	
    	// Create the acquirePlayers-query if it doesn't exist :-)
    	try {
			engine.createChannel("acquirePlayers-query");
		} catch (ChannelExistsException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		};
    	
    	// Subscribe myself to the acquirePlayers-query channel
    	try {
			engine.subscribeChannel("acquirePlayers-query", engine.getId());
		} catch (NoSuchChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	// Subscribe to dynamoth-debug channel
    	try {
			engine.subscribeChannel("dynamoth-debug", engine.getId());
		} catch (NoSuchChannelException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	// Subscribe to track-info channel
    	try {
			engine.subscribeChannel("track-info", engine.getId());
		} catch (NoSuchChannelException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	// Subscribe to the load-balancer-stats channel to receive info about messages
    	try {
			engine.subscribeChannel("load-balancer-stats", engine.getId());
		} catch (NoSuchChannelException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	// Register for incoming RGameAcquirePlayerMessages
    	reactor.register(RGameAcquirePlayerMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleGameAcquirePlayerMessage((RGameAcquirePlayerMessage)msg);		
			}
		});
    	
    	// Register for debug plan applied control message
    	reactor.register(PlanAppliedControlMessage.class, new Handler() {

			@Override
			public void handle(Message msg) {
				handlePlanAppliedControlMessage((PlanAppliedControlMessage)msg);				
			}
    		
    	});
    	// Register for RPlayer crashed control message
    	reactor.register(RPlayerCrashedControlMessage.class, new Handler() {

			@Override
			public void handle(Message msg) {
				handleRPlayerCrashedControlMessage((RPlayerCrashedControlMessage)msg);				
			}
    		
    	});
    	// Register for Subscribed to channel control messages
    	reactor.register(SubscribedToChannelControlMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleSubscribedToChannelControlMessage((SubscribedToChannelControlMessage)msg);				
			}
		});
    	// Register for track info control messages
    	reactor.register(TrackInfoControlMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleTrackInfoControlMessage((TrackInfoControlMessage)msg);
			}
		});
    	// Register for load-balancer-stats control messages
    	reactor.register(LoadBalancerStatsControlMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleLoadBalancerStatsControlMessage((LoadBalancerStatsControlMessage)msg);
			}
		});
    	
    	// Process consoleout
    	System.out.println("<Entering console mode>");
    	Console c = System.console();
    	while (true) {
    		String line = c.readLine();
    		processCommand(line);
    	}
	}

	private void processCommand(String line) {
		if (line.equals("exit")) {
			System.exit(0);
		} else if (line.startsWith("+")) { //+n
			this.subscribedToChannelCounter.set(0);
			try {
				int increment = Integer.parseInt(line.substring(1));
				// Increase limit by n
				synchronized(this.nextPlayerIdLock) {
					this.playerIdLimit += increment;
					processDeliveryQueue();
				}
			}
			catch (NumberFormatException ex) {
				System.out.println("Invalid increment!");
			}
			
			// Send player count
			try {
				engine.send("loadbalancer-channel", new RGameUpdatePlayerInfoMessage(nextPlayerId-disabledPlayerCount.get(), flockingActivePlayerCount.get()));
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
						
		} else if (line.toLowerCase().startsWith("flock ")) { //flock (0,0)=1 (0,1)=1
			// Our new hotspot
			RPlayerFlockInfo flockInfo = new RPlayerFlockInfo();
			
			// Eliminate "flock "
			line = line.substring("flock ".length());
			// Split according to all spaces
			String[] hotspots = line.split(" ");
			// For each hotspot, find "="
			for (String hotspot: hotspots) {
				String tuple = hotspot.split("=")[0];
				int value = Integer.parseInt(hotspot.split("=")[1]);
				
				// Skip empty strings
				if (tuple.equals(""))
					continue;
				
				// Set appropriate variable based on tuple
				if (tuple.equals("()")) {
					// Assign Free Destination Weight
					flockInfo.setFreeDestinationWeight(value);
				} else if (tuple.equals("s")) {
					// Stay inside hotspot
					flockInfo.setStayInsideHotspotProbability(value / 100.0);
				} else {
					// Extract hotspot X and Y
					int hotspotX = Integer.parseInt(tuple.split(",")[0].substring(1));
					String hotspotYStr = tuple.split(",")[1];
					int hotspotY = Integer.parseInt(hotspotYStr.substring(0, hotspotYStr.length()-1));
					
					flockInfo.getFlockWeights().put(new RPlayerFlockInfoHotspot(hotspotX, hotspotY), value);
				}
			} 
			
			// Send game update flock info msg
			System.out.println("*** Sending Update Flock Info Message ***");
			try {
				engine.send("rgame-broadcast", new RGameUpdateFlockInfoMessage(flockInfo));
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
		} else if (line.toLowerCase().startsWith("updateplan ")) {
			// Informs the load balancer to apply a new plan based on the current plan (or send the plan directly)?
			// COMMAND-LINE: updateplan ch1:0 ch2:0 ch3:0 ch4:1 ch5:pfc(0,1)
			// For now we don't handle other schemes
			line = line.substring("updateplan ".length());
			String[] entries = line.split(" ");
			
			// Get the current plan from the network engine's manager
			DynamothRPubManager dynamothManager = (DynamothRPubManager)(engine.getRPubManager());
			PlanImpl currentPlan = (PlanImpl)(dynamothManager.getCurrentPlan());
			PlanImpl newPlan = new PlanImpl(currentPlan);
			// Increment ID for new plan
			newPlan.setPlanId(new PlanId(newPlan.getPlanId().getId()+1));
			// Apply the plan id to all mappings
			newPlan.applyPlanIdToMappings();
			
			// Update each entry
			for (String entry: entries) {
				String channel = entry.split(":")[0];
				
				try {
					int mapping = Integer.parseInt(entry.split(":")[1]);
					// Apply regular mapping (non-replicated)
					newPlan.setMapping(channel, new PlanMappingImpl(newPlan.getPlanId(), channel, new RPubClientId(mapping)));
				} catch (NumberFormatException nfex) {
					// Apply replicated mapping
					String mappingStr = entry.split(":")[1];
					System.out.println(mappingStr.substring(4, mappingStr.length()-1));
					String[] mappings = mappingStr.substring(4, mappingStr.length()-1).split(",");
					PlanMappingStrategy strategy = PlanMappingStrategy.DEFAULT_STRATEGY;
					if (mappingStr.startsWith("pfc")) {
						strategy = PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED;
					} else if (mappingStr.startsWith("sfc"))  {
						strategy = PlanMappingStrategy.SUBSCRIBERS_FULLY_CONNECTED;
					}
					RPubClientId[] clientIds = new RPubClientId[mappings.length];
					for (int i=0; i<clientIds.length; i++) {
						clientIds[i] = new RPubClientId(Integer.parseInt(mappings[i]));
					}
					// Apply plan with strategy
					newPlan.setMapping(channel, new PlanMappingImpl(newPlan.getPlanId(), channel, clientIds, strategy));
				}
				
			}
			
			// Set current plan in manager
			dynamothManager.setCurrentPlan(newPlan);
			
			this.planAppliedCounter.set(0);
			
			// Push the new plan
			
			/* WARNING -> the relabancer in the LoadBalancer will not be aware of the new plan so
			 * auto rebalancings will keep using the new plan. The LoadBalancer should push any new
			 * plan change to the rebalancer */
			
			String pushChannelName = "plan-push-channel";
			if (DynamothRPubManager.LAZY_PLAN_PROPAGATION) {
				pushChannelName = "plan-push-channel-lla";
			}
			
			ChangePlanControlMessage cpMessage = new ChangePlanControlMessage(newPlan);
			String ser;
			try {
				ser = JedisRPubClient.toString(cpMessage);
				ChangePlanControlMessage cpMessageCopy = (ChangePlanControlMessage) (JedisRPubClient.fromString(ser));
				ser = ser + "";
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				engine.send(pushChannelName, cpMessage);
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
			
			System.out.println("Pushed new plan...");
		} else if (line.toLowerCase().startsWith("match")) {
			Set<Integer> authorizedNotConfirmed = new HashSet<Integer>(this.subscribedAuthorizedSet);
			Set<Integer> confirmedNotAuthorized = new HashSet<Integer>(this.subscribedConfirmedSet);
			authorizedNotConfirmed.removeAll(subscribedConfirmedSet);
			confirmedNotAuthorized.removeAll(subscribedAuthorizedSet);
			System.out.println("AUTHORIZED NOT CONFIRMED:");
			for (Integer h: authorizedNotConfirmed) {
				System.out.println(h);
			}
			System.out.println("CONFIRMED NOT AUTHORIZED:");
			for (Integer h: confirmedNotAuthorized) {
				System.out.println(h);
			}
			
		} else if (line.toLowerCase().startsWith("deliveryqueue")) {
			Map<String,Integer> queues = new HashMap<String,Integer>();
			RGameAcquirePlayerMessage[] acquireMessages = nextPlayerDeliveryQueue.toArray(new RGameAcquirePlayerMessage[] {});
			for (RGameAcquirePlayerMessage acquireMessage: acquireMessages) {
				if (queues.containsKey(acquireMessage.getHostname()) == false) {
					queues.put(acquireMessage.getHostname(), 0);
				}
				queues.put(acquireMessage.getHostname(), queues.get(acquireMessage.getHostname())+1);
			}
			// Output size of all queues
			for (Map.Entry<String,Integer> entry: queues.entrySet()) {
				System.out.println(entry.getKey() + ": " + entry.getValue());
			}
		} else if (line.toLowerCase().startsWith("sleep ")) {
			// Sleep for a certain duration
			try {
				long duration = Long.parseLong(line.split(" ")[1]);
				Thread.sleep(duration);
				System.out.println("Slept!");
			} catch (Exception ex) {
				// Do nothing -> skip to next command
				System.out.println("Cannot sleep!");
			}
		} else if (line.toLowerCase().startsWith("exec ")) {
			// Open a file and exec all commands line by line
			String filename = line.split(" ")[1];
			try {
				
				BufferedReader reader = new BufferedReader(new FileReader(filename));
				
				try{
					String readLine = null;
					while (( readLine = reader.readLine()) != null) {
						
						// Execute line
						processCommand(readLine);
						
					}
				}
				finally {
					reader.close();
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (line.toLowerCase().startsWith("activate ")) {
			// Sleep for a certain duration
			try {
				int playerStart = Integer.parseInt(line.split(" ")[1]);
				int playerEnd = Integer.parseInt(line.split(" ")[2]);
				boolean active = Boolean.parseBoolean(line.split(" ")[3]);
				boolean flocking = true;
				String region = "";
				if (line.split(" ").length > 4) {
					flocking = Boolean.parseBoolean(line.split(" ")[4]);	
				}
				if (line.split(" ").length > 5) {
					region = line.split(" ")[5];
				}
				
				// Bound playerEnd according to nextPlayerId
				if (playerEnd > nextPlayerId) {
					playerEnd = nextPlayerId;
				}
				
				System.out.println("Activating...[" + playerStart + ";" + playerEnd + "[" + " value=" + active);
				
				// Send msg
				engine.send("rgame-broadcast", new RGameActivateMessage(playerStart, playerEnd, active, flocking, region));
				
				// Adjust our disabled player count and flocking count
				int count = playerEnd - playerStart;
				int flockingDiff = 0;
				if (active) {
					count = -count;
					if (flocking)
						flockingDiff += Math.abs(count);
					else
						flockingDiff -= Math.abs(count);
					
				}
				disabledPlayerCount.addAndGet(count);
				flockingActivePlayerCount.addAndGet(flockingDiff);
				
			} catch (Exception ex) {
				// Do nothing -> skip to next command
				System.out.println("Cannot activate!");
			}
			
			// Send player count
			try {
				engine.send("loadbalancer-channel", new RGameUpdatePlayerInfoMessage(nextPlayerId-disabledPlayerCount.get(), flockingActivePlayerCount.get()));
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
		} else if (line.toLowerCase().startsWith("unsubscribeall ")) {
			try {
				int rpubId = Integer.parseInt(line.split(" ")[1]);
				
				// Send the appropriate message
				engine.send(AbstractRPubManager.getBroadcastChannelName(), new UnsubscribeFromAllChannelsControlMessage(new RPubClientId(rpubId)));
				
			} catch (Exception ex) {
				// Do nothing -> skip to next command
				System.out.println("Cannot unsubscribe!");
			}
			
		}
	}
	
	private void processDeliveryQueue() {
		synchronized(this.nextPlayerIdLock) {
			
			// Check for limit
			while (this.nextPlayerId < playerIdLimit) {
			
				RGameAcquirePlayerMessage acquireMessage = this.nextPlayerDeliveryQueue.poll();
				if (acquireMessage == null) {
					// Queue is empty - do nothing
					return;
				}
				NetworkEngineID engineID = acquireMessage.getNetworkId();
				
				// Process item - accept player
				int nextId = this.nextPlayerId;
				
				System.out.println("Issuing player ID " + nextId);	
				
				this.nextPlayerId++;
				
				try {
					engine.send(engineID, new RGameAcquirePlayerMessage(nextId, this.engine.getId()));
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
	
	private int getLatencyPercentile(int percentile) {
		// Assumes list is sorted!
		synchronized(responseTimesLock) {
			int index = percentile * responseTimes.size() / 100;
			return responseTimes.get(index);
		}
		
	}
	
	private void handleGameAcquirePlayerMessage(RGameAcquirePlayerMessage msg) {
		// Make sure this is a query msg
		if (msg.isQuery()) {
			// Good !
			
			int nextId = -1;
			synchronized(this.nextPlayerIdLock) {
				nextId = this.nextPlayerId;
				
				// Check if we can send the id at this stage. It might not be the case because of the playerIdLimit.
				if (nextId >= playerIdLimit) {
					// Enqueue instead of sending
					//System.out.println("Enqueuing player " + nextId + " because we cannot accept anymore players at this stage!" + "|" + msg.getHostname());
					System.out.println("Enqueuing player [" + this.nextPlayerDeliveryQueue.size() + "] | " + msg.getHostname());
					this.nextPlayerDeliveryQueue.add(msg);
					return;
				}
				
				this.nextPlayerId++;
			}
			
			// Send the next available client ID
			
			//System.out.println("Issuing player ID " + nextId + "|" + msg.getHostname());
			//System.out.println(msg.getNetworkId().hashCode() + "A");
			subscribedAuthorizedSet.add(msg.getNetworkId().hashCode());
			
			try {
				engine.send(msg.getNetworkId(), new RGameAcquirePlayerMessage(nextId, this.engine.getId()));
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
				
		} else {
			// Bad, should never happen !
		}
	}
	
	private void handlePlanAppliedControlMessage(PlanAppliedControlMessage msg) {
		int value = this.planAppliedCounter.incrementAndGet();
		//System.out.println("PlanAppliedCounter:" + value);
	}
	
	private void handleRPlayerCrashedControlMessage(RPlayerCrashedControlMessage msg) {
		System.out.println("CLIENT CRASHED !!!");
	}
	
	private void handleSubscribedToChannelControlMessage(SubscribedToChannelControlMessage msg) {
		int value = this.subscribedToChannelCounter.incrementAndGet();
		//System.out.println("SubscribedToPlanPushChannel:" + value);
		//System.out.println(-(msg.getNetworkId().hashCode()) + "B");
		subscribedConfirmedSet.add(msg.getNetworkId().hashCode());
	}
	
	private void handleTrackInfoControlMessage(TrackInfoControlMessage msg) {
		long currentTime = System.nanoTime() - startTime;
		int currentTimeSeconds = (int)(Math.round( currentTime/1000000000.0 ));
		if (msg.getAverageTime() >= 0) {
			// Get current time relative to startTime
			System.out.println(currentTimeSeconds + ";" + nextPlayerId + ";" + msg.getAverageTime());
			
			// Add to hashmap of latencies under X for each measurement
			for (int responseTime: msg.getResponseTimes()) {
				// Add to sum
				outputSum += responseTime;
				outputCount++;
				
				// Add to list
				synchronized(responseTimesLock) {
					responseTimes.add(responseTime);
				}
				
				for (int i=LATENCY_INCREMENT_OUTPUT; i<=MAX_LATENCY_OUTPUT; i+=LATENCY_INCREMENT_OUTPUT) {
					if (responseTime <= i) {
						if (latenciesUnder.containsKey(i) == false)
							latenciesUnder.put(i, 1);
						else {
							latenciesUnder.put(i, latenciesUnder.get(i) + 1);
						}
					}	
				}
			}
			
			
		}
		//else {
			//outputSum += 1000;
			//outputCount++;
		//}
		this.moveMessageCount += msg.getMoveMessageCount();
		
		// *** Should we write to output ***
		if (currentTimeSeconds > outputCurrentTime) {
			
			System.out.println(">>> TICM, OutputCount=" + outputCount);
			
			// Only write if outputCount > 0 otherwise skip
			if (outputCount > 0) {
				int average = (int)( Math.round(outputSum * 1.0 / outputCount) );
				int rebalancingValue = 0;
				if (this.rebalancingTriggered.getAndSet(false))
					rebalancingValue = 1;
				
				// Write to output
				try {
					int playerCount = nextPlayerId - disabledPlayerCount.get();
					// Output stuff
					outputFileWriter.write(outputCurrentTime + ";");
					outputFileWriter.write(playerCount + ";");
					outputFileWriter.write(this.totalInMessage.get() + ";");
					outputFileWriter.write(this.totalOutMessage.get() + ";");
					outputFileWriter.write(this.totalOutMessage.get()/50 + ";");
					
					outputFileWriter.write(this.computedByteIn.get() + ";");
					outputFileWriter.write(this.computedByteOut.get() + ";");
					outputFileWriter.write(this.measuredByteIn.get() + ";");
					outputFileWriter.write(this.measuredByteOut.get() + ";");
					
					// For all load ratios... 
					synchronized(this.loadRatiosLock) {
						double sum = 0;
						for (int i=0; i<LOAD_RADIO_COUNT; i++) {
							double val;
							int index = 0;
							if (i<2)
								index=i;
							else
								index=i+1;
							if (index >= loadRatios.length) {
								val = 0;
							} else {
								val=loadRatios[index];
							}
							sum+=val;
							// Load-ratio (individual)
							outputFileWriter.write(val + ";");
						}
						// Load-ratio (avg)
						outputFileWriter.write(sum/hostCount.get() + ";");
					}
					
					// Write latencies under
					for (int i=LATENCY_INCREMENT_OUTPUT; i<=MAX_LATENCY_OUTPUT; i+=LATENCY_INCREMENT_OUTPUT) {
						if (latenciesUnder.containsKey(i) == false) {
							outputFileWriter.write(0 + ";");							
						} else {
							outputFileWriter.write(((float)(latenciesUnder.get(i)) / outputCount) + ";");
						}
							
					}
					outputFileWriter.write(average + ";");
					
					// Write percentile 75 and 95
					synchronized(responseTimesLock) {
						// Sort collection
						Collections.sort(responseTimes);
						outputFileWriter.write(getLatencyPercentile(75) + ";");
						outputFileWriter.write(getLatencyPercentile(95) + ";");
					}
					
					outputFileWriter.write(rebalancingValue*1000 + ";");
					outputFileWriter.write(hostCount.get()*125 + "\n");
					outputFileWriter.flush();
					//System.out.println("WROTE OUTPUT...");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		
			outputCurrentTime = currentTimeSeconds;
			outputSum = 0;
			outputCount = 0;
			moveMessageCount = 0;
			latenciesUnder.clear();
			synchronized(responseTimesLock) {
				responseTimes.clear();
			}
		}
		
		// Console-out missed / sent
		System.out.println("Sent: " + msg.getStateUpdatesSent() + " | Missed: " + msg.getStateUpdatesMissed());
	}
	
	private void handleLoadBalancerStatsControlMessage(LoadBalancerStatsControlMessage msg) {
		// Save # of incoming and outgoing messages, that's it
		
		if (msg.isRebalancingRequested()) {
			System.out.println("--> FROM LB: REBALANCING IS REQUESTED");
		}
		
		this.totalInMessage.set(msg.getInMessage());
		this.totalOutMessage.set(msg.getOutMessage());
		this.computedByteIn.set(msg.getComputedByteIn());
		this.computedByteOut.set(msg.getComputedByteOut());
		this.measuredByteIn.set(msg.getMeasuredByteIn());
		this.measuredByteOut.set(msg.getMeasuredByteOut());
		if (this.rebalancingTriggered.get() == false && msg.isRebalancingRequested())
			this.rebalancingTriggered.set(msg.isRebalancingRequested());
		this.hostCount.set(msg.getHostCount());
		synchronized (this.loadRatiosLock) {
			this.loadRatios = msg.getLoadRatios();
		}
	}
}
