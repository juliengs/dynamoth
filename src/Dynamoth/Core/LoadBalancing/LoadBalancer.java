package Dynamoth.Core.LoadBalancing;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang.StringUtils;

import Dynamoth.Client.Client;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.ChannelExistsException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.ControlMessages.AddRPubClientControlMessage;
import Dynamoth.Core.ControlMessages.ChangePlanControlMessage;
import Dynamoth.Core.ControlMessages.LLAUpdateControlMessage;
import Dynamoth.Core.ControlMessages.LoadBalancerStatsControlMessage;
import Dynamoth.Core.Game.RConfig;
import Dynamoth.Core.Game.Messages.RGameUpdatePlayerInfoMessage;
import Dynamoth.Core.LoadAnalyzing.Channel;
import Dynamoth.Core.LoadBalancing.CostModel.CostAnalyzer;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.AveragedLoadEvaluator;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.DiscreteLoadEvaluator;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.NewPlanEstimatedLoadEvaluator;
import Dynamoth.Core.LoadBalancing.Rebalancing.DynaWANLocationRebalancer;
import Dynamoth.Core.LoadBalancing.Rebalancing.DynaWANMessageCounter;
import Dynamoth.Core.LoadBalancing.Rebalancing.DynamothRebalancer;
import Dynamoth.Core.LoadBalancing.Rebalancing.LoadBasedRebalancer;
import Dynamoth.Core.LoadBalancing.Rebalancing.MultiPubRebalancer;
import Dynamoth.Core.LoadBalancing.Rebalancing.Rebalancer;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanId;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.Util.RPubHostInfo;
import Dynamoth.Core.Util.RPubUtil;
import Dynamoth.Util.Message.Handler;
import Dynamoth.Util.Message.Message;
import Dynamoth.Util.Message.Reactor;
import Dynamoth.Util.Properties.PropertyManager;

public class LoadBalancer {

	private RPubNetworkEngine engine = null;
	private Reactor reactor = null;
	
	// Timer to process LoadBalancer stuff
	private Timer timer = new Timer();
	
	// Map: RPubClientId->RPubHostInfo
	private Map<RPubClientId, RPubHostInfo> hostInfoMap = new HashMap<RPubClientId, RPubHostInfo>(); 
	
	// Map: RPubClientId->Channels (Channel: same thing as channel in LoadAnalyzing)
	private Map<RPubClientId, Map<String, Channel>> channels = new HashMap<RPubClientId, Map<String, Channel>>();
	
	// Time for which we have the latest full info available (for all channels)
	private Map<RPubClientId, Integer> clientLastUpdateTimes = new HashMap<RPubClientId, Integer>();
	
	// Sigar/local network interface stats
	private Map<RPubClientId, Map<Integer, Long>> measuredByteInMap  = new HashMap<RPubClientId, Map<Integer, Long>>();
	private Map<RPubClientId, Map<Integer, Long>> measuredByteOutMap = new HashMap<RPubClientId, Map<Integer, Long>>();
	private Map<RPubClientId, Long> cumulativeMeasuredByteIn  = new HashMap<RPubClientId, Long>();
	private Map<RPubClientId, Long> cumulativeMeasuredByteOut = new HashMap<RPubClientId, Long>();
	
	
	private int processTickCount = 0;
	
	// Load Evaluators
	private Map<Integer, LoadEvaluator> loadEvaluators = new LinkedHashMap<Integer, LoadEvaluator>();
	private int lastLoadEvaluatorTime = -1;
	
	// Rebalancer
	private Rebalancer rebalancer = null;
	
	// Cost analyzer
	private CostAnalyzer costAnalyzer = null;
	
	// Graph builder
	ChannelRelationGraphBuilder graphBuilder = null;
	
	// Player count
	private int playerCount = 0;
	private int flockingCount = 0;
	
	
	/**
	 * Queue of rpub clients that could be used if needed
	 * Eventually, we could have a priority queue if some machines would be more adequate than
	 * others (more powerful machines or cheaper datacenters for instance)
	 */
	private Queue<RPubClientId> rpubClientPool = new LinkedList<RPubClientId>(); 
	
	public LoadBalancer() {
		initRPubClientPool();
		
		// Schedule timer
		timer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				// Invoke the method
				processTimer();
			}
		}, 1000, 1000);
	}
	
	private long getMeasuredByteIn(RPubClientId clientId, int time) {
		if (measuredByteInMap.get(clientId) == null || measuredByteInMap.get(clientId).get(time) == null)
			return 0;
		else
			return measuredByteInMap.get(clientId).get(time);
	}
	
	private long getMeasuredByteOut(RPubClientId clientId, int time) {
		if (measuredByteOutMap.get(clientId) == null || measuredByteOutMap.get(clientId).get(time) == null)
			return 0;
		else
			return measuredByteOutMap.get(clientId).get(time);
	}
	
	private long getCumulativeMeasuredByteIn(RPubClientId clientId) {
		if (cumulativeMeasuredByteIn.get(clientId) == null)
			return 0;
		else
			return cumulativeMeasuredByteIn.get(clientId);
	}
	
	private long getCumulativeMeasuredByteOut(RPubClientId clientId) {
		if (cumulativeMeasuredByteOut.get(clientId) == null)
			return 0;
		else
			return cumulativeMeasuredByteOut.get(clientId);
	}	
	
	private synchronized void processTimer() {
		int currentTime = RPubUtil.getCurrentSystemTime();
		
		processTickCount += 1;
		
		// Evaluate load
		// TODO - WARNING: we might not yet have info for all the time points
		//this.loadEvaluators.
		LoadEvaluator loadEvaluator = new DiscreteLoadEvaluator(
				this.hostInfoMap,
				this.channels,
				this.measuredByteInMap,
				this.measuredByteOutMap,
				this.clientLastUpdateTimes,
				currentTime);
		this.loadEvaluators.put(currentTime, loadEvaluator);
		this.lastLoadEvaluatorTime = currentTime;
		
		// Build our averaged load evaluator
		LoadEvaluator avgLoadEvaluator = new AveragedLoadEvaluator(this.loadEvaluators, 10, currentTime);
		
		//System.out.println("*** LOAD ***");
		
		printLoadByteout(loadEvaluator, "D");
		
		//printLoadEvaluator(avgLoadEvaluator, "A");
		
		printLoadByteout(avgLoadEvaluator, "A");
		
		// Debug - merge missing channels if any
		((PlanImpl) getCurrentPlan()).mergeMissingChannels(this.channels);
		
		//System.out.println("*** End LoadEvaluator Dump ***");
		
		if (processTickCount == 30) {
			
			//pushDummyLBPlan(0);
			
		} else if (processTickCount == 60) {
			
			//pushDummyLBPlan(1);
			
		} else if (processTickCount == 2000) {
			// === GENERATE CFG GRAPH ===
			graphBuilder = new ChannelRelationGraphBuilder(this.channels, avgLoadEvaluator, currentTime);
			System.out.println(graphBuilder.generateOutput());
			String metisOutput = graphBuilder.generateMetisOutput();
			System.out.println(metisOutput);
			FileWriter fWriter;
			try {
				fWriter = new FileWriter("LoadBalancer.metis");
				
				fWriter.write(metisOutput);
				
				fWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (processTickCount == 5000 /* 50 */) {
			// Read LoadBalancer metis partition file
			try {
				String partMetisContents = ChannelRelationGraphBuilder.deserializeString(new File("LoadBalancer.metis.part.2"));
				
				// Create plan
				PlanImpl partMetisPlan = graphBuilder.generatePlanFromMetisPartition(getCurrentPlan(), partMetisContents);
				
				// Increment ID for new plan
				partMetisPlan.setPlanId(new PlanId(partMetisPlan.getPlanId().getId()+1));
				
				partMetisPlan.applyPlanIdToChangedMappings(getCurrentPlan());
				
				// Apply plan
				applyPlan(partMetisPlan);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		// === END GENERATE CFG GRAPH ===
		
		boolean rebalancingTriggered = false;
		int hostCount = 1;
		
		// If time > 10, start rebalancer if not started
		if (processTickCount >= 10 && this.rebalancer == null) {
			// Create and start rebalancer
			this.rebalancer = new MultiPubRebalancer(this.getCurrentPlan(), currentTime, avgLoadEvaluator, this.hostInfoMap);
			//this.rebalancer = new DynaWANLocationRebalancer(this.getCurrentPlan(), currentTime, avgLoadEvaluator, this.hostInfoMap);
			//this.rebalancer = new DynamothRebalancer(this.getCurrentPlan(), currentTime, avgLoadEvaluator, this.hostInfoMap);
			//this.rebalancer = new ConsistentHashingRebalancer(this.getCurrentPlan(), currentTime, avgLoadEvaluator, this.hostInfoMap);
			/*HierarchicalLoadBasedRebalancer hlb = new HierarchicalLoadBasedRebalancer(this.getCurrentPlan(), currentTime, avgLoadEvaluator, this.hostInfoMap);
			hlb.addRebalancer(new DynamothReplicationRebalancer(this.getCurrentPlan(), currentTime, avgLoadEvaluator, this.hostInfoMap));
			hlb.addRebalancer(new DynamothRebalancer(this.getCurrentPlan(), currentTime, avgLoadEvaluator, this.hostInfoMap));
			this.rebalancer = hlb;*/
			
			this.rebalancer.start();
			System.out.println("*** STARTING REBALANCER ***");
		} else if (this.rebalancer != null && this.rebalancer.isRunning()) {
			// Update some info
			if (this.rebalancer instanceof LoadBasedRebalancer) {
				LoadBasedRebalancer lbRebalancer = (LoadBasedRebalancer)(this.rebalancer);
				lbRebalancer.setCurrentLoadEvaluator(avgLoadEvaluator);
				lbRebalancer.setCurrentPlan(getCurrentPlan());
				lbRebalancer.setCurrentTime(currentTime);
			}
			
			// Check if we have a new plan to apply
			//System.out.println("Rebalancer: new plan available? " + this.rebalancer.isNewPlanAvailable());
			
			if (this.rebalancer.isNewPlanAvailable()) {
				PlanImpl proposedPlan = (PlanImpl)(this.rebalancer.getNewPlan());
				// Increment ID for new plan
				proposedPlan.setPlanId(new PlanId(proposedPlan.getPlanId().getId()+1));
				// Apply the plan id to all mappings
				proposedPlan.applyPlanIdToChangedMappings(this.rebalancer.getCurrentPlan());
				
				
				applyPlan(proposedPlan);
				rebalancingTriggered = true;
			}
		}
		
		// Clear old slice stats values
		synchronized (this.channels) {
			// For each rpub client
			for (Map.Entry<RPubClientId, Map<String, Channel>> channelMapEntry: this.channels.entrySet()) {
				// For each channel
				for (Map.Entry<String, Channel> channelEntry: channelMapEntry.getValue().entrySet()) {
					channelEntry.getValue().clearSliceStats(50);
				}
			}
		}
		
		// Publish our load-evaluators to whoever could be interested
		// Extract some stuff
		if (this.rebalancer instanceof LoadBasedRebalancer) {
			LoadBasedRebalancer lbRebalancer = (LoadBasedRebalancer)(this.rebalancer);
			hostCount = lbRebalancer.getActiveHosts().size();
			//System.out.println("RebalancerHostCount=" + hostCount);
		}
		try {
			engine.send("load-balancer-stats", prepareLoadBalancerStatsControlMessage(loadEvaluator, rebalancingTriggered, hostCount));
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
		
		// Update cost analyzer if it is enabled
		if (CostAnalyzer.shouldEnable()) {
			updateCostAnalyzer(currentTime, loadEvaluator);
		}
	}
	
	private void updateCostAnalyzer(int currentTime, LoadEvaluator evaluator) {
		if (this.costAnalyzer == null) {
			this.costAnalyzer = new CostAnalyzer(currentTime, this.engine);
			
		}
		
		// Compute global byteOut
		long byteOut = evaluator.getClientMeasuredByteOut(new RPubClientId(0));
		// Compute byteOut for every tile: avg byteOut * 60 seconds
		// Get # subscribers for every tile
		
		this.costAnalyzer.submitLoadData(currentTime, evaluator, playerCount, flockingCount);
		
		// Print current metric
		double accumMBReal = (long) (this.costAnalyzer.getAccumulatedTotalByteOut()/(1024*1024));
		double accumMBExtrapolated = (long) (this.costAnalyzer.getAccumulatedTotalXByteOut()/(1024*1024));
		double accumMBLow = (long) (this.costAnalyzer.getAccumulatedLowByteOut()/(1024*1024));
		double accumMBLowExtrapolated = (long) (this.costAnalyzer.getAccumulatedLowXByteOut()/(1024*1024));
		double plannedMB = (long) (this.costAnalyzer.getPlannedByteOutPeriod()/(1024*1024));
		System.out.println("CostAnalyzer: B_used_REAL=" + accumMBReal + " MB" + " | B_used_X=" + accumMBExtrapolated);
		System.out.println("CostAnalyzer: B_used_LOW=" + accumMBLow + " MB" + " | B_used_LOW_X=" + accumMBLowExtrapolated);
		System.out.println("B_planned=" + plannedMB + " MB");
		System.out.println("PlayerCount=" + costAnalyzer.getPlayerCount());
	}

	private void applyPlan(PlanImpl proposedPlan) {
		AveragedLoadEvaluator averagedLoadEvaluator = new AveragedLoadEvaluator(this.loadEvaluators, 10, this.lastLoadEvaluatorTime);
		
		// Apply it
		NewPlanEstimatedLoadEvaluator estimatedLoadEvaluator = new NewPlanEstimatedLoadEvaluator(this.getCurrentPlan(), proposedPlan, averagedLoadEvaluator, this.hostInfoMap, this.lastLoadEvaluatorTime);
		
		// Print load evaluator
		printLoadByteout(estimatedLoadEvaluator, "E");
		
		System.out.println("---Begin printing channels---");
		for (RPubClientId clientId: proposedPlan.getAllShards()) {
			if (clientId.getId() != 0 && clientId.getId() != 2) {
				for (String channel: proposedPlan.getClientChannels(clientId)) {
					System.out.print("   " + channel + " -> ");
					// Print each RPubClient...
					for (RPubClientId shard: proposedPlan.getMapping(channel).getShards()) {
						System.out.print("RPubClientId" + shard.getId() + " ");
					}
					System.out.println(proposedPlan.getMapping(channel).getStrategy().toString());
				}
			}
		}
		System.out.println("---End printing channels---");
		
		String pushChannelName = "plan-push-channel";
		if (DynamothRPubManager.LAZY_PLAN_PROPAGATION) {
			pushChannelName = "plan-push-channel-lla";
		}
		try {
			engine.send(pushChannelName, new ChangePlanControlMessage(proposedPlan));
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
		
		// Set current plan
		if (this.rebalancer != null) {
			this.rebalancer.setCurrentPlan(proposedPlan);
		}
		// Also override it in dynamoth manager because our engine will probably not receive the
		// ChangePlanControlMessage (engines do not receive their own broadcast)
		this.setCurrentPlan(proposedPlan);
		
	}
	
	private LoadBalancerStatsControlMessage prepareLoadBalancerStatsControlMessage(LoadEvaluator evaluator, boolean rebalancingTriggered, int hostCount) {
		long inMessage = 0;
		long outMessage = 0;
		long computedByteIn = 0;
		long computedByteOut = 0;
		long measuredByteIn = 0;
		long measuredByteOut = 0;
		
		int rpubHighestIndex = 0;
		
		// Find highest index amongst all rpub clients
		for (RPubClientId client: evaluator.getRPubClients()) {
			if (client.getId() > rpubHighestIndex)
				rpubHighestIndex = client.getId();
		}
		
		double[] loadRatios = new double[rpubHighestIndex+1]; 
		
		for (RPubClientId client: evaluator.getRPubClients()) {
			inMessage += evaluator.getClientMessageIn(client);
			outMessage += evaluator.getClientMessageOut(client);
			computedByteIn += evaluator.getClientComputedByteIn(client);
			computedByteOut += evaluator.getClientComputedByteOut(client);
			measuredByteIn += evaluator.getClientMeasuredByteIn(client);
			measuredByteOut += evaluator.getClientMeasuredByteOut(client);
			if (client.getId() >= loadRatios.length) // test 3
				inMessage = inMessage + 0;
			loadRatios[client.getId()] = evaluator.getClientByteOutRatio(client);
		}
		
		return new LoadBalancerStatsControlMessage(inMessage, outMessage, computedByteIn, computedByteOut, measuredByteIn, measuredByteOut, loadRatios, rebalancingTriggered, hostCount);
	}

	private void printLoadEvaluator(LoadEvaluator loadEvaluator, String identifier) {
		for (RPubClientId client: loadEvaluator.getRPubClients()) {
			
			System.out.println(identifier + "|" + processTickCount +
					"|RPubClientId" + client.getId() +
					"|Tin=" + hostInfoMap.get(client).getMaxByteIn() +
					";Tout=" + hostInfoMap.get(client).getMaxByteOut() +
					";Cin=" + loadEvaluator.getClientComputedByteIn(client) +
					";Cout=" + loadEvaluator.getClientComputedByteOut(client) +
					";Min=" + loadEvaluator.getClientMeasuredByteIn(client) +
					";Mout=" + loadEvaluator.getClientMeasuredByteOut(client) +
					";Win=" + loadEvaluator.getClientWastedByteIn(client) +
					";Wout=" + loadEvaluator.getClientWastedByteOut(client) +
					";Uin=" + loadEvaluator.getClientUnusedByteIn(client) +
					";Uout=" + loadEvaluator.getClientUnusedByteOut(client) +
					";%in=" + loadEvaluator.getClientByteInRatio(client) * 100.0 +
					";%out=" + loadEvaluator.getClientByteOutRatio(client) * 100.0
					); 
			
		}
	}
	
	private void printLoadByteout(LoadEvaluator loadEvaluator, String identifier) {
		for (RPubClientId client: loadEvaluator.getRPubClients()) {
			
			System.out.println(identifier + "|" + processTickCount +
					"|RPubClientId" + client.getId() +
					";%out=" + loadEvaluator.getClientByteOutRatio(client) * 100.0
					); 
			
		}
	}
	
	private void printLoadByteoutWAN(LoadEvaluator loadEvaluator, String identifier) {
		int subsDomainA = 0, pubsDomainA = 0, subsDomainB = 0, pubsDomainB = 0;
		
		System.out.print(identifier + "|" + processTickCount); // testxyz
		for (int i=0; i<17; i++) {
			for (RPubClientId client: loadEvaluator.getRPubClients()) {
				if (client.getId() != i)
					continue;
				
				long ratio = Math.round(loadEvaluator.getClientByteOutRatio(client) * 100.0);
				System.out.print(" | " + ratio + " ");
				if (ratio < 10)
					System.out.print(" ");
				
				/*
				for (int x=0; x<RConfig.getTileCountX(); x++) {
					for (int y=0; y<RConfig.getTileCountY(); y++) {
						for (RPubNetworkID netID : loadEvaluator.getClientChannelSubscriberList(client, "tile_" + x + "_" + y + "|A")) {
							if (netID.getDomain().startsWith("us"))
								subsDomainA += 1;
							else
								subsDomainB += 1;
							
						}
						
						for (RPubNetworkID netID : loadEvaluator.getClientChannelSubscriberList(client, "tile_" + x + "_" + y + "|B")) {
							if (netID.getDomain().startsWith("us"))
								subsDomainA += 1;
							else
								subsDomainB += 1;
							
						}
						
						for (RPubNetworkID netID : loadEvaluator.getClientChannelPublisherList(client, "tile_" + x + "_" + y + "|A")) {
							if (netID.getDomain().startsWith("us"))
								pubsDomainA += 1;
							else
								pubsDomainB += 1;
							
						}
						
						for (RPubNetworkID netID : loadEvaluator.getClientChannelPublisherList(client, "tile_" + x + "_" + y + "|B")) {
							if (netID.getDomain().startsWith("us"))
								pubsDomainA += 1;
							else
								pubsDomainB += 1;
							
						}
					}
				}
		
				// Print # of subs on domain A, # subs on domain B, # pubs on domain A, # pubs on domain B
				System.out.print(" | subsDomainA=" + subsDomainA + " | subsDomainB=" + subsDomainB + " | pubsDomainA=" + pubsDomainA + " | pubsDomainB=" + pubsDomainB);
				*/
				
				int publicationsA = 0, publicationsB = 0, publicationsAGlobal = 0, publicationsBGlobal = 0;
				
				// From the Rebalancer, retrieve ss, sl and ll count (message counters)
				int ssCount = 0, slCount = 0, llCount = 0;
				if (this.rebalancer instanceof DynaWANLocationRebalancer) {
					DynaWANLocationRebalancer dynaWan = (DynaWANLocationRebalancer)(this.rebalancer);
					Map<String, DynaWANMessageCounter> counters = dynaWan.getMessageCounters();
					if (counters.get("tile_0_0|A") != null) {
						DynaWANMessageCounter counter = counters.get("tile_0_0|A");
						ssCount += counter.getSSMessageCount();
						slCount += counter.getSLMessageCount();
						llCount += counter.getLLMessageCount();
					}
					if (counters.get("tile_0_0|B") != null) {
						DynaWANMessageCounter counter = counters.get("tile_0_0|B");
						ssCount += counter.getSSMessageCount();
						slCount += counter.getSLMessageCount();
						llCount += counter.getLLMessageCount();
					}
				}
				
				System.out.print(" | {ss,sl,ll}={" + ssCount + ";" + slCount + ";" + llCount + "}");
				
				for (int x=0; x<RConfig.getTileCountX(); x++) {
					for (int y=0; y<RConfig.getTileCountY(); y++) {
						for (RPubNetworkID publisher : loadEvaluator.getClientChannelPublisherList(client, "tile_" + x + "_" + y + "|A")) {
							publicationsA += loadEvaluator.getClientChannelPublisherPublications(client, "tile_" + x + "_" + y + "|A", publisher);
							//publicationsA++;
						}
						publicationsAGlobal += loadEvaluator.getClientChannelPublications(client, "tile_" + x + "_" + y + "|A");
						
						for (RPubNetworkID publisher : loadEvaluator.getClientChannelPublisherList(client, "tile_" + x + "_" + y + "|B")) {
							publicationsB += loadEvaluator.getClientChannelPublisherPublications(client, "tile_" + x + "_" + y + "|B", publisher);
							//publicationsB++;
						}
						publicationsBGlobal += loadEvaluator.getClientChannelPublications(client, "tile_" + x + "_" + y + "|B");
					}
				}
				
				// Print #publicationsA
				//System.out.print(" | p[A,A*]=[" + publicationsA + ";" + publicationsAGlobal + "] | p[B,B*]=[" + publicationsB + ";" + publicationsBGlobal + "]");
				
			}
		}
				
		System.out.print("\n");
	}
	
	/**
	 * Load client pool stuff from prop file
	 */
	private void initRPubClientPool() {
		// Determine the initial RPubClientIndex
		// Load initial servers (rpub clients will connect to those servers when the app is starting)
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		String rawServers = StringUtils.strip(
				props.getProperty("network.rpub.dynamoth.initial_servers"));
		
		// Prepare initial jedis clients
		for (String server: rawServers.split(";")) {
			
			// Parse hostInfo
			RPubHostInfo hostInfo = new RPubHostInfo(server);
			
			// Add entry into hostInfoMap
			hostInfoMap.put(hostInfo.getClientId(), hostInfo);
			
			// Fill the Map RPubClientId->Channels data structure with initial clients and sets of channels
			channels.put(hostInfo.getClientId(), new HashMap<String, Channel>());
		}
		
		// Load pool
		String rawPoolServers = StringUtils.strip(
				props.getProperty("network.rpub.dynamoth.pool_servers"));
		
		for (String server: rawPoolServers.split(";")) {
			
			// Parse hostInfo
			RPubHostInfo hostInfo = new RPubHostInfo(server);
			
			// Add entry into hostInfoMap
			hostInfoMap.put(hostInfo.getClientId(), hostInfo);
			
			// Add client id to client pool queue
			rpubClientPool.add(hostInfo.getClientId());
		}
	}
	
	/**
	 * Retrieves the next most suitable pooled rpub client
	 * @return Next most suitable pool rpub client
	 */
	private RPubClientId pollRPubClient() {
		RPubClientId pooledClientId = this.rpubClientPool.poll();
		// If we return null then no more clients are available
		return pooledClientId;
	}
	
	/**
	 * Puts the specified RPub client back in the pool
	 * @param pooledClient RPub client to free
	 */
	private void freeRPubClient(RPubClientId pooledClientId) {
		this.rpubClientPool.add(pooledClientId);
	}
	
	/**
	 * Starts the LoadBalancer. Connect to the Mammoth Network.
	 */
	public void start() {
		// Create and connect engine
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
    	reactor = new Reactor("RPubLoadBalancerReactor", engine);
    	 
    	// Create channel
    	try {
			engine.createChannel("plan-push-channel");
		} catch (ChannelExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	// Subscribe to loadbalancer-channel so that we can receive LLA events
    	try {
			engine.subscribeChannel("loadbalancer-channel", engine.getId());
			engine.subscribeChannel("lla-channel", engine.getId());
		} catch (NoSuchChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	// Register our handlers with the reactor
    	reactor.register(LLAUpdateControlMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				// Invoke our real handler
				processLLAUpdateControlMessage((LLAUpdateControlMessage)msg);
				
			}
		});
    	
    	reactor.register(RGameUpdatePlayerInfoMessage.class, new Handler() {
    		public void handle(Message msg) {
    			playerCount = ((RGameUpdatePlayerInfoMessage)msg).getPlayerCount();
    			flockingCount = ((RGameUpdatePlayerInfoMessage)msg).getFlockCount();
    		}
    	});
    	
	}
	
	private synchronized void processLLAUpdateControlMessage(LLAUpdateControlMessage message) {
		// Record the info contained in the message. Info will eventually be used for Load Balancing purposes.
		// Iterate through each channel
		int lastUpdateTime = Integer.MAX_VALUE;
		int currentTime = RPubUtil.getCurrentSystemTime();
		
		// Compute the difference between the current time (on the load-balancer) and
		// the last time in the message
		int messageLastTime = message.getLatestTime();
		int timeDelta = currentTime - messageLastTime;
		
		// DBG INFO
		//System.out.println("Received LLAUpdateControlMessage from " + message.getClientId().getId());
		
		for (String channelName: message.getChannels()) {
			// Retrieve the set of channels that are currently defined in our LoadBalancer for this ClientId 
			Map<String, Channel> channels = getClientChannels(message.getClientId());
			// If our map doesn't contain the channel currently being processed (received in message)
			// Then add it to the map. This is permitted since the map is a reference and not a copy.
			if (channels.containsKey(channelName) == false) {
				channels.put(channelName, new Channel(channelName));				
			}
			// Retrieve the channel
			Channel channel = channels.get(channelName);
			// Call the channel's unpack method to unpack from the message
			// Also pass in timeDelta so that we can synchronize the clocks
			channel.unpackFromUpdateMessage(message, timeDelta);
			
			// Update client last update time
			if (channel.getLastTime() < lastUpdateTime) {
				lastUpdateTime = channel.getLastTime();
			}
		}
		// Set last update time
		this.clientLastUpdateTimes.put(message.getClientId(), lastUpdateTime);
		
		// Unpack bandwidth
		
		// Create the hash maps and default values if they do not exist
		if (this.measuredByteInMap.containsKey(message.getClientId()) == false)
			this.measuredByteInMap.put(message.getClientId(), new HashMap<Integer, Long>());
		if (this.measuredByteOutMap.containsKey(message.getClientId()) == false)
			this.measuredByteOutMap.put(message.getClientId(), new HashMap<Integer, Long>());
		if (this.cumulativeMeasuredByteIn.containsKey(message.getClientId()) == false)
			this.cumulativeMeasuredByteIn.put(message.getClientId(), 0L);
		if (this.cumulativeMeasuredByteOut.containsKey(message.getClientId()) == false)
			this.cumulativeMeasuredByteOut.put(message.getClientId(), 0L);
		
		// Unpack everything to hash maps
		for (Integer time: message.getLocalByteTimes()) {
			this.measuredByteInMap.get(message.getClientId()).put(time + timeDelta, message.getLocalByteIn(time));
			this.measuredByteOutMap.get(message.getClientId()).put(time + timeDelta, message.getLocalByteOut(time));
			
			this.cumulativeMeasuredByteIn.put(message.getClientId(), this.cumulativeMeasuredByteIn.get(message.getClientId()) + message.getLocalByteIn(time));
			this.cumulativeMeasuredByteOut.put(message.getClientId(), this.cumulativeMeasuredByteOut.get(message.getClientId()) + message.getLocalByteOut(time));
		}
	}
	
	private Plan getCurrentPlan() {
		// Extracts the current plan from the engine
		// Perhaps the plan could be cached later
		DynamothRPubManager dynamothManager = (DynamothRPubManager)(this.engine.getRPubManager());
		return dynamothManager.getCurrentPlan();
	}
	
	private void setCurrentPlan(Plan plan) {
		// Sets the current plan from the engine
		DynamothRPubManager dynamothManager = (DynamothRPubManager)(this.engine.getRPubManager());
		dynamothManager.setCurrentPlan(plan);
	}
	
	public void pushDummyLBPlan(int index) {
		System.out.println("Pushing dummy LB plan...");
		// Build new plan
		PlanImpl plan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
		
		plan.setPlanId(new PlanId(plan.getPlanId().getId()+1));
		
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				if (index==0) {
					//plan.setMapping("tile_" + i + "_" + j + "|A", new PlanMappingImpl(new PlanId(1), "tile_" + i + "_" + j + "|A", new RPubClientId(1)));
					//plan.setMapping("tile_" + i + "_" + j + "|B", new PlanMappingImpl(new PlanId(1), "tile_" + i + "_" + j + "|B", new RPubClientId(1)));
					plan.setMapping("tile_" + i + "_" + j, new PlanMappingImpl(new PlanId(0), "tile_" + i + "_" + j, new RPubClientId[] {
							new RPubClientId(0),  new RPubClientId(1), new RPubClientId(3)
							}, PlanMappingStrategy.DYNAWAN_ROUTING));
				} else if (index == 1) {
					//plan.setMapping("tile_" + i + "_" + j + "|A", new PlanMappingImpl(new PlanId(1), "tile_" + i + "_" + j + "|A", new RPubClientId(1)));
					//plan.setMapping("tile_" + i + "_" + j + "|B", new PlanMappingImpl(new PlanId(1), "tile_" + i + "_" + j + "|B", new RPubClientId(1)));
					plan.setMapping("tile_" + i + "_" + j, new PlanMappingImpl(new PlanId(0), "tile_" + i + "_" + j, new RPubClientId[] {
							new RPubClientId(0)
							}, PlanMappingStrategy.DYNAWAN_ROUTING));
				}
			}
		}
		
		// Apply the plan id to all mappings
		plan.applyPlanIdToChangedMappings(getCurrentPlan());
		
		
		applyPlan(plan);

	}
	
	public void stubPushNewPlan() {
		
		// Build new plan
		PlanImpl plan = new PlanImpl(new PlanId(1));
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				plan.setMapping("tile_" + i + "_" + j, new PlanMappingImpl(new PlanId(1), "tile_" + i + "_" + j, new RPubClientId(1)));
			}
		}
		
		// Attempt to anticipate new plan changes
		// Build the avg load evaluator
		AveragedLoadEvaluator averagedLoadEvaluator = new AveragedLoadEvaluator(this.loadEvaluators, 10, this.lastLoadEvaluatorTime);
		NewPlanEstimatedLoadEvaluator estimatedLoadEvaluator = new NewPlanEstimatedLoadEvaluator(
				this.getCurrentPlan(), plan, averagedLoadEvaluator,
				this.hostInfoMap, this.lastLoadEvaluatorTime);
		
		// Print load evaluator
		printLoadEvaluator(estimatedLoadEvaluator, "E");
		
		try {
			engine.send("plan-push-channel", new ChangePlanControlMessage(plan));
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
	
	public void stubAddRPubClient() {
		// Create add rpub client message
		RPubClientId nextRPubClientId = pollRPubClient();
		RPubHostInfo nextRPubHostInfo = hostInfoMap.get(nextRPubClientId);
		AddRPubClientControlMessage message = new AddRPubClientControlMessage(
				nextRPubHostInfo.getClientId(),
				nextRPubHostInfo.getHostName(),
				nextRPubHostInfo.getPort());
		
		// Send the add rpub client control message
		try {
			engine.send("plan-push-channel", message);
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
	
	private Map<String, Channel> getClientChannels(RPubClientId clientId) {
		// If specified entry doesn't exist, create and add it
		if (this.channels.containsKey(clientId) == false) {
			this.channels.put(clientId, new HashMap<String, Channel>());
		}
		return this.channels.get(clientId);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*
		// test diff
		PlanImpl plan1 = new PlanImpl(new PlanId(0));
		plan1.setMapping("movechannel1", new PlanMappingImpl("movechannel1", new RPubClientId(1)));
		plan1.setMapping("movechannel2", new PlanMappingImpl("movechannel2", new RPubClientId(1)));
		plan1.setMapping("movechannel3", new PlanMappingImpl("movechannel3", new RPubClientId(1)));
		plan1.setMapping("movechannel4", new PlanMappingImpl("movechannel4", new RPubClientId(1)));

		PlanImpl plan2 = new PlanImpl(new PlanId(1));
		plan2.setMapping("movechannel1", new PlanMappingImpl("movechannel1", new RPubClientId(0)));
		plan2.setMapping("movechannel2", new PlanMappingImpl("movechannel2", new RPubClientId(0)));
		plan2.setMapping("movechannel3", new PlanMappingImpl("movechannel3", new RPubClientId(0)));
		plan2.setMapping("movechannel4", new PlanMappingImpl("movechannel4", new RPubClientId(0)));
		
		PlanDiff pd = new PlanDiffImpl(plan1, plan2);
		System.out.println("RPubClientId0:");
		System.out.println("SUBSCRIPTIONS:");
		for (String channel: pd.getSubscriptions(new RPubClientId(0))) {
			System.out.println(channel);
		}
		System.out.println("UNSUBSCRIPTIONS:");
		for (String channel: pd.getUnsubscriptions(new RPubClientId(0))) {
			System.out.println(channel);
		}
		System.out.println("RPubClientId1:");
		System.out.println("SUBSCRIPTIONS:");
		for (String channel: pd.getSubscriptions(new RPubClientId(1))) {
			System.out.println(channel);
		}
		System.out.println("UNSUBSCRIPTIONS:");
		for (String channel: pd.getUnsubscriptions(new RPubClientId(1))) {
			System.out.println(channel);
		}
		*/
		
		
		// Create new LoadBalancer
		LoadBalancer loadBalancer = new LoadBalancer();
		loadBalancer.start();
		
		// Wait
		/*try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

		// Announce a new rpub server
		//System.out.println("Announcing a new RPubClient...");
		//loadBalancer.stubAddRPubClient();
		
		// Wait
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		// Build and output the graph
		/*
		int currentTime = RPubUtil.getCurrentSystemTime();
		ChannelRelationGraphBuilder graphBuilder = new ChannelRelationGraphBuilder(loadBalancer.channels, currentTime);
		System.out.println(graphBuilder.generateOutput());
		*/
		
		// Announce a new plan
		/*
		System.out.println("Announcing a new plan...");
		loadBalancer.stubPushNewPlan();
		*/
		
		
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
