package Dynamoth.Core.LoadAnalyzing;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import Dynamoth.Mammoth.NetworkEngine.NetworkEngine;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.ControlMessages.TrackInfoControlMessage;
import Dynamoth.Core.Util.RPubUtil;
import Dynamoth.Util.Collection.CollectionUtils;

public abstract class AbstractResponseTimeTracker {

	private static AbstractResponseTimeTracker instance = null; 
	
	// Map: int (discrete) time in seconds -> response time in ms (obtained from recv msg - sent msg)
	private Map<Integer, List<Integer>> responseTimes = new HashMap<Integer, List<Integer>>();
	
	// Move message count
	private AtomicInteger moveMessageCount = new AtomicInteger(0);
	
	// The following are specific to RGame / DynAvail to track how many state updates
	// are missed during the reconfiguration process...
	private AtomicInteger stateUpdatesMissed = new AtomicInteger(0);
	private AtomicInteger stateUpdatesSent = new AtomicInteger(0);
	
	private List<NetworkEngine> networkEngines = new ArrayList<NetworkEngine>(); 
	
	private int trackInfoLastSent = -1;
	
	private Object lockEngine = new Object();
	private Object lockObject = new Object();
	
	
	protected AbstractResponseTimeTracker() {
	}
	
	public static AbstractResponseTimeTracker getInstance() {
		if (instance == null) {
			// Determine which object we should construct
			String instanceType = RPubUtil.stringProperty("responsetimetracker.type", "Simple");
			if (instanceType.toLowerCase().equals("extended")) {
				instance = new ExtendedResponseTimeTracker();
			} else {
				instance = new SimpleResponseTimeTracker();
			}
		}
		return instance;
	}
	
	public void registerNetworkEngine(NetworkEngine networkEngine) {
		synchronized (lockEngine) {
			this.networkEngines.add(networkEngine);
		}
	}
	
	private NetworkEngine getRandomEngine() {
		synchronized (lockEngine) {
			int engineIndex = CollectionUtils.random.nextInt(this.networkEngines.size());
			return this.networkEngines.get(engineIndex);
		}
	}

	public void addOutgoingMessage(RPubMessage message, String channel) {
		addOutgoingMessageInternal(message, channel);
		sendTrackInfo();
	}
	
	public abstract void addOutgoingMessageInternal(RPubMessage message, String channel);
	
	public void addIncomingMessage(RPubMessage message, String channel) {
		addIncomingMessageInternal(message, channel);
		sendTrackInfo();
	}
	
	public abstract void addIncomingMessageInternal(RPubMessage message, String channel);
	
	private int[] toIntArray(List<Integer> list){
	  int[] ret = new int[list.size()];
	  for(int i = 0;i < ret.length;i++)
	    ret[i] = list.get(i);
	  return ret;
	}
	
	protected void addResponseTime(int responseTime) {
		synchronized( lockObject ) {
			long currentTime = System.nanoTime();
			int currentSecond = nanosecondsToSeconds(currentTime);
			
			List<Integer> times = responseTimes.get(currentSecond);
			if (times == null) {
				times = new LinkedList<Integer>();
				responseTimes.put(currentSecond, times);
			}
			
			times.add(responseTime);
		}
	}
	
	public int[] cloneResponseTimes(int time) {
		List<Integer> times = null;
		
		synchronized (lockObject) {
			List<Integer> respTimes = responseTimes.get(time);
			if (respTimes != null)
				times = new ArrayList<Integer>(respTimes);
		}
			
		if (times == null)
			return null;
		
		int[] timesArray = toIntArray(times);
		
		return timesArray;
	}
	
	public int getAverageResponseTime(int time) {
		// Integer.MIN_VALUE will be returned if no metrics are available for the specified time
		int intAverage = 0;
		synchronized (lockObject) {
			List<Integer> times = responseTimes.get(time);
			if (times == null)
				return Integer.MIN_VALUE;
			
			// Just average them :-)
			double sum = 0;
			for (Integer recordedTime: times) {
				sum += recordedTime;
			}
			double average = 0;
			try {
				average = sum / times.size();
				intAverage = (int)(Math.round(average));
			} catch (ArithmeticException e) {
				return Integer.MIN_VALUE;
			}
		}
		return intAverage;
	}
	
	/*
	public int getPercentileResponseTime(int time) {
		// Integer.MIN_VALUE will be returned if no metrics are available for the specified time
		int intPercentile = 0;
		synchronized (lockObject) {
			List<Integer> times = responseTimes.get(time);
			if (times == null)
				return Integer.MIN_VALUE;
			
			// Sort values
			List<Integer> sortedTimes = new ArrayList<Integer>(times);
			Collections.sort(sortedTimes);
			
			// Take the nth (sorted) value
			
			
		}
		return intAverage;	
	}*/
	
	public void addMoveMessage() {
		this.moveMessageCount.incrementAndGet();
	}

	public int getStateUpdatesMissed() {
		return stateUpdatesMissed.get();
	}

	public int getStateUpdatesSent() {
		return stateUpdatesSent.get();
	}
	
	public void addStateUpdatesMissed(int missed) {
		stateUpdatesMissed.addAndGet(missed);
	}
	
	public void addStateUpdatesSent(int sent) {
		stateUpdatesSent.addAndGet(sent);
	}
	
	private void sendTrackInfo() {
		// Send track info if the second 'increased'
		int currentTime = nanosecondsToSeconds(System.nanoTime());
		if (this.trackInfoLastSent < 0) {
			this.trackInfoLastSent = currentTime;
		}
		if (currentTime > this.trackInfoLastSent) {
			for (int i=this.trackInfoLastSent; i<currentTime; i++) {
				// Print avg
				int average = this.getAverageResponseTime(currentTime-1);
				int[] responseTimes = cloneResponseTimes(currentTime-1);
				
				// Get response times
				
				// Send network message using a random engine
				try {
					getRandomEngine().send("track-info", new TrackInfoControlMessage(i, responseTimes, average, this.moveMessageCount.getAndSet(0), this.stateUpdatesMissed.get(), this.stateUpdatesSent.get()));
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
				
				int length = 0;
				if (responseTimes != null)
					length = responseTimes.length;
				System.out.println("TRACKINFO: sending track info to server, respCount=" + length);
			}
			
			this.trackInfoLastSent = currentTime;
		}
	}
	
	public static int nanosecondsToSeconds(long time) {
		return (int)(Math.round( time/1000000000.0 ));
	}
	
	public static int nanosecondsToMilliseconds(long time) {
		return (int)(Math.round( time/1000000.0 ));
	}
	
	protected boolean isLocalRPubClientId(RPubNetworkID id) {
		for (NetworkEngine ne: networkEngines) {
			if (id.equals(ne.getId()))
				return true;
		}
		return false;
	}
}
