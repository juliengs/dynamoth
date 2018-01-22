package Dynamoth.Core.ControlMessages;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.Client.JedisRPubClient;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadAnalyzing.SliceStats;

public class LLAUpdateControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8920618248128240895L;

	/**
	 * RPubClientId for this instance
	 */
	private RPubClientId clientId;
	
	// Map of maps: String (channel name) -> <Integer (Time), Channel.SliceStats>
	private Map<String, Map<Integer, SliceStats>> stats = new HashMap<String, Map<Integer,SliceStats>>();
	
	// Map of String (channel name) -> Integer (LastTime)
	private Map<String, Integer> lastTimes = new HashMap<String, Integer>();
	
	// Sigar/local network interface stats
	private Map<Integer, Long> localByteInMap = new HashMap<Integer, Long>();
	private Map<Integer, Long> localByteOutMap = new HashMap<Integer, Long>();
	
	
	
	public LLAUpdateControlMessage(RPubClientId clientId) {
		this.clientId = clientId;
	}
	
	public void printDebugInfo() {
		System.out.println("...#channels=" + stats.size());
		for (Map.Entry<String, Map<Integer, SliceStats>> statsEntry: stats.entrySet()) {
			System.out.println("......#entries=" + statsEntry.getValue().size());
			
			try {
				int statsEntrySize = JedisRPubClient.toString((Serializable) statsEntry.getValue()).length();
				System.out.println("PACKED statsSize=" + statsEntrySize);
				
				// Print all entries
				for (Map.Entry<Integer, SliceStats> sliceEntry: statsEntry.getValue().entrySet()) {
					int sliceEntrySize = JedisRPubClient.toString((Serializable) sliceEntry.getValue()).length();
					System.out.println("...PACKED time=" + sliceEntry.getKey() + " | sliceEntrySize=" + sliceEntrySize);
					//sliceEntry.getValue().
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// Serialize ind stuff
		// Calc and print msg size
		/*
		try {
			int lastTimesSize = JedisRPubClient.toString((Serializable) lastTimes).length();
			int statsSize = JedisRPubClient.toString((Serializable) stats).length();
			System.out.println("PACKED lastTimesSize= " + lastTimesSize + " | statsSize=" + statsSize);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
	
	public void putSliceStats(String channel, int time, SliceStats sliceStats) {
		if (stats.containsKey(channel) == false) {
			stats.put(channel, new HashMap<Integer, SliceStats>());
		}
		stats.get(channel).put(time, sliceStats);
	}
	
	public void putLastTime(String channel, int lastTime) {
		lastTimes.put(channel, lastTime);
	}

	public Set<String> getChannels() {
		return stats.keySet();
	}
	
	public Set<Integer> getChannelTimes(String channel) {
		return stats.get(channel).keySet();
	}
	
	public SliceStats getSliceStats(String channel, int time) {
		return stats.get(channel).get(time);
	}
	
	public RPubClientId getClientId() {
		return this.clientId;
	}
	
	public int getLastTime(String channel) {
		return this.lastTimes.get(channel);
	}
	
	/**
	 * Finds the latest "last" time (by checking the last time of each channel)
	 * @return Latest "last" time amongst all channels
	 */
	public int getLatestTime() {
		int latestTime = 0;
		for (Integer lastTime: this.lastTimes.values()) {
			if (lastTime > latestTime) {
				latestTime = lastTime;
			}
		}
		return latestTime;
	}
	
	/**
	 * Finds the earliest "last" time (by checking the last time of each channel)
	 * @return Earliest "last" time amongst all channels
	 */
	public int getEarliestTime() {
		int earliestTime = Integer.MAX_VALUE;
		for (Integer lastTime: this.lastTimes.values()) {
			if (lastTime < earliestTime) {
				earliestTime = lastTime;
			}
		}
		return earliestTime;
	}
	
	public Set<Integer> getLocalByteTimes() {
		return this.localByteInMap.keySet();
	}

	public long getLocalByteIn(int timeSlice) {
		if (localByteInMap.get(timeSlice) == null) {
			return 0;
		} else {
			return localByteInMap.get(timeSlice);
		}
	}

	public void putLocalByteIn(int timeSlice, long localByteIn) {
		this.localByteInMap.put(timeSlice, localByteIn);
	}

	public long getLocalByteOut(int timeSlice) {
		if (localByteOutMap.get(timeSlice) == null) {
			return 0;
		} else {
			return localByteOutMap.get(timeSlice);
		}
	}

	public void putLocalByteOut(int timeSlice, long localByteOut) {
		this.localByteOutMap.put(timeSlice, localByteOut);
	}
}
