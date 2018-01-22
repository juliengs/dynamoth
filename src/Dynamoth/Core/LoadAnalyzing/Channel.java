package Dynamoth.Core.LoadAnalyzing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.ControlMessages.LLAUpdateControlMessage;

public class Channel implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8653213796268709549L;
	
	private String channelName = null;
	private Map<Integer,SliceStats> sliceStatsMap = new HashMap<Integer,SliceStats>();
	private int lastTime = -1;
	
	public Channel(String channelName) {
		this.channelName = channelName;
		
	}

	public String getChannelName() {
		return channelName;
	}

	/**
	 * Get the slice stats for a given time (ie second)
	 * @param time
	 * @return Slice stats
	 */
	public synchronized SliceStats getSliceStats(int time) {
		return sliceStatsMap.get(time);
	}
	
	public synchronized Map<Integer,SliceStats> getSliceStatsMap() {
		return this.sliceStatsMap;
	}
	
	public synchronized void initializeSliceStats(int time) {
		if (getSliceStats(time) == null) {
			
			// Initialize all previous slice stats
			
			// Find closest previous slice stats
			int currentPrevious = Integer.MAX_VALUE;
			for (int i=time-1; i>time-10000; i--) {
				if (getSliceStats(i) != null) {
					currentPrevious = i;
					break;
				}
			}
			
			// Build the slice stats up to the current time
			if (currentPrevious == Integer.MAX_VALUE) {
				this.sliceStatsMap.put(time, new SliceStats());
			} else {
				for (int i=currentPrevious + 1; i <= time; i++) {
					this.sliceStatsMap.put(i, new SliceStats(this.sliceStatsMap.get(i-1)));
				}
			}
		
			// If requested time > lastTime then set lastTime to requested time
			if (time > this.lastTime) {
				this.lastTime = time;
			}
		}
	}
	
	public synchronized void clearSliceStats(int amountToKeep) {
		//if (1==1) return;
		
		// Clear out old entries older than clearUnder
		int clearUnder = lastTime - amountToKeep;
		
		Set<Integer> valuesToRemove = new HashSet<Integer>();
		
		for (Integer key: sliceStatsMap.keySet()) {
			if (key < clearUnder) {
				valuesToRemove.add(key);
			}
		}
		
		for (Integer key: valuesToRemove) {
			sliceStatsMap.remove(key);
		}
		
	}
	
	public synchronized int getLastTime() {
		return this.lastTime;
	}
	
	public synchronized void packToUpdateMessage(LLAUpdateControlMessage message, int startTime, int currentTime) {
		// Iterate from startTime to currenTime

		// Iterate from lastLLAUpdateTime to currentTimeSlice
		for (int i=startTime; i<currentTime; i++) {
			// Make sure the slice stats structure is initialized
			this.initializeSliceStats(i);
			// Get the slice stats
			SliceStats stats = this.getSliceStats(i);
			// Pack the stats in our message
			message.putSliceStats(this.channelName, i, stats);
			
			//message.printDebugInfo();
		}
		//message.putLastTime(this.channelName, this.lastTime);
		message.putLastTime(this.channelName, currentTime-1);
	}
	
	public synchronized void unpackFromUpdateMessage(LLAUpdateControlMessage message, int timeOffset) {
		// Unpack everything regarding this channel that is contained in the message
		for (Integer time: message.getChannelTimes(this.channelName)) {
			// Set stats
			this.sliceStatsMap.put(time+timeOffset, message.getSliceStats(this.channelName, time));
		}
		this.lastTime = message.getLastTime(this.channelName)+timeOffset;
	}
}
