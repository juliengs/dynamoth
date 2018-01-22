package Dynamoth.Core.Availability;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.DynamothRPubManager;
import java.util.HashSet;
import java.util.Set;

/**
 * Various settings related to Dynamoth availability.
 * 
 * @author Julien Gascon-Samson
 */
public class AvailabilityConfiguration {
	private int keepAliveTime = 20000;
	private int maxSendTime = 1000;
	private int pastPublicationsTime = 21500;
	
	
	
	// Response time measurements
	/**
	 * Collect response times while in a failed state?
	 */
	private boolean[] collectResponseTimesStates = new boolean[5];
	
	/**
	 * Collect response time measurements only from failed channels
	 */
	private boolean collectResponseTimesOnlyFromFailedChannels = true;
	
	/**
	 * When replaying publications: playback mode
	 */
	private PlaybackMode playbackMode = PlaybackMode.PLAYBACK_CONCURRENT;
	
	/**
	 * Time to wait between each publication when replaying using the Concurrent mode
	 */
	private int playbackConcurrentWaitTime = 100;
	
	/**
	 * Time to wait between each publication when replaying using the Ordered mode
	 */
	private int playbackOrderedWaitTime = 100;
			
	private Set<RPubClientId> failureIgnore = new HashSet<RPubClientId>();

	private static AvailabilityConfiguration instance = null;
	
	public enum PlaybackMode {
		NO_PLAYBACK,
		PLAYBACK_CONCURRENT,
		PLAYBACK_ORDERED
	}
	
	private AvailabilityConfiguration() {
		failureIgnore.add(new RPubClientId(0));
		failureIgnore.add(new RPubClientId(2));
		
		// Setup collection states
		setupReplayOnly();
	}
	
	private void setupAll() {
		collectResponseTimesStates[DynamothRPubManager.STATE_BEFORE_FAILURE] = true;
		collectResponseTimesStates[DynamothRPubManager.STATE_FAILURE_UNDETECTED] = true;
		collectResponseTimesStates[DynamothRPubManager.STATE_FAILURE_DETECTED] = true;
		collectResponseTimesStates[DynamothRPubManager.STATE_SUBSCRIPTIONS_REESTABLISHED] = true;
		collectResponseTimesStates[DynamothRPubManager.STATE_AFTER_RECOVERY] = true;	
	}
	
	private void setupNewOnly() {
		collectResponseTimesStates[DynamothRPubManager.STATE_BEFORE_FAILURE] = true;
		collectResponseTimesStates[DynamothRPubManager.STATE_FAILURE_UNDETECTED] = false;
		collectResponseTimesStates[DynamothRPubManager.STATE_FAILURE_DETECTED] = false;
		collectResponseTimesStates[DynamothRPubManager.STATE_SUBSCRIPTIONS_REESTABLISHED] = true;
		collectResponseTimesStates[DynamothRPubManager.STATE_AFTER_RECOVERY] = true;
	}
	
	private void setupReplayOnly() {
		collectResponseTimesStates[DynamothRPubManager.STATE_BEFORE_FAILURE] = true;
		collectResponseTimesStates[DynamothRPubManager.STATE_FAILURE_UNDETECTED] = true;
		collectResponseTimesStates[DynamothRPubManager.STATE_FAILURE_DETECTED] = false;
		collectResponseTimesStates[DynamothRPubManager.STATE_SUBSCRIPTIONS_REESTABLISHED] = false;
		collectResponseTimesStates[DynamothRPubManager.STATE_AFTER_RECOVERY] = false;
	}
	
	public static AvailabilityConfiguration getInstance() {
		if (instance==null)
			instance = new AvailabilityConfiguration();
		return instance;
	}
	
	public int getKeepAliveTime() {
		return keepAliveTime;
	}

	public int getMaxSendTime() {
		return maxSendTime;
	}

	public int getPastPublicationsTime() {
		return pastPublicationsTime;
	}
	
	public Set<RPubClientId> getFailureIgnore() {
		return failureIgnore;
	}
	
	public boolean isCollectingResponseTimes(int state) {
		return collectResponseTimesStates[state];
	}

	public boolean isCollectResponseTimesOnlyFromFailedChannels() {
		return collectResponseTimesOnlyFromFailedChannels;
	}

	public PlaybackMode getPlaybackMode() {
		return playbackMode;
	}

	public int getPlaybackConcurrentWaitTime() {
		return playbackConcurrentWaitTime;
	}

	public int getPlaybackOrderedWaitTime() {
		return playbackOrderedWaitTime;
	}
	
}
