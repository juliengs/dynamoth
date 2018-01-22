package Dynamoth.Core.Availability;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadAnalyzing.AbstractResponseTimeTracker;
import Dynamoth.Core.RPubMessage;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Julien Gascon-Samson
 */
public class FailureDetector {
	private Map<RPubClientId,Integer> lastIncomingMessageTimes = new HashMap<RPubClientId, Integer>();
	private Object lastIncomingMessageTimesLock = new Object();
	private Thread failureCheckThread = null;
	
	private Set<RPubClientId> currentFailedServers = new HashSet<RPubClientId>();
	private Object currentFailedServersLock = new Object();
	
	private Set<FailureListener> failureListeners = new HashSet<FailureListener>();
	private Object failureListenersLock = new Object();
	
	public FailureDetector() {
		System.err.println("New FailureDetector...");
		this.failureCheckThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					//System.err.println("Checking for FAILED SERVERS");

					// Check for failed servers			
					Set<RPubClientId> failedServers = updateFailedServers();
					/*
					for (RPubClientId failedServer: failedServers) {
						System.out.println("FailedServer: " + failedServer.getId());
					*/

					try {
						// Sleep 500ms
						Thread.sleep(1000);
					} catch (InterruptedException ex) {
						Logger.getLogger(FailureDetector.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
			}
		});
		
		this.failureCheckThread.start();
	}
	
	/**
	 * Add a failure listener.
	 * 
	 * @param failureListener Failure listener
	 */
	public void addFailureListener(FailureListener failureListener) {
		synchronized (failureListenersLock) {
			this.failureListeners.add(failureListener);
		}
	}
	
	/**
	 * Remove a failure listener.
	 * 
	 * @param failureListener Failure listener
	 */
	public void removeFailureListener(FailureListener failureListener) {
		synchronized (failureListenersLock) {
			this.failureListeners.remove(failureListener);
		}
	}
	
	/**
	 * Remove all failure listeners.
	 */
	public void clearFailureListeners() {
		synchronized (failureListenersLock) {
			this.failureListeners.clear();
		}
	}
	
	/**
	 * Notify all failure listeners of a failing server.
	 */
	private void notifyFailureListeners(RPubClientId clientId) {
		synchronized (failureListenersLock) {
			for (FailureListener failureListener: this.failureListeners) {
				failureListener.failureDetected(clientId);
			}
		}
	}
	
	/**
	 * Feeds a message to the FailureDetector. The FailureDetector will then update
	 * the appropriate last incoming message time based on the clientID of the processing server.
	 * 
	 * @message Message
	 */
	public void feedMessage(RPubMessage message) {
		synchronized (lastIncomingMessageTimesLock) {
			lastIncomingMessageTimes.put(message.getRpubServer(), AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()));
		}
		
		//System.err.println("Feeding message for server #" + message.getRpubServer().getId());

	}
	
	public Set<RPubClientId> updateFailedServers() {
		Set<RPubClientId> failedServers = new HashSet<RPubClientId>();
		
		synchronized (lastIncomingMessageTimesLock) {
		
			int currentTime = AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime());
			for (Map.Entry<RPubClientId,Integer> entry: lastIncomingMessageTimes.entrySet()) {
				if (isFailed(currentTime, entry.getValue())) {
					// If we are not ignoring it...
					if (entry.getKey() != null) {
						if (AvailabilityConfiguration.getInstance().getFailureIgnore().contains(entry.getKey()) == false) {
							failedServers.add(entry.getKey());
						}
					}
				}
			}
			
		}
		
		// Determine which new servers failed...
		Set<RPubClientId> newlyFailedServers;
		synchronized (this.currentFailedServersLock) {
			 newlyFailedServers = new HashSet<RPubClientId>(failedServers);
			 newlyFailedServers.removeAll(this.currentFailedServers);
			 this.currentFailedServers.addAll(failedServers);
			 for (RPubClientId failed: currentFailedServers) {
				 //System.out.println("!CurFailed: " + failed.getId());
			 }
		}
		
		// For all newly failing servers, notify listeners
		for (RPubClientId clientId: newlyFailedServers) {
			this.notifyFailureListeners(clientId);
		}
		
		return failedServers;
	}
	
	private boolean isFailed(int currentTime, int lastMessageTime) {
		return ((currentTime - lastMessageTime) > AvailabilityConfiguration.getInstance().getKeepAliveTime());
	}

	/**
	 * Resets the timestamps of the various servers to prevent cascading failures.
	 * Does that by putting the current time as last incoming message time for all servers.
	 * Useful to do this after a recovery...
	 */
	public void resetServerMonitoring() {
		synchronized (lastIncomingMessageTimesLock) {
			for (RPubClientId clientId: lastIncomingMessageTimes.keySet()) {
				lastIncomingMessageTimes.put(clientId, AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()));
			}
		}		
	}
	
	
}
