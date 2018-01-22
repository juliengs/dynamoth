package Dynamoth.Core.LoadAnalyzing;

import java.util.HashMap;
import java.util.Map;

import Dynamoth.Core.RPubMessage;

public class SimpleResponseTimeTracker extends AbstractResponseTimeTracker {

	// Map: sent message hashes -> time in ns
	private Map<Integer, Long> outgoingMessages = new HashMap<Integer, Long>();
	
	private Object lockObject = new Object();
	
	protected SimpleResponseTimeTracker() {
	}
	
	public void addOutgoingMessageInternal(RPubMessage message, String channel) {
		long currentTime = System.nanoTime();
		synchronized (lockObject) {
			outgoingMessages.put(message.getMessageID(), currentTime);	
		}
	}
	
	public void addIncomingMessageInternal(RPubMessage message, String channel) {	
		synchronized (lockObject) {
			// Check if message is in outgoingMessages; otherwise, discard
			if (outgoingMessages.containsKey(message.getMessageID()) == false)
				return;			
			
			long currentTime = System.nanoTime();
					
			long outgoingTime = outgoingMessages.get(message.getMessageID());
			long responseTime = currentTime - outgoingTime;
			
			int intResponseTime = nanosecondsToMilliseconds(responseTime);
			
			// Add only if <=4000 (above 4000 considered lost)
			if (intResponseTime <= 60000)
				addResponseTime(intResponseTime);
		}
	}

}
