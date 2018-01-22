package Dynamoth.Core.Game;

import java.util.LinkedHashSet;

public class RPlayerAvatar {

	private double baseX = 0.0, baseY = 0.0;
	private double currentX = 0.0, currentY = 0.0;
	private double targetX = 0.0, targetY = 0.0;
	
	private LinkedHashSet<Integer> counters = new LinkedHashSet<Integer>();
	
	// Message identifiers
	private int currentMessageId = -1;
	private int missingMessages = 0;
	private int previousMessageId = -1;
	private int previousMissingMessages = 0;
	
	public RPlayerAvatar() {

	}

	public double getBaseX() {
		return baseX;
	}

	public void setBaseX(double baseX) {
		this.baseX = baseX;
	}

	public double getBaseY() {
		return baseY;
	}

	public void setBaseY(double baseY) {
		this.baseY = baseY;
	}

	public double getCurrentX() {
		return currentX;
	}

	public void setCurrentX(double currentX) {
		this.currentX = currentX;
	}

	public double getCurrentY() {
		return currentY;
	}

	public void setCurrentY(double currentY) {
		this.currentY = currentY;
	}

	public double getTargetX() {
		return targetX;
	}

	public void setTargetX(double targetX) {
		this.targetX = targetX;
	}

	public double getTargetY() {
		return targetY;
	}

	public void setTargetY(double targetY) {
		this.targetY = targetY;
	}

	public void feedMessageId(int messageId) {
		previousMessageId = currentMessageId;
		previousMissingMessages = missingMessages;
		
		if (currentMessageId == -1) {
			currentMessageId = messageId;
		} else {
			if (messageId > currentMessageId) {
				missingMessages += (messageId - currentMessageId - 1);
				currentMessageId = messageId;
			}
		}
	}

	public int getMissingMessages() {
		return missingMessages;
	}

	public int getCurrentMessageId() {
		return currentMessageId;
	}

	public int getPreviousMessageId() {
		return previousMessageId;
	}

	public int getPreviousMissingMessages() {
		return previousMissingMessages;
	}
	
	public LinkedHashSet<Integer> getCounters() {
		return counters;
	}
	
	/**
	 * Determine which counters were missing
	 * 
	 * @return Array of missing counters
	 */
	public LinkedHashSet<Integer> getMissingCounters() {
		// Determine the largest value (not optimal)
		int largest = -1;
		for (Integer counter : counters) {
			if (counter > largest)
				largest = counter;
		}
		
		// Create a new set with all items up to the largest value and remove all items in counters
		LinkedHashSet<Integer> allCounters;
		if (largest > 0)
			allCounters = new LinkedHashSet<Integer>(largest);
		else
			allCounters = new LinkedHashSet<Integer>();
		
		for (int i=0; i<largest; i++) {
			allCounters.add(i);
		}
		// Remove all
		for (Integer counter : counters) {
			allCounters.remove(counter);
		}
		
		return allCounters;
	}

	public void move(double timeStep) {
		// Only move if we did not reach destination
		if (isDestinationReached() == false) {
			// Right now, only move SPEED along X and Y
			currentX += Math.signum(targetX - currentX) * RConfig.SPEED * timeStep;
			currentY += Math.signum(targetY - currentY) * RConfig.SPEED * timeStep;
		}
	}
	
	public boolean isDestinationReached() {
		// Did we reach our destination?
		// We consider 'yes' if we are within 1 in x and y of the target
		if (Math.abs(targetX - currentX) < 1.0 && Math.abs(targetY - currentY) < 1.0) {
			return true;
		} else {
			return false;
		}
	}
}
