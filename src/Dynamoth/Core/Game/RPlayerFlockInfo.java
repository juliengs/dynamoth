package Dynamoth.Core.Game;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RPlayerFlockInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6977139769054224911L;

	/**
	 * Flock Weights
	 */
	private Map<RPlayerFlockInfoHotspot, Integer> flockWeights = new HashMap<RPlayerFlockInfoHotspot, Integer>();
	
	/**
	 * Free Destination Weight
	 */
	private int freeDestinationWeight = 0;
	
	/**
	 * Probability to stay inside the same hotspot when generating a new move
	 */
	private double stayInsideHotspotProbability = 0.80;
	
	
	public RPlayerFlockInfo() {
		// Nothing to do
	}
	
	public Map<RPlayerFlockInfoHotspot, Integer> getFlockWeights() {
		return this.flockWeights;
	}

	public int getFreeDestinationWeight() {
		return this.freeDestinationWeight;
	}
	
	public void setFreeDestinationWeight(int freeDestinationWeight) {
		this.freeDestinationWeight = freeDestinationWeight;
	}
	
	public double getStayInsideHotspotProbability() {
		return this.stayInsideHotspotProbability;
	}
	
	public void setStayInsideHotspotProbability(double stayInsideHotspotProbability) {
		this.stayInsideHotspotProbability = stayInsideHotspotProbability;
	}
	
	public RPlayerFlockInfoHotspot generateRandomHotspot(Random random) {
		// Build thresholds
		List<RPlayerFlockInfoHotspot> hotspots = new ArrayList<RPlayerFlockInfoHotspot>();
		
		// Add all hotspot weights
		for (Map.Entry<RPlayerFlockInfoHotspot, Integer> weightEntry : this.flockWeights.entrySet()) {
			for (int i=0; i<weightEntry.getValue(); i++) {
				hotspots.add(weightEntry.getKey());
			}
		}
		
		// Add stay free destination weight
		for (int i=0; i<freeDestinationWeight; i++) {
			hotspots.add(null);
		}
		
		// Choose randomly and return proper hotspot
		if (hotspots.size() == 0)
			return null;
		else
			return hotspots.get(random.nextInt(hotspots.size()));
	}
	
	/**
	 * Helper function to decide whether or not we should stay inside the same hotspot
	 * that we are currently in. Uses the stayInsideHotspotProability.
	 * 
	 * @return True if we should stay inside the same hotspot; otherwise False.
	 */
	public boolean shouldStayInsideHotspot(Random random) {
		return (random.nextDouble() < this.stayInsideHotspotProbability);
	}

	@Override
	public String toString() {
		return "RPlayerFlockInfo [flockWeights=" + flockWeights
				+ ", freeDestinationWeight=" + freeDestinationWeight
				+ ", stayInsideHotspotProbability="
				+ stayInsideHotspotProbability + "]";
	}
	
	
}
