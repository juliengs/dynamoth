package Dynamoth.Core.Game;

import java.io.Serializable;
import java.util.Random;

public class RPlayerFlockInfoHotspot implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8242662011648596725L;
	
	private int x;
	private int y;
	
	public RPlayerFlockInfoHotspot(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public int getX() {
		return this.x;
	}
	
	public int getY() {
		return this.y;
	}
	
	public double getCenterX() {
		return RConfig.getHotspotX(x);
	}
	
	public double getCenterY() {
		return RConfig.getHotspotY(y);
	}
	
	public double getRadius() {
		return RConfig.HOTSPOT_RADIUS;
	}
	
	/**
	 * Generate a random point inside the hotspot
	 * @return A random point inside the hotspot
	 */
	public void generateRandomPointInHotspot(Random random, double[] points) {
		double angle = Math.random()*Math.PI*2;
		points[0] = getCenterX() + Math.cos(angle)*getRadius();
		points[1] = getCenterY() + Math.sin(angle)*getRadius();
	}
	
	@Override
	public String toString() {
		return "RPlayerFlockInfoHotspot [x=" + x + ", y=" + y + "]";
	}
}
