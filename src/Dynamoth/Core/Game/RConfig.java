package Dynamoth.Core.Game;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import Dynamoth.Client.Client;
import Dynamoth.Core.Util.RPubUtil;
import Dynamoth.Util.Properties.PropertyManager;

/**
 * Class which contains some config variables used by RGame 
 * @author Julien Gascon-Samson
 *
 */
public class RConfig {

	public static double SPEED = RPubUtil.doubleProperty("rgame.player_speed");
	
	public static double MAP_BOUND_X = RPubUtil.doubleProperty("rgame.map_bounds.x");
	public static double MAP_BOUND_Y = RPubUtil.doubleProperty("rgame.map_bounds.y");
	/*
	 *  (-100,100)-----------(100,100)
	 *  |                            |
	 *  |                            |
	 *  |                            |
	 *  |                            |
	 *  |                            |
	 *  |                            |
	 *  |                            |
	 *  |                            |
	 *  |                            |
	 * (-100,-100)----------(100,-100)
	 */
	
	//Default size of: 25 50
	public static double TILE_SIZE_X = RPubUtil.doubleProperty("rgame.tile_size.x");
	public static double TILE_SIZE_Y = RPubUtil.doubleProperty("rgame.tile_size.y");
	
	// Hotspot configuration
	public static int HOTSPOT_COUNT_X = 3;
	public static int HOTSPOT_COUNT_Y = 3;
	public static double HOTSPOT_RADIUS = 10.0;
	
	// For specific experiments
	public static boolean ONLY_ONE_PUBLISHER = false;
	public static boolean ONLY_ONE_SUBSCRIBER = false;
	public static boolean ENABLE_FAKE_FLOCKING = RPubUtil.boolProperty("rgame.enable_fake_flocking");
	
	// Subscribe to only the requested tile or to a surrounding area?
	// 0 => only the tile
	// 1 => one tile around the target tile
	// 2 => two tiles around the target tile
	public static int SUBSCRIPTION_RANGE = RPubUtil.intProperty("rgame.subscription_range");
	
	private RConfig() {
		
	}

	public static int getTileX(double x) {
		return getTileXY(x, MAP_BOUND_X, TILE_SIZE_X);
	}
	
	public static int getTileY(double y) {
		return getTileXY(y, MAP_BOUND_Y, TILE_SIZE_Y);
	}
	
	private static int getTileXY(double position, double bound, double tileSize) {
		// ex: pos=-85, bound=100, tilesize=10 then tile=1 (zero-indexed)
		int iPosition = (int) ( Math.round(position) );
		int iBound =    (int) ( Math.round(bound)    );
		int iTileSize = (int) ( Math.round(tileSize) );
		
		// Make position/bound positive
		// ex: pos=15
		iPosition += iBound;
		
		// Apply intDiv tileSize
		// ex: tile = 15/10 = 1
		int tile = iPosition / iTileSize;
		
		// Return the tile
		return tile;
	}
	
	public static int getTileCountX() {
		// Return the tileX for the bound threshold
		return getTileX(RConfig.MAP_BOUND_X);
	}
	
	public static int getTileCountY() {
		// Return the tileY for the bound threshold
		return getTileY(RConfig.MAP_BOUND_Y);		
	}
	
	public static void printTileMap(int[][] subscribers) {
		String verticalSeparator = StringUtils.repeat("-", getTileCountX() * 5) + "-";
		
		for (int j=0; j<getTileCountY(); j++) {
			
			System.out.println(verticalSeparator);
			
			System.out.print("|");
			
			for (int i=0; i<getTileCountX(); i++) {
				// Print spacing
				System.out.print(" ");
				
				// Print the value
				System.out.print(StringUtils.repeat(" ", 2 - Integer.toString(subscribers[i][j]).length()) + subscribers[i][j]);
				
				// Add right sep
				System.out.print(" |");
			}	
			
			System.out.print("\n");
		}
		System.out.println(verticalSeparator);
	}
	
	public static double getHotspotX(int indexX) {
		return (2*MAP_BOUND_X) / (HOTSPOT_COUNT_X+1) * (indexX+1) - MAP_BOUND_X;
	}
	
	public static double getHotspotY(int indexY) {
		return (2*MAP_BOUND_Y) / (HOTSPOT_COUNT_Y+1) * (indexY+1) - MAP_BOUND_Y;
	}
	
}
