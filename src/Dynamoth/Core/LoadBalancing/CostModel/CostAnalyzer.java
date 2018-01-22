package Dynamoth.Core.LoadBalancing.CostModel;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import Dynamoth.Client.Client;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.ControlMessages.UpdateRetentionRatiosControlMessage;
import Dynamoth.Core.Game.RConfig;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Util.Properties.PropertyManager;

/**
 * Dynamoth Cost Analyzer.
 * 
 * @author Julien Gascon-Samson
 *
 */
public class CostAnalyzer {

	// "CONSTANTS" AND THRESHOLDS
	
	// Bandwidth cost per GB (Cloud)
	private double byteOutCostPerGB = 0.09;
	
	// Period duration in units [1 hour = 60 units]
	private int periodDuration = 30; 
	
	// Unit duration in Dynamoth Units of Time: seconds [1 minute]
	private int unitDuration = 20;
	
	// Allocated bandwidth quota per period IN BYTES
	private long byteOutQuotaPerPeriod = 8000L * 1024 * 1024; // in BYTES
	
	// ----- //
	
	// STATS/DATA ACCUMULATION FROM LB
	
	// Current unit in period
	private int currentUnit = 0;
	
	// Current (accumulated) measurements in period
	private long accumulatedTotalByteOut = 0;
	private long accumulatedTotalXByteOut = 0;
	private long accumulatedHighByteOut = 0;
	private long accumulatedLowByteOut = 0;
	private long accumulatedLowXByteOut = 0;
	
	private long[][] accumulatedTotalByteOutTiles = null;
	private long[][] accumulatedTotalXByteOutTiles = null;
	private long[][] accumulatedHighByteOutTiles = null;
	private long[][] accumulatedLowByteOutTiles = null;
	private long[][] accumulatedLowXByteOutTiles = null;
	
	// Player count
	private int playerCount = 0;
	private int flockingCount = 0;
	
	// Current (accumulated) measurements for prev unit
	private long prevTotalByteOut = 0;
	private long prevTotalXByteOut = 0;
	private long prevHighByteOut = 0;
	private long prevLowByteOut = 0;
	private long prevLowXByteOut = 0;
	
	private long[][] prevTotalByteOutTiles = null;
	private long[][] prevTotalXByteOutTiles = null;
	private long[][] prevHighByteOutTiles = null;
	private long[][] prevLowByteOutTiles = null;
	private long[][] prevLowXByteOutTiles = null;
	
	private long[][] prevSubscribersTiles = null;
	
	// Initial Dynamoth time (to compute offsets)
	private long initialDynamothTime = 0;
	
	private boolean averageWholePeriod = false; 
	
	private Writer outputFileWriter = null;
	
	private RPubNetworkEngine engine;
	
	// ----- //
	
	// COST BALANCING STUFF (later)
	
	// Rxy : filtering ratio - ratio of packets that will be transferred to TL
	// TODO: One ratio per tile!
	private double filteringRatio = 1;
	
	private double[][] retentionRatios = null;
	
	private double avgRetentionRatioAll = 0.0;
	private double avgRetentionRatioLow = 0.0;
	
	private double minRetentionRatio = 0.25;
	
	// ----- //
	
	// CONSTRUCTORS
	
	public CostAnalyzer(long initialDynamothTime, RPubNetworkEngine engine) {
		this.initialDynamothTime = initialDynamothTime;
		this.engine = engine;
		
		// Create our arrays
		
		resetPrev();
		
		this.accumulatedTotalByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		this.accumulatedTotalXByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		this.accumulatedHighByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		this.accumulatedLowByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		this.accumulatedLowXByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		
		this.retentionRatios = new double[RConfig.getTileCountX()][RConfig.getTileCountY()];
		
		// Prefill retentionRatios ratios
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				retentionRatios[i][j] = 1.00;
			}
		}
	}
	
	// ----- //
	
	// METHODS
	
	public void submitLoadData(int currentDynamothTime, LoadEvaluator evaluator, int playerCount, int flockingCount) {
		// Compute current unit
		int currentUnit = (int) Math.floor((currentDynamothTime - initialDynamothTime) * 1.0 / unitDuration);
		
		System.out.println("DynamothTimeDelta=" + (currentDynamothTime - initialDynamothTime));
		System.out.println("CurrentUnit=" + currentUnit);
		
		// Check if unit changed in which case we will trigger a cost recomputation
		int lastUnit = this.currentUnit;
		if (currentUnit > lastUnit && currentUnit > 0) {
			// First algo - same retention ratio everywhere - do we have time for that? No!!
			System.out.println("LAST UNIT: " + lastUnit + " | " + "CURRENT UNIT: " + currentUnit);
			computeCosts();
			
			// Output to CSV
			outputToCSV();
			// Output FRMap
			outputFRMap();
			
			// Reset prev - start new unit...
			resetPrev();
		}
		// Set current unit
		this.currentUnit = currentUnit;
		
		this.playerCount = playerCount;
		this.flockingCount = flockingCount;
		
		// Extract relevant metrics from load evaluator
		// For this dynamoth time:
		RPubClientId clientId = new RPubClientId(0);
		
		// Global byteout
		// DON'T USE GLOBAL BYTEOUT FOR NOW - ONLY USE BYTEOUT FROM TILES
		//long globalByteOut = evaluator.getClientMeasuredByteOut(clientId);
		
		Set<String> allSubscribers = new HashSet<String>(); 
		
		// Byteout for every tile + subscribers
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				if (evaluator.getRPubClients().contains(clientId) == false) {
					continue;
				}
				
				long highTileByteOut = evaluator.getClientChannelComputedByteOut(clientId, "tile_" + i + "_" + j);
				int highTileSubscribers = evaluator.getClientChannelSubscribers(clientId, "tile_" + i + "_" + j);
				long lowTileByteOut = evaluator.getClientChannelComputedByteOut(clientId, "tile_" + i + "_" + j + "_L");
				int lowTileSubscribers = evaluator.getClientChannelSubscribers(clientId, "tile_" + i + "_" + j + "_L");
				
				// Compensate the effects of the filtering matrix
				long extrapolatedLowTileByteOut = lowTileByteOut; 
				if (retentionRatios[i][j] < 1.00)
					extrapolatedLowTileByteOut = (long) (lowTileByteOut / retentionRatios[i][j]);

				// Add to arrays
				
				this.accumulatedTotalByteOutTiles[i][j] += highTileByteOut + lowTileByteOut;
				this.accumulatedTotalXByteOutTiles[i][j] += highTileByteOut + extrapolatedLowTileByteOut;
				this.accumulatedHighByteOutTiles[i][j] += highTileByteOut;
				this.accumulatedLowByteOutTiles[i][j] += lowTileByteOut;
				this.accumulatedLowXByteOutTiles[i][j] += extrapolatedLowTileByteOut;
				
				this.prevTotalByteOutTiles[i][j] += highTileByteOut + lowTileByteOut;
				this.prevTotalXByteOutTiles[i][j] += highTileByteOut + extrapolatedLowTileByteOut;
				this.prevHighByteOutTiles[i][j] += highTileByteOut;
				this.prevLowByteOutTiles[i][j] += lowTileByteOut;
				this.prevLowXByteOutTiles[i][j] += extrapolatedLowTileByteOut;
				
				this.prevSubscribersTiles[i][j] = highTileSubscribers + lowTileSubscribers;
				
				// Add to global accumulators
				
				this.accumulatedTotalByteOut += highTileByteOut + lowTileByteOut;
				this.accumulatedTotalXByteOut += highTileByteOut + extrapolatedLowTileByteOut;
				this.accumulatedHighByteOut += highTileByteOut;
				this.accumulatedLowByteOut += lowTileByteOut;
				this.accumulatedLowXByteOut += extrapolatedLowTileByteOut;
				
				this.prevTotalByteOut += highTileByteOut + lowTileByteOut;
				this.prevTotalXByteOut += highTileByteOut + extrapolatedLowTileByteOut;
				this.prevHighByteOut += highTileByteOut;
				this.prevLowByteOut += lowTileByteOut;
				this.prevLowXByteOut += extrapolatedLowTileByteOut;
				
			}
		}
	}
	
	private void computeCosts() {
		int newCurrentUnit = currentUnit + 1;
		
		double sumOfWeights = 0;
		//double sumOfB
		
		// For all tiles, compute density factor -> load factor
		double[][] loadFactors = new double[RConfig.getTileCountX()][RConfig.getTileCountY()];
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				long avgByteOutTile = this.prevTotalXByteOutTiles[i][j] / 60;
				long avgSubscribersTile = this.prevSubscribersTiles[i][j]; // /60 was working!!!
				double densityFactor = 1;
				if (avgSubscribersTile > 0) {
					densityFactor = Math.log(avgSubscribersTile) / Math.log(2);
				}
				double loadFactor = avgByteOutTile * densityFactor;
				//loadFactor = avgByteOutTile;
				loadFactors[i][j] = loadFactor;
				
				sumOfWeights += loadFactor;
			}
		}
		
		// Byte remaining computed from real usage
		long byteRemaining = byteOutQuotaPerPeriod - accumulatedTotalByteOut;
		// Find out our target bandwidth (for current unit)
		long byteTarget = (long) (byteRemaining * 1.0 / (periodDuration-newCurrentUnit));
		// Find out hot we consumed in average (since the beginning of the period) - take extrapolated usage
		double totalXByteOut = 0;
		long byteAverage = 0;
		if (averageWholePeriod) {
			// Version A: take average since beginning of period
			totalXByteOut = accumulatedTotalXByteOut;
			byteAverage = (long) (totalXByteOut * 1.0 / (newCurrentUnit));
		} else {
			// Version B: take measurement from last unit only
			totalXByteOut = prevTotalXByteOut;
			byteAverage = (long) (totalXByteOut);
		}

		// ASSUMING AVG>TARGET...
		// Find out how many bytes we need to remove. 
		long byteRemove = byteAverage-byteTarget;
		System.out.println("ByteRemaining: " + (long) ( byteRemaining / (1024*1024)));
		System.out.println("ByteTarget: " + (long) ( byteTarget / (1024*1024)));
		System.out.println("ByteLowAverage: " + (long) ( byteAverage / (1024*1024)));
		System.out.println("ByteRemove: " + (long) ( byteRemove / (1024*1024)));
		
		// First version of filtering ratio algo: take amount of bytes to remove, take extrapolated global byte out low
		double lowXByteOut = 0;
		long byteLowAverage = 0;
		long byteTotalAverage = 0;
		if (averageWholePeriod) {
			// Version A: take average since beginning of period
			lowXByteOut = accumulatedLowXByteOut;
			byteLowAverage = (long) (lowXByteOut * 1.0 / (newCurrentUnit));
			byteTotalAverage = (long) (accumulatedTotalXByteOut * 1.0 / (newCurrentUnit));
		} else {
			// Version B: take measurement from last unit only
			lowXByteOut = prevLowXByteOut;
			byteLowAverage = (long) lowXByteOut;
			byteTotalAverage = (long) prevTotalXByteOut;
		}
		
		// For now on, just compare : byteRemove / byteAverage... we get the filtering ratio
		double rRatio = 1-(byteRemove * 1.0 / byteLowAverage);
		double rRatioAll = 1-(byteRemove * 1.0 / byteTotalAverage);
		avgRetentionRatioLow = rRatio;
		avgRetentionRatioAll = rRatioAll;
		if (avgRetentionRatioLow < 0.25)
			avgRetentionRatioLow = 0.25;
		if (avgRetentionRatioLow > 1.00)
			avgRetentionRatioLow = 1.00;
		if (avgRetentionRatioAll < 0.25)
			avgRetentionRatioAll = 0.25;
		if (avgRetentionRatioAll > 1.00)
			avgRetentionRatioAll = 1.00;
		System.out.println("FlatRetentionRatio: " + avgRetentionRatioLow);
		
		// PUT IN RETENTION RATIOS
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				//retentionRatios[i][j] = rRatio;
			}
		}
		
		// Refined version : take weights into account
		// byteRemove = sumOfWeights * m, find m
		double m = 0;
		if (sumOfWeights > 0) { // False should not happen
			m = byteRemove * 1.0 / sumOfWeights;
		}
		
		// For all tiles, compute how many bytes will be removed
		double sumByteRemoveTile = 0;
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				double byteRemoveTile =  loadFactors[i][j] * m;
				sumByteRemoveTile += byteRemoveTile;
				
				// Get byte low avg in tile
				double lowXByteOutTile = 0;
				long byteLowAverageTile = 0;
				if (averageWholePeriod) {
					// Version A: take average since beginning of period
					lowXByteOutTile = accumulatedLowXByteOutTiles[i][j];
					byteLowAverageTile = (long) (lowXByteOutTile * 1.0 / (newCurrentUnit));
				} else {
					// Version B: take measurement from last unit only
					lowXByteOutTile = prevLowXByteOutTiles[i][j];
					byteLowAverageTile = (long) (lowXByteOutTile);
				}
				
				// Compute and apply retention ratio
				double rRatioTile = 1;
				if (byteLowAverageTile != 0)
					rRatioTile = 1-(byteRemoveTile * 1.0 / byteLowAverageTile);
				
				// Filter out according to min/max
				if (rRatioTile > 1.00)
					rRatioTile = 1.00;
				else if (rRatioTile < minRetentionRatio)
					rRatioTile = minRetentionRatio;
				
				retentionRatios[i][j] = rRatioTile;
			}
		}
		System.out.println(" *****##### sumByteRemoveTile=" + sumByteRemoveTile + " | byteRemove=" + byteRemove);
		
		// Generate our filtering matrix for our network message
		HashMap<String,Double> filteringMatrix = new HashMap<String,Double>();
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				
				filteringMatrix.put("tile_" + i + "_" + j, retentionRatios[i][j]);
				
			}
		}
		// Prepare message and Send it
		UpdateRetentionRatiosControlMessage updRatiosMsg = new UpdateRetentionRatiosControlMessage(filteringMatrix);
		try {
			engine.send("plan-push-channel-lla", updRatiosMsg);
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
	}
	
	private void resetAccumulated() {
		
	}
	
	private void resetPrev() {
		this.prevTotalByteOut = 0;
		this.prevTotalXByteOut = 0;
		this.prevHighByteOut = 0;
		this.prevLowByteOut = 0;
		this.prevLowXByteOut = 0;
		
		this.prevTotalByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		this.prevTotalXByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		this.prevHighByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		this.prevLowByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		this.prevLowXByteOutTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
		
		this.prevSubscribersTiles = new long[RConfig.getTileCountX()][RConfig.getTileCountY()];
	}
	
	/**
	 * Compute planned byteOut for this period. Based on allocated quota (byteOutQuotaPerPeriod).
	 * 
	 * @return Planned byteOut for this period.
	 */
	public long getPlannedByteOutPeriod() {
		long planned = (long) (byteOutQuotaPerPeriod * (currentUnit*1.0/periodDuration));
		return planned;
	}

	// ----- //
	
	// ACCESSORS
	
	public double getByteOutCostPerGB() {
		return byteOutCostPerGB;
	}

	public int getPeriodDuration() {
		return periodDuration;
	}

	public int getUnitDuration() {
		return unitDuration;
	}

	public double getByteOutQuotaPerPeriod() {
		return byteOutQuotaPerPeriod;
	}

	public int getCurrentUnit() {
		return currentUnit;
	}

	public double getAccumulatedTotalByteOut() {
		return accumulatedTotalByteOut;
	}
	
	public double getAccumulatedTotalXByteOut() {
		return accumulatedTotalXByteOut;
	}

	public long getAccumulatedLowByteOut() {
		return accumulatedLowByteOut;
	}

	public long getAccumulatedLowXByteOut() {
		return accumulatedLowXByteOut;
	}

	public long getAccumulatedHighByteOut() {
		return accumulatedHighByteOut;
	}

	public long[][] getAccumulatedTotalByteOutTiles() {
		return accumulatedTotalByteOutTiles;
	}

	public long[][] getAccumulatedTotalXByteOutTiles() {
		return accumulatedTotalXByteOutTiles;
	}

	public long[][] getAccumulatedHighByteOutTiles() {
		return accumulatedHighByteOutTiles;
	}

	public long[][] getAccumulatedLowByteOutTiles() {
		return accumulatedLowByteOutTiles;
	}

	public long[][] getAccumulatedLowXByteOutTiles() {
		return accumulatedLowXByteOutTiles;
	}

	public double[][] getRetentionRatios() {
		return retentionRatios;
	}

	public long getInitialDynamothTime() {
		return initialDynamothTime;
	}

	public double getFilteringRatio() {
		return filteringRatio;
	}
	
	public int getPlayerCount() {
		return playerCount;
	}

	public void outputToCSV() {
		LinkedHashMap<String, String> values = new LinkedHashMap<String, String>();
		
		// Prepare values
		values.put("Unit", Integer.toString(getCurrentUnit()));
		values.put("PlayerCount", Integer.toString(playerCount));
		values.put("FlockingPlayerCount", Integer.toString(flockingCount));
		values.put("accumulatedTotalByteOut", Long.toString(accumulatedTotalByteOut));
		values.put("accumulatedTotalXByteOut", Long.toString(accumulatedTotalXByteOut));
		values.put("accumulatedHighByteOut", Long.toString(accumulatedHighByteOut));
		values.put("accumulatedLowByteOut", Long.toString(accumulatedLowByteOut));
		values.put("accumulatedLowXByteOut", Long.toString(accumulatedLowXByteOut));
		
		values.put("avgRetentionRatioLow", Double.toString(avgRetentionRatioLow));
		values.put("avgRetentionRatioAll", Double.toString(avgRetentionRatioAll));
		
		// Find min and max retention ratios
		double minRetentionRatio = 1.00;
		double maxRetentionRatio = 0.00;
		for (int i=0; i<RConfig.getTileCountX(); i++) {
			for (int j=0; j<RConfig.getTileCountY(); j++) {
				if (retentionRatios[i][j] > maxRetentionRatio)
					maxRetentionRatio = retentionRatios[i][j];
				if (retentionRatios[i][j] < minRetentionRatio)
					minRetentionRatio = retentionRatios[i][j];
			}
		}
		
		values.put("minRetentionRatio", Double.toString(minRetentionRatio));
		values.put("maxRetentionRatio", Double.toString(maxRetentionRatio));
		
		if (outputFileWriter == null) {
			try {
				
				outputFileWriter = new FileWriter("CostAnalyzer.csv");

				boolean first = true;
				for (Map.Entry<String,String> entry: values.entrySet()) {
					if (first == false) {
						outputFileWriter.write(";");
					}
					outputFileWriter.write(entry.getKey());
					first = false;
				}
				outputFileWriter.write("\n");
				
				// Todo- heatmaps in the future?
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		try {

			boolean first = true;
			for (Map.Entry<String,String> entry: values.entrySet()) {
				if (first == false) {
					outputFileWriter.write(";");
				}
				outputFileWriter.write(entry.getValue());
				first = false;
			}
			outputFileWriter.write("\n");
			outputFileWriter.flush();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	void outputFRMap() {
		try {
			FileWriter fw = new FileWriter("CostAnalyzer_FRMap_" + getCurrentUnit() + ".html");
			
			fw.write("<html>\n");
			
			fw.write("<head>\n");
			fw.write("<link rel='stylesheet' href='FRMap.css' type='text/css' />\n");
			fw.write("</head>\n");
			
			fw.write("<body>\n");
			
			fw.write("<table>\n");
			
			for (int i=0; i<RConfig.getTileCountX(); i++) {
				
				fw.write("<tr>\n");
				
				for (int j=0; j<RConfig.getTileCountY(); j++) {
		
					fw.write("<td>\n");
					
					// Generate color
					int color = (int) (retentionRatios[i][j] * 255);
					
					fw.write("<div class='content' style='background: rgb(" + color + "," + color + "," + color + ");'>" + prevSubscribersTiles[i][j] + "</div>\n");
					
					fw.write("</td>\n");
					
				}
				
				fw.write("</tr>\n");
			}
			
			fw.write("</table>\n");
			
			fw.write("</body>\n");
			
			fw.flush();
			fw.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	
	public static boolean shouldEnable() {
		return Boolean.parseBoolean(getProperties().getProperty("costanalyzer.enable", "False"));
	}
	
	private static Properties getProperties() {
		return PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
	}

}

// HTML TABLE CELL FORMAT SAMPLE http://jsfiddle.net/kQXkt/72/