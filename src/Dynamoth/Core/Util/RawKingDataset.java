package Dynamoth.Core.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RawKingDataset {

	/**
	 * Singleton instance variable
	 */
	public static RawKingDataset instance = null;
	private Random random = new Random();
	
	public static void main(String[] args) {
		// Test - create king dataset instance and output a couple of latency values
		RawKingDataset king = RawKingDataset.instance();
		int interval=50;
		FrequencyTable table = new FrequencyTable(interval);
		for (int i=0; i<10000; i++) {
			int point = king.nextInt() / 2; // IMPORTANT - King is RTT
			//System.out.print(point + " ");
			table.addDataPoint(point);
		}
		System.out.println();
		for (int i=0; i<1000; i+=interval) {
			System.out.println("[" + i + "-" + (i+interval) + "[ : " + table.getFrequency(i));
		}
	}
	
	/**
	 * Creates (if necessary) and returns the singleton instance of the
	 * <code>RawKingDataset</code>
	 * 
	 * @return Singleton instance of the <code>RawKingDataset</code>
	 */
	public static RawKingDataset instance() {
		if (instance == null) {
			instance = new RawKingDataset();
		}
		return instance;
	}
	
	/**
	 * List of latencies as read from the metrics file
	 */
	private List<Float> latencies = new ArrayList<Float>();
	
	public RawKingDataset() {
		String filename = "bin/latency" + File.separator + "king_measurements_matrix_usa";
		// Load all latencies
		this.readLatencies(filename);
		
	}

	private void readLatencies(String filename) {
		try {
			
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			
			try{
				String line = null;
				while (( line = reader.readLine()) != null) {
					
					// Space-split to get each value
					String[] latencies = line.split(" ");
					// Add every non-null latency
					for (String latency: latencies) {
						if (latency.equals(""))
							continue;
						
						try {
							float fLatency = Float.parseFloat(latency);
							
							if (fLatency > 0.0) {
								this.latencies.add(fLatency);
							}
						} catch (NumberFormatException ex) {
							continue;
						}
						
					}
					
				}
			}
			finally {
				reader.close();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Generates the next random latency value by choosing between all raw King samples and
	 * adjusting to obtain the correct value.
	 * 
	 * @return Generated next random latency value as float
	 */
	public float nextFloat() {
		int index = random.nextInt(latencies.size());
		float latency = this.latencies.get(index);
		return latency;
	}
	
	/**
	 * Generates the next random latency value by choosing between all raw King samples and
	 * adjusting to obtain the correct value.
	 * 
	 * @return Generated next random latency value as int
	 */
	public int nextInt() {
		int index = random.nextInt(latencies.size());
		int latency = Math.round(this.latencies.get(index));
		return latency;
	}
}
