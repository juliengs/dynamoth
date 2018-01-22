package Dynamoth.Core.Util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Julien Gascon-Samson
 */
public class MultiPubKingDataset {
	private static MultiPubKingDataset instance = null;

	private String[] regions = new String[] {"us-east-1", "us-west-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2", "sa-east-1"};
	
	private HashMap<String, List<HashMap<String, Integer>>> dataset = new HashMap<String, List<HashMap<String, Integer>>>();
	
	// Hashmap RegionName -> List (of HashMap<RegionNameString, Latency>)
	
	public static MultiPubKingDataset getInstance() {
		if (instance == null)
			instance = new MultiPubKingDataset();
		return instance;
	}
	
	public static void main(String[] args)
	{
		MultiPubKingDataset.getInstance();
	}
	
	public int getPartialLatencySample(int clientId, String sourceRegion, String rpubServerRegion) {
		List<HashMap<String, Integer>> regionDataset = dataset.get(sourceRegion);
		// Get id to fetch
		int id = clientId % regionDataset.size();
		// Fetch it
		int latency = regionDataset.get(id).get(rpubServerRegion);
		return latency;
	}
	
	public int getFullLatencySample(int clientId, String sourceRegion, String rpubServerRegion, int destinationClientId, String destinationRegion) {
		try {
			int sourceLatency = getPartialLatencySample(clientId, sourceRegion, rpubServerRegion);
			int destinationLatency = getPartialLatencySample(destinationClientId, destinationRegion, rpubServerRegion);
			return sourceLatency + destinationLatency;
		}
		catch (Exception ex) {
			ex.printStackTrace();
			return 0;
		}
	}
	
	private MultiPubKingDataset() {
		for (String region: regions) {
			List<HashMap<String, Integer>> regionDataset = readLatencyFile("bin/latency/MultiPub/MultiPubCloudLatencies_" + region + ".csv");
			dataset.put(region, regionDataset);
		}
		//System.out.println(getFullLatencySample(1, "us-east-1", "us-east-1", 2, "ap-southeast-1"));
	}
	
	private List<HashMap<String, Integer>> readLatencyFile(String filename) {
		List<HashMap<String, Integer>> samples = new ArrayList<HashMap<String, Integer>>();
		
		try {
			BufferedReader br = null;
			String line = "";
			String cvsSplitBy = ";";
			
			br = new BufferedReader(new FileReader(filename));
			boolean firstLine = true;
			
			List<String> headers = new ArrayList<String>();
			
			while ((line = br.readLine()) != null) {
				
				// use comma as separator
				String[] values = line.split(cvsSplitBy);
				
				if (firstLine) {
					firstLine = false;
					// Add all headers
					for (String value : values) {
						headers.add(value);
					}
				}
				else {
					// Not first line create a new sample
					HashMap<String, Integer> sample = new HashMap<String, Integer>();
					int index = 0;
					for (String value : values) {
						sample.put(headers.get(index), Integer.parseInt(value));
						index++;
					}
					// Add sample
					samples.add(sample);
				}
				
			}
		} catch (FileNotFoundException ex) {
			Logger.getLogger(MultiPubKingDataset.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(MultiPubKingDataset.class.getName()).log(Level.SEVERE, null, ex);
		}
	
		return samples;
	}
}
