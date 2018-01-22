package Dynamoth.Core.Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;

/**
 * Singleton class that generates random latencies (in ms) using an input file
 * generated using the King Dataset (a set of million latency measurements between two
 * Internet hosts, see references below). The generated random latencies distribution
 * approximates precisely the latencies we can measure on the Internet.    
 * <p>
 * See :
 * <ul>
 * <li>http://www.cs.washington.edu/homes/gummadi/king/</li>
 * <li>http://pdos.csail.mit.edu/p2psim/kingdata/</li>
 * </ul>
 * <p>
 * The latencies from the original data set have been converted to a binary file (for
 * quick loading) using a small utility developed along with this project,
 * <code>KingBinaryGenerator</code>. Each byte in the binary file corresponds to one
 * latency value. See the <code>next()</code> function code to see how to obtain the
 * latency value from a byte value. Refer to the source of the
 * <code>KingBinaryGenerator</code> utility to see how the binary file has been generated.
 * 
 * @author Julien Gascon-Samson
 *
 */
public class KingDataset {
	
	/**
	 * Singleton instance variable
	 */
	private static KingDataset instance = null;
	private Random random = new Random();
	
	public static void main(String[] args) {
		// Test - create king dataset instance and output a couple of latency values
		KingDataset king = KingDataset.instance();
		int interval=50;
		FrequencyTable table = new FrequencyTable(interval);
		for (int i=0; i<10000; i++) {
			int point = king.next() / 2; // IMPORTANT - King is RTT
			System.out.print(point + " ");
			table.addDataPoint(point);
		}
		System.out.println();
		for (int i=0; i<1000; i+=interval) {
			System.out.println("[" + i + "-" + (i+interval) + "[ : " + table.getFrequency(i));
		}
	}
	
	/**
	 * Creates (if necessary) and returns the singleton instance of the
	 * <code>KingDataset</code>
	 * 
	 * @return Singleton instance of the <code>KingDataset</code>
	 */
	public static KingDataset instance() {
		if (instance == null) {
			instance = new KingDataset();
		}
		return instance;
	}
	
	/**
	 * Loaded bytes from the KingDataset binary file.
	 */
	private byte[] values = null;
	
	/**
	 * Constructs the instance of the king dataset class and read the binary file
	 */
	private KingDataset() {
		String filename = "bin" + File.separator + "KingDataset.bin";
		
		// Prepare our array based on the dataset filename
		values = new byte[(int)new File(filename).length()];
		
		// Read the king dataset
		try {
			FileInputStream input = new FileInputStream(filename);
			input.read(values);
			input.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Generates the next random latency value by choosing between all King samples and
	 * adjusting to obtain the correct value.
	 * 
	 * @return Generated next random latency value
	 */
	public int next() {
		// Get a random value
		int index = random.nextInt(values.length);
		int byteValue = values[index] + 128; // For an unsigned value
		int value = byteValue * 3;
		// If byteValue = 255 (max), then choose between 765000 and 799993
		if (byteValue == 255) {
			value += random.nextInt(800-765+1);
		}
		// Otherwise, choose between value and value + 2 (inc)
		else {
			value += random.nextInt(3);
		}
		return value;
	}
}
