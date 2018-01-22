package Dynamoth.Core.Util;

import java.util.LinkedHashMap;
import java.util.Map;

public class FrequencyTable {

	
	private int interval;
	private Map<Integer,Integer> table = new LinkedHashMap<Integer, Integer>();

	public FrequencyTable(int interval) {
		this.interval = interval;
	}

	public void addDataPoint(int point) {
		// Find key: int-divide by interval and remultiply by interval
		int key = (point/interval) * interval;
		if (table.containsKey(key) == false) {
			table.put(key, 0);
		}
		table.put(key, table.get(key)+1);
	}
	
	public int getFrequency(int intervalStart) {
		if (this.table.containsKey(intervalStart) == false) {
			return 0;
		} else {
			return this.table.get(intervalStart);
		}
	}
}
