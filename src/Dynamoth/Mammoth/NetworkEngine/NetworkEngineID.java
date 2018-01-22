package Dynamoth.Mammoth.NetworkEngine;

import java.io.Serializable;

/**
 * The ID uniquely identifying a participant in Mammoth. This replaces the 
 * previous ID which was simply an int.
 * 
 * @version Nov 2, 2007
 * @author Dominik Zindel
 * 
 */
public interface NetworkEngineID extends Serializable {
	
	/**
	 * Check if this ID is equal to another id.
	 * 
	 * @param id The ID with which this ID has to be compared.
	 * @return A boolean indicating if the two IDs are equal.
	 */
	public boolean equals(Object o);

	public int hashCode();
}
