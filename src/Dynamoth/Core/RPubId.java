package Dynamoth.Core;

import java.io.Serializable;

public abstract class RPubId implements Serializable, Comparable<RPubId> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8422967933931910554L;
	
	protected int id = -1;

	protected RPubId() {
	}

	public RPubId(int id) {
		this.id = id;
	}
	
	public int getId() {
		return this.id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RPubId other = (RPubId) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
	@Override
	public int compareTo(RPubId other) {
		return (new Integer(this.id)).compareTo(other.id);
	}
}
