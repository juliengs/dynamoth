package Dynamoth.Core;

import java.util.Random;

import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;

public class RPubNetworkID implements NetworkEngineID {
	private static final long serialVersionUID = 1032658101640217754L;

	private Integer id;
	private String domain; // new: for the cloud: domain

	public static int lastId = 1;
	
	public RPubNetworkID() {
		super();
		
		// For a randomly generated id...
		Random random = new Random();
		// [0-1000[ are reserved for RPub clients -> NO
		this.id = random.nextInt(Integer.MAX_VALUE);
		setDomainFromEC2Region();
	}
	
	public RPubNetworkID(int id) {
		super();
		
		this.id = id;
		setDomainFromEC2Region();
	}
	
	private void setDomainFromEC2Region() {
		this.domain = System.getProperty("ec2.region", ""); 
	}
	
	public Integer getId() {
		return id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((domain == null) ? 0 : domain.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return "RPubNetworkID [id=" + id + ", domain=" + domain + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RPubNetworkID other = (RPubNetworkID) obj;
		if (domain == null) {
			if (other.domain != null)
				return false;
		} else if (!domain.equals(other.domain))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}
}
