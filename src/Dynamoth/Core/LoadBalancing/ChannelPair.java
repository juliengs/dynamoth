package Dynamoth.Core.LoadBalancing;

public class ChannelPair {

	private String channel1 = null, channel2 = null;
	
	
	public ChannelPair(String channel1, String channel2) {
		if (channel1.hashCode() <= channel2.hashCode()) {
			this.channel1 = channel1;
			this.channel2 = channel2;
		} else {
			this.channel1 = channel2;
			this.channel2 = channel1;
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((channel1 == null) ? 0 : channel1.hashCode());
		result = prime * result
				+ ((channel2 == null) ? 0 : channel2.hashCode());
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
		ChannelPair other = (ChannelPair) obj;
		if (channel1 == null) {
			if (other.channel1 != null)
				return false;
		} else if (!channel1.equals(other.channel1))
			return false;
		if (channel2 == null) {
			if (other.channel2 != null)
				return false;
		} else if (!channel2.equals(other.channel2))
			return false;
		return true;
	}

	public String getChannel1() {
		return channel1;
	}

	public String getChannel2() {
		return channel2;
	}

}
