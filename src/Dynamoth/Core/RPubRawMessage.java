package Dynamoth.Core;

import java.io.Serializable;

public class RPubRawMessage extends RPubMessage {

	private static final long serialVersionUID = -8101897938668403L;

	// Message payload
	private String payload;
		
	public RPubRawMessage(RPubNetworkID sourceID, String payload)
	{
		super(sourceID);
		this.payload = payload;
	}

	/**
	 * @return the payload
	 */
	public String getPayload() {
		return payload;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((payload == null) ? 0 : payload.hashCode());
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
		RPubRawMessage other = (RPubRawMessage) obj;
		if (payload == null) {
			if (other.payload != null)
				return false;
		} else if (!payload.equals(other.payload))
			return false;
		return true;
	}
	
}
