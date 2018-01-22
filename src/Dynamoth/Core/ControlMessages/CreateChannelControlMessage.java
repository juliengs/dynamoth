package Dynamoth.Core.ControlMessages;

public class CreateChannelControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8205636811702616952L;

	private String channelName = "";
	
	public CreateChannelControlMessage(String channelName) {
		super();
		this.channelName = channelName;
	}

	public String getChannelName() {
		return this.channelName;
	}
}
