package Dynamoth.Core.LoadAnalyzing;

import java.io.Serializable;

public class SliceStatsPublicationCounter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8072107302108614936L;

	private int publications = 0;   	// # of incoming messages
	
	private int sentMessages = 0;   	// # of sent messages, will be equal to publications * subscribers = sentMessages
										// if subscribers doesn't change; otherwise it might not match exactly
	
	private long byteIn = 0;			// # of bytes received
	private long byteOut = 0;			// # of bytes transmitted
	
	public SliceStatsPublicationCounter() {
		super();
	}

	public SliceStatsPublicationCounter(int publications, int sentMessages,
			long byteIn, long byteOut) {
		super();
		this.publications = publications;
		this.sentMessages = sentMessages;
		this.byteIn = byteIn;
		this.byteOut = byteOut;
	}

	public int getPublications() {
		return publications;
	}

	public void setPublications(int publications) {
		this.publications = publications;
	}

	public int getSentMessages() {
		return sentMessages;
	}

	public void setSentMessages(int sentMessages) {
		this.sentMessages = sentMessages;
	}

	public long getByteIn() {
		return byteIn;
	}

	public void setByteIn(long byteIn) {
		this.byteIn = byteIn;
	}

	public long getByteOut() {
		return byteOut;
	}

	public void setByteOut(long byteOut) {
		this.byteOut = byteOut;
	}
	
	// Incrementers
	public int incrementPublications(int publications) {
		this.publications += publications;
		return this.publications;
	}
	
	public int incrementSentMessages(int sentMessages) {
		this.sentMessages += sentMessages;
		return this.sentMessages;
	}
	
	public long incrementByteIn(long byteIn) {
		this.byteIn += byteIn;
		return this.byteIn;
	}
	
	public long incrementByteOut(long byteOut) {
		this.byteOut += byteOut;
		return this.byteOut;
	}
}
