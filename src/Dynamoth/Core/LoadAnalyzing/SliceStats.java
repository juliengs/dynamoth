package Dynamoth.Core.LoadAnalyzing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.RPubNetworkID;

public class SliceStats implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2966876866532125061L;

	private int subscribers = 0;    	// # of subscribers (may change!) - should then be the average? or final?
										// or do we care if the timedelta is small enough?
										// might be removed because we can simply count the number of items in the
										// subscriber list...
	
	private SliceStatsPublicationCounter publicationStats = new SliceStatsPublicationCounter();
										// Publication statistics. Replaces publications, sentMessages, byteIn, byteOut
	
	private Map<RPubNetworkID, SliceStatsPublicationCounter> publishers =
			new HashMap<RPubNetworkID, SliceStatsPublicationCounter>();
										// map of publishers -> publication counter structure
										// we then have	publication stats for each individual publisher
	
	private Set<RPubNetworkID> subscriberList = new HashSet<RPubNetworkID>(); // list of subscribers. two roles:
												// 1- prevent double subscriptions for the same node
												// 2- optimizing partitionning (ie grouping channels with the same set of users together)
	
	// What appears below doesn't appear to be relevant anymore... could be removed in the future
	
	private int cumulativePublications = 0;  // cumulative # of publications (for statistics purposes)
	private int cumulativeSentMessages = 0;  // cumulative # of sent messages (for statistics purposes)
	
	private long cumulativeByteIn = 0;        // cumulative # of incoming bytes (for statistics purposes)
	private long cumulativeByteOut = 0;       // cumulative # of outgoing bytes (for statistics purposes)
	
	public SliceStats() {
		
	}

	public SliceStats(SliceStats copy) {
		// Copy the number and list of subscribers because they carry over to the next time slice!
		// As well as the totals for statistics purposes
		this.cumulativePublications = copy.cumulativePublications;
		this.subscribers = copy.subscribers;
		this.subscriberList = new HashSet<RPubNetworkID>(copy.subscriberList);
		this.cumulativeSentMessages = copy.cumulativeSentMessages;
		this.cumulativeByteIn = copy.cumulativeByteIn;
		this.cumulativeByteOut = copy.cumulativeByteOut;
	}
	

	public int getSubscribers() {
		return subscribers;
	}
	
	public void setSubscribers(int subscribers) {
		this.subscribers = subscribers;
	}

	public Set<RPubNetworkID> getSubscriberList() {
		return subscriberList;
	}

	public int getCumulativePublications() {
		return cumulativePublications;
	}

	public void setCumulativePublications(int totalPublications) {
		this.cumulativePublications = totalPublications;
	}

	public int getCumulativeSentMessages() {
		return cumulativeSentMessages;
	}

	public void setCumulativeSentMessages(int totalSentMessages) {
		this.cumulativeSentMessages = totalSentMessages;
	}

	public long getCumulativeByteIn() {
		return cumulativeByteIn;
	}

	public void setCumulativeByteIn(long totalByteIn) {
		this.cumulativeByteIn = totalByteIn;
	}

	public long getCumulativeByteOut() {
		return cumulativeByteOut;
	}

	public void setCumulativeByteOut(long totalByteOut) {
		this.cumulativeByteOut = totalByteOut;
	}
	
	// Incrementers
	public int incrementSubscribers(int subscribers) {
		this.subscribers += subscribers;
		return this.subscribers;
	}
	
	public int incrementCumulativePublications(int totalPublications) {
		this.cumulativePublications += totalPublications;
		return this.cumulativePublications;
	}
	
	public int incrementCumulativeSentMessages(int totalSentMessages) {
		this.cumulativeSentMessages += totalSentMessages;
		return this.cumulativeSentMessages;
	}
	
	public long incrementCumulativeByteIn(long totalByteIn) {
		this.cumulativeByteIn += totalByteIn;
		return this.cumulativeByteIn;
	}
	
	public long incrementCumulativeByteOut(long totalByteOut) {
		this.cumulativeByteOut += totalByteOut;
		return this.cumulativeByteOut;
	}

	public SliceStatsPublicationCounter getPublicationStats() {
		return publicationStats;
	}

	public Map<RPubNetworkID, SliceStatsPublicationCounter> getPublishers() {
		return publishers;
	}
	
}
