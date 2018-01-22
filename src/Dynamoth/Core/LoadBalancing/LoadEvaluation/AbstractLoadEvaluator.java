package Dynamoth.Core.LoadBalancing.LoadEvaluation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.Plan;

public abstract class AbstractLoadEvaluator implements LoadEvaluator {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8632610804934179646L;

	protected int time;
	
	protected long totalComputedByteIn = 0;
	protected long totalComputedCumulativeByteIn = 0;
	protected long totalComputedByteOut = 0;
	protected long totalComputedCumulativeByteOut = 0;
	
	protected Map<RPubClientId,Map<String,Integer>> clientChannelSubscribers = new HashMap<RPubClientId, Map<String,Integer>>();
	protected Map<RPubClientId,Map<String,Integer>> clientChannelPublishers = new HashMap<RPubClientId, Map<String,Integer>>();
	protected Map<RPubClientId,Map<String,Set<RPubNetworkID>>> clientChannelSubscriberList = new HashMap<RPubClientId, Map<String,Set<RPubNetworkID>>>();
	protected Map<RPubClientId,Map<String,Set<RPubNetworkID>>> clientChannelPublisherList = new HashMap<RPubClientId, Map<String,Set<RPubNetworkID>>>();
	protected Map<RPubClientId,Map<String,Map<RPubNetworkID,Integer>>> clientChannelPublisherPublications = new HashMap<RPubClientId, Map<String,Map<RPubNetworkID,Integer>>>();
	protected Map<RPubClientId,Map<String,Integer>> clientChannelPublications = new HashMap<RPubClientId, Map<String,Integer>>();
	protected Map<RPubClientId,Map<String,Integer>> clientChannelSentMessages = new HashMap<RPubClientId, Map<String,Integer>>();
	
	protected Map<RPubClientId,Map<String,Long>> clientChannelComputedByteIn = new HashMap<RPubClientId, Map<String,Long>>();
	protected Map<RPubClientId,Map<String,Long>> clientChannelComputedByteOut = new HashMap<RPubClientId, Map<String,Long>>();
	
	protected Map<RPubClientId,Long> clientComputedByteIn = new HashMap<RPubClientId, Long>();
	protected Map<RPubClientId,Long> clientComputedCumulativeByteIn = new HashMap<RPubClientId, Long>();
	protected Map<RPubClientId,Long> clientComputedByteOut = new HashMap<RPubClientId, Long>();
	protected Map<RPubClientId,Long> clientComputedCumulativeByteOut = new HashMap<RPubClientId, Long>();
	protected Map<RPubClientId,Long> clientMeasuredByteIn = new HashMap<RPubClientId,Long>();
	protected Map<RPubClientId,Long> clientMeasuredByteOut = new HashMap<RPubClientId,Long>();
	protected Map<RPubClientId,Long> clientWastedByteIn = new HashMap<RPubClientId,Long>();
	protected Map<RPubClientId,Long> clientWastedByteOut = new HashMap<RPubClientId,Long>();
	protected Map<RPubClientId,Long> clientUnusedByteIn = new HashMap<RPubClientId,Long>();
	protected Map<RPubClientId,Long> clientUnusedByteOut = new HashMap<RPubClientId,Long>();
	protected Map<RPubClientId,Double> clientByteInRatio = new HashMap<RPubClientId,Double>();
	protected Map<RPubClientId,Double> clientByteOutRatio = new HashMap<RPubClientId,Double>();
	protected Map<RPubClientId,Long> clientMessageIn = new HashMap<RPubClientId,Long>();
	protected Map<RPubClientId,Long> clientMessageOut = new HashMap<RPubClientId,Long>();
	
	public AbstractLoadEvaluator(int time) {
		super();
		
		this.time = time;
	}

	/**
	 * Perform Load Evaluation and cache results.
	 */
	protected abstract void evaluateLoad();

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getRPubClients()
	 */
	@Override
	public Set<RPubClientId> getRPubClients() {
		return this.clientComputedByteIn.keySet();
	}

	public long getTotalComputedByteIn() {
		return this.totalComputedByteIn;
	}

	public long getTotalComputedByteOut() {
		return this.totalComputedByteOut;
	}

	public long getTotalComputedCumulativeByteIn() {
		return this.totalComputedCumulativeByteIn;
	}

	public long getTotalComputedCumulativeByteOut() {
		return this.totalComputedCumulativeByteOut;
	}

	@Override
	public Set<String> getClientChannels(RPubClientId client) {
		if (this.clientChannelComputedByteIn.get(client) == null)
			return new HashSet<String>();
		else
			return this.clientChannelComputedByteIn.get(client).keySet();
	}
	
	@Override
	public int getClientChannelSubscribers(RPubClientId client, String channel) {
		if (this.clientChannelSubscribers.get(client) == null || this.clientChannelSubscribers.get(client).containsKey(channel) == false)
			return 0;
		else
			return this.clientChannelSubscribers.get(client).get(channel);
	}
	
	@Override
	public int getClientChannelPublishers(RPubClientId client, String channel) {
		if (this.clientChannelPublishers.get(client) == null || this.clientChannelPublishers.get(client).containsKey(channel) == false)
			return 0;
		else
			return this.clientChannelPublishers.get(client).get(channel);
	}
	
	@Override
	public Set<RPubNetworkID> getClientChannelSubscriberList(RPubClientId client, String channel) {
		if (this.clientChannelSubscriberList.get(client) == null || this.clientChannelSubscriberList.get(client).containsKey(channel) == false)
			return new HashSet<RPubNetworkID>();
		else
			return this.clientChannelSubscriberList.get(client).get(channel);
	}

	@Override
	public Set<RPubNetworkID> getClientChannelPublisherList(RPubClientId client, String channel) {
		if (this.clientChannelPublisherList.get(client) == null || this.clientChannelPublisherList.get(client).containsKey(channel) == false)
			return new HashSet<RPubNetworkID>();
		else
			return this.clientChannelPublisherList.get(client).get(channel);
	}
	
	@Override
	public int getClientChannelPublisherPublications(RPubClientId client, String channel, RPubNetworkID publisher) {
		if (this.clientChannelPublisherPublications.get(client) == null || this.clientChannelPublisherPublications.get(client).containsKey(channel) == false || this.clientChannelPublisherPublications.get(client).get(channel).containsKey(publisher) == false)
			return 0;
		else
			return this.clientChannelPublisherPublications.get(client).get(channel).get(publisher);
	}

	@Override
	public int getClientChannelPublications(RPubClientId client, String channel) {
		if (this.clientChannelPublications.get(client) == null || this.clientChannelPublications.get(client).containsKey(channel) == false)
			return 0;
		else
			return this.clientChannelPublications.get(client).get(channel);
	}
	
	@Override
	public int getClientChannelSentMessages(RPubClientId client, String channel) {
		if (this.clientChannelSentMessages.get(client) == null || this.clientChannelSentMessages.get(client).containsKey(channel) == false)
			return 0;
		else
			return this.clientChannelSentMessages.get(client).get(channel);
	}

	@Override
	public long getClientChannelComputedByteIn(RPubClientId client, String channel) {
		if (this.clientChannelComputedByteIn.get(client) == null || this.clientChannelComputedByteIn.get(client).containsKey(channel) == false)
			return 0;
		else
			return this.clientChannelComputedByteIn.get(client).get(channel);
	}

	@Override
	public long getClientChannelComputedByteOut(RPubClientId client, String channel) {
		if (this.clientChannelComputedByteOut.get(client) == null || this.clientChannelComputedByteOut.get(client).containsKey(channel) == false)
			return 0;
		else
			return this.clientChannelComputedByteOut.get(client).get(channel);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientComputedByteIn(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientComputedByteIn(RPubClientId client) {
		return this.clientComputedByteIn.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientComputedByteOut(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientComputedByteOut(RPubClientId client) {
		return this.clientComputedByteOut.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientComputedCumulativeByteIn(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientComputedCumulativeByteIn(RPubClientId client) {
		return this.clientComputedCumulativeByteIn.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientComputedCumulativeByteOut(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientComputedCumulativeByteOut(RPubClientId client) {
		return this.clientComputedCumulativeByteOut.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientMeasuredByteIn(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientMeasuredByteIn(RPubClientId client) {
		return this.clientMeasuredByteIn.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientMeasuredByteOut(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientMeasuredByteOut(RPubClientId client) {
		return this.clientMeasuredByteOut.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientWastedByteIn(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientWastedByteIn(RPubClientId client) {
		return this.clientWastedByteIn.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientWastedByteOut(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientWastedByteOut(RPubClientId client) {
		return this.clientWastedByteOut.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientUnusedByteIn(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientUnusedByteIn(RPubClientId client) {
		return this.clientUnusedByteIn.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientUnusedByteOut(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public long getClientUnusedByteOut(RPubClientId client) {
		return this.clientUnusedByteOut.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientByteInRatio(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public double getClientByteInRatio(RPubClientId client) {
		return this.clientByteInRatio.get(client);
	}

	/* (non-Javadoc)
	 * @see Mammoth.NetworkEngine.RPub.LoadBalancing.LoadEvaluation.LoadEvaluator#getClientByteOutRatio(Mammoth.NetworkEngine.RPub.Client.RPubClientId)
	 */
	@Override
	public double getClientByteOutRatio(RPubClientId client) {
		return this.clientByteOutRatio.get(client);
	}

	@Override
	public long getClientMessageIn(RPubClientId client) {
		return this.clientMessageIn.get(client);
	}

	@Override
	public long getClientMessageOut(RPubClientId client) {
		return this.clientMessageOut.get(client);
	}

	@Override
	public RPubClientId getClientHighestByteOut() {
		return getClientHighestByteOut(getRPubClients());
	}

	@Override
	public RPubClientId getClientLowestByteOut() {
		return getClientLowestByteOut(getRPubClients());
	}

	@Override
	public RPubClientId getClientHighestByteOut(Set<RPubClientId> activeHosts) {
		RPubClientId highest = null;
		double highLoadRatio = Double.MIN_VALUE;
		for (RPubClientId clientId: this.getRPubClients()) {
			if (this.getClientByteOutRatio(clientId) > highLoadRatio && activeHosts.contains(clientId)) {
				highest = clientId;
				highLoadRatio = this.getClientByteOutRatio(clientId);
			}
		}
		return highest;
	}

	@Override
	public RPubClientId getClientLowestByteOut(Set<RPubClientId> activeHosts) {
		RPubClientId lowest = null;
		double lowLoadRatio = Double.MAX_VALUE;
		for (RPubClientId clientId: this.getRPubClients()) {
			if (this.getClientByteOutRatio(clientId) < lowLoadRatio && activeHosts.contains(clientId)) {
				lowest = clientId;
				lowLoadRatio = this.getClientByteOutRatio(clientId);
			}
		}
		return lowest;
	}

	@Override
	public String getClientChannelHighestByteOut(RPubClientId client) {
		return getClientChannelHighestByteOut(client, new HashSet<String>());
	}

	@Override
	public String getClientChannelHighestByteOut(RPubClientId client, Set<String> ignoreChannels) {
		return getClientChannelHighestByteOut(client, ignoreChannels, null);
	}

	@Override
	public String getClientChannelHighestByteOut(RPubClientId client, Set<String> ignoreChannels, Plan plan) {
		String highest = null;
		long highByteOut = Long.MIN_VALUE;
		for (String channel: this.getClientChannels(client)) {
			// Check if channel should be ignored
			if (ignoreChannels.contains(channel))
				continue;
			// Channel should also be ignored if not part of the plan!
			if (plan.getClientChannels(client).contains(channel) == false)
				continue;
			
			if (this.getClientChannelComputedByteOut(client, channel) > highByteOut) {
				highest = channel;
				highByteOut = this.getClientChannelComputedByteOut(client, channel); 
			}
		}
		return highest;				
	}

	protected void copyTo(AbstractLoadEvaluator loadEvaluator) {
		loadEvaluator.totalComputedByteIn = this.totalComputedByteIn;
		loadEvaluator.totalComputedCumulativeByteIn = this.totalComputedCumulativeByteIn;
		loadEvaluator.totalComputedByteOut = this.totalComputedByteOut;
		loadEvaluator.totalComputedCumulativeByteOut = this.totalComputedCumulativeByteOut;
		
		// A deep copy will be needed
		for (RPubClientId clientId: this.clientChannelSubscribers.keySet()) {
			loadEvaluator.clientChannelSubscribers.put(clientId, new HashMap<String,Integer>(this.clientChannelSubscribers.get(clientId)));
		}
		for (RPubClientId clientId: this.clientChannelPublishers.keySet()) {
			loadEvaluator.clientChannelPublishers.put(clientId, new HashMap<String,Integer>(this.clientChannelPublishers.get(clientId)));
		}
		for (RPubClientId clientId: this.clientChannelSubscriberList.keySet()) {
			// For each channel...
			// Set new empty map entry in hashmap
			Map<String, Set<RPubNetworkID>> newEntry = new HashMap<String, Set<RPubNetworkID>>();
			loadEvaluator.clientChannelSubscriberList.put(clientId, newEntry);
			for (String channel: this.clientChannelSubscriberList.get(clientId).keySet()) {
				// Copy set of subscribers
				newEntry.put(channel, new HashSet<RPubNetworkID>(this.clientChannelSubscriberList.get(clientId).get(channel)));
			}
		}
		for (RPubClientId clientId: this.clientChannelPublisherList.keySet()) {
			// For each channel...
			// Set new empty map entry in hashmap
			Map<String, Set<RPubNetworkID>> newEntry = new HashMap<String, Set<RPubNetworkID>>();
			loadEvaluator.clientChannelPublisherList.put(clientId, newEntry);
			for (String channel: this.clientChannelPublisherList.get(clientId).keySet()) {
				// Copy set of publishers
				newEntry.put(channel, new HashSet<RPubNetworkID>(this.clientChannelPublisherList.get(clientId).get(channel)));
			}
		}
		for (RPubClientId clientId: this.clientChannelPublisherPublications.keySet()) {
			// For each channel...
			// Set new empty map entry in hashmap
			Map<String, Map<RPubNetworkID, Integer>> newEntry = new HashMap<String, Map<RPubNetworkID, Integer>>();
			loadEvaluator.clientChannelPublisherPublications.put(clientId, newEntry);
			for (String channel: this.clientChannelPublisherPublications.get(clientId).keySet()) {
				// Copy map of publishers->#publications
				newEntry.put(channel, new HashMap<RPubNetworkID, Integer>(this.clientChannelPublisherPublications.get(clientId).get(channel)));
			}
		}
		for (RPubClientId clientId: this.clientChannelPublications.keySet()) {
			loadEvaluator.clientChannelPublications.put(clientId, new HashMap<String,Integer>(this.clientChannelPublications.get(clientId)));
		}
		for (RPubClientId clientId: this.clientChannelSentMessages.keySet()) {
			loadEvaluator.clientChannelSentMessages.put(clientId, new HashMap<String,Integer>(this.clientChannelSentMessages.get(clientId)));
		}
		for (RPubClientId clientId: this.clientChannelComputedByteIn.keySet()) {
			loadEvaluator.clientChannelComputedByteIn.put(clientId, new HashMap<String,Long>(this.clientChannelComputedByteIn.get(clientId)));
		}
		for (RPubClientId clientId: this.clientChannelComputedByteOut.keySet()) {
			loadEvaluator.clientChannelComputedByteOut.put(clientId, new HashMap<String,Long>(this.clientChannelComputedByteOut.get(clientId)));
		}
		
		loadEvaluator.clientComputedByteIn = new HashMap<RPubClientId, Long>(this.clientComputedByteIn);
		loadEvaluator.clientComputedByteOut = new HashMap<RPubClientId, Long>(this.clientComputedByteOut);
		loadEvaluator.clientComputedCumulativeByteIn = new HashMap<RPubClientId, Long>(this.clientComputedCumulativeByteIn);
		loadEvaluator.clientComputedCumulativeByteOut = new HashMap<RPubClientId, Long>(this.clientComputedCumulativeByteOut);
		
		loadEvaluator.clientMeasuredByteIn = new HashMap<RPubClientId, Long>(this.clientMeasuredByteIn);
		loadEvaluator.clientMeasuredByteOut = new HashMap<RPubClientId, Long>(this.clientMeasuredByteOut);
		
		loadEvaluator.clientWastedByteIn = new HashMap<RPubClientId, Long>(this.clientWastedByteIn);
		loadEvaluator.clientWastedByteOut = new HashMap<RPubClientId, Long>(this.clientWastedByteOut);
		
		loadEvaluator.clientUnusedByteIn = new HashMap<RPubClientId, Long>(this.clientUnusedByteIn);
		loadEvaluator.clientUnusedByteOut = new HashMap<RPubClientId, Long>(this.clientUnusedByteOut);
		
		loadEvaluator.clientByteInRatio = new HashMap<RPubClientId, Double>(this.clientByteInRatio);
		loadEvaluator.clientByteOutRatio = new HashMap<RPubClientId, Double>(this.clientByteOutRatio);
		
		loadEvaluator.clientMessageIn = new HashMap<RPubClientId, Long>(this.clientMessageIn);
		loadEvaluator.clientMessageOut = new HashMap<RPubClientId, Long>(this.clientMessageOut);
	}
}