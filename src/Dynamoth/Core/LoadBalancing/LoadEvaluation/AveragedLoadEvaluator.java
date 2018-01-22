package Dynamoth.Core.LoadBalancing.LoadEvaluation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClientId;

public class AveragedLoadEvaluator extends AbstractLoadEvaluator {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4570861921043490046L;
	private Map<Integer, LoadEvaluator> loadEvaluators = null;
	private int window = 1;
	
	public AveragedLoadEvaluator(Map<Integer, LoadEvaluator> loadEvaluators, int window, int time) {
		super(time);
		
		this.loadEvaluators = loadEvaluators;
		this.window = window;

		evaluateLoad();
	}

	@Override
	protected void evaluateLoad() {
		// Smooth values
		
		// Get last / current load evaluator
		LoadEvaluator lastEvaluator = this.loadEvaluators.get(time);
		
		for (RPubClientId clientId: lastEvaluator.getRPubClients()) {
			// Define our lists
			Map<String, List<Long>> clientChannelComputedByteInCounter = new HashMap<String, List<Long>>();
			Map<String, List<Long>> clientChannelComputedByteOutCounter = new HashMap<String, List<Long>>();
			List<Long> clientComputedByteInList = new LinkedList<Long>();
			List<Long> clientComputedByteOutList = new LinkedList<Long>();
			List<Long> clientComputedCumulativeByteInList = new LinkedList<Long>();
			List<Long> clientComputedCumulativeByteOutList = new LinkedList<Long>();
			List<Long> clientMeasuredByteInList = new LinkedList<Long>();
			List<Long> clientMeasuredByteOutList = new LinkedList<Long>();
			List<Long> clientWastedByteInList = new LinkedList<Long>();
			List<Long> clientWastedByteOutList = new LinkedList<Long>();
			List<Long> clientUnusedByteInList = new LinkedList<Long>();
			List<Long> clientUnusedByteOutList = new LinkedList<Long>();
			List<Double> clientByteInRatioList = new LinkedList<Double>();
			List<Double> clientByteOutRatioList = new LinkedList<Double>();
			List<Long> clientMessageInList = new LinkedList<Long>();
			List<Long> clientMessageOutList = new LinkedList<Long>();
			Map<String, List<Integer>> clientChannelSubscribersCounter = new HashMap<String, List<Integer>>();
			Map<String, List<Integer>> clientChannelPublishersCounter = new HashMap<String, List<Integer>>();
			Map<String, List<Integer>> clientChannelPublicationsCounter = new HashMap<String, List<Integer>>();
			Map<String, List<Integer>> clientChannelSentMessagesCounter = new HashMap<String, List<Integer>>();
			// Publications of publishers (of channels of clientId)!
			Map<String, Map<RPubNetworkID, List<Integer>>> clientChannelPublisherPublicationsCounter = new HashMap<String, Map<RPubNetworkID, List<Integer>>>();
			// TODO- KEEP WORKING ON THAT...

			for (int i=time; i>time-this.window; i--) {
				LoadEvaluator le = loadEvaluators.get(i);
				if (le == null)
					continue;
				
				// Initialize all integer lists
				clientComputedByteInList.add(le.getClientComputedByteIn(clientId));
				clientComputedByteOutList.add(le.getClientComputedByteOut(clientId));
				clientComputedCumulativeByteInList.add(le.getClientComputedCumulativeByteIn(clientId));
				clientComputedCumulativeByteOutList.add(le.getClientComputedCumulativeByteOut(clientId));
				clientMeasuredByteInList.add(le.getClientMeasuredByteIn(clientId));
				clientMeasuredByteOutList.add(le.getClientMeasuredByteOut(clientId));
				clientWastedByteInList.add(le.getClientWastedByteIn(clientId));
				clientWastedByteOutList.add(le.getClientWastedByteOut(clientId));
				clientUnusedByteInList.add(le.getClientUnusedByteIn(clientId));
				clientUnusedByteOutList.add(le.getClientUnusedByteOut(clientId));
				clientByteInRatioList.add(le.getClientByteInRatio(clientId));
				clientByteOutRatioList.add(le.getClientByteOutRatio(clientId));
				clientMessageInList.add(le.getClientMessageIn(clientId));
				clientMessageOutList.add(le.getClientMessageOut(clientId));
				
				// Obtain all channels for this time slot
				// In order to initialize all maps
				for (String channel: le.getClientChannels(clientId)) {
					if (clientChannelComputedByteInCounter.containsKey(channel) == false)
						clientChannelComputedByteInCounter.put(channel, new LinkedList<Long>());
					if (clientChannelComputedByteOutCounter.containsKey(channel) == false)
						clientChannelComputedByteOutCounter.put(channel, new LinkedList<Long>());
					if (clientChannelSubscribersCounter.containsKey(channel) == false)
						clientChannelSubscribersCounter.put(channel, new LinkedList<Integer>());
					if (clientChannelPublishersCounter.containsKey(channel) == false)
						clientChannelPublishersCounter.put(channel, new LinkedList<Integer>());
					if (clientChannelPublicationsCounter.containsKey(channel) == false)
						clientChannelPublicationsCounter.put(channel, new LinkedList<Integer>());
					if (clientChannelSentMessagesCounter.containsKey(channel) == false)
						clientChannelSentMessagesCounter.put(channel, new LinkedList<Integer>());
					
					// For the extended cases of publications of publishers of channels, we need to initialize
					// the hashmap but we will also need to initialize the list itself for every RPubNetworkID
					if (clientChannelPublisherPublicationsCounter.containsKey(channel) == false) {
						// Initialize the hashmap RPubNetworkID -> List<Integer>
						clientChannelPublisherPublicationsCounter.put(channel, new HashMap<RPubNetworkID, List<Integer>>());
						Map<RPubNetworkID, List<Integer>> publicationsPublisher = clientChannelPublisherPublicationsCounter.get(channel);
						// Initialize all List<Integer>
						for (RPubNetworkID publisher: le.getClientChannelPublisherList(clientId, channel)) {
							publicationsPublisher.put(publisher, new LinkedList<Integer>());
							
							// Add this counter (# of publications for publisher for channel for client)...
							publicationsPublisher.get(publisher).add(le.getClientChannelPublisherPublications(clientId, channel, publisher));
							
						}
					}
					
					clientChannelComputedByteInCounter.get(channel).add(le.getClientChannelComputedByteIn(clientId, channel));
					clientChannelComputedByteOutCounter.get(channel).add(le.getClientChannelComputedByteOut(clientId, channel));
					clientChannelSubscribersCounter.get(channel).add(le.getClientChannelSubscribers(clientId, channel));
					clientChannelPublishersCounter.get(channel).add(le.getClientChannelPublishers(clientId, channel));
					clientChannelPublicationsCounter.get(channel).add(le.getClientChannelPublications(clientId, channel));
					clientChannelSentMessagesCounter.get(channel).add(le.getClientChannelSentMessages(clientId, channel));

					// Special case: for lists of subscribers and publishers, add all subscribers and publishers in all load evaluators
					// over the averaging window
					if (this.clientChannelSubscriberList.containsKey(clientId) == false)
						this.clientChannelSubscriberList.put(clientId, new HashMap<String, Set<RPubNetworkID>>());
					if (this.clientChannelPublisherList.containsKey(clientId) == false)
						this.clientChannelPublisherList.put(clientId, new HashMap<String, Set<RPubNetworkID>>());
					
					Set<RPubNetworkID> subscriberList = le.getClientChannelSubscriberList(clientId, channel);
					Set<RPubNetworkID> publisherList = le.getClientChannelPublisherList(clientId, channel);
					// Ensure that the lists are not null
					if (subscriberList == null)
						subscriberList = new HashSet<RPubNetworkID>();
					if (publisherList == null)
						publisherList = new HashSet<RPubNetworkID>();
					// If sets are null then initialize them
					if (this.clientChannelSubscriberList.get(clientId).get(channel) == null) {
						this.clientChannelSubscriberList.get(clientId).put(channel, new HashSet<RPubNetworkID>());
					}
					if (this.clientChannelPublisherList.get(clientId).get(channel) == null) {
						this.clientChannelPublisherList.get(clientId).put(channel, new HashSet<RPubNetworkID>());
					}

					// Obtain the sets of our avg load evaluator
					Set<RPubNetworkID> globalSubscriberList = this.clientChannelSubscriberList.get(clientId).get(channel);
					Set<RPubNetworkID> globalPublisherList = this.clientChannelPublisherList.get(clientId).get(channel);
					
					// Add all subscribers and publishers to our global lists
					globalSubscriberList.addAll(subscriberList);
					globalPublisherList.addAll(publisherList);
					
				}
			}
			
			// Smooth all values
			this.clientComputedByteIn.put(clientId, smoothLongs(clientComputedByteInList));
			this.clientComputedByteOut.put(clientId, smoothLongs(clientComputedByteOutList));
			this.clientComputedCumulativeByteIn.put(clientId, smoothLongs(clientComputedCumulativeByteInList));
			this.clientComputedCumulativeByteOut.put(clientId, smoothLongs(clientComputedCumulativeByteOutList));
			this.clientMeasuredByteIn.put(clientId, smoothLongs(clientMeasuredByteInList));
			this.clientMeasuredByteOut.put(clientId, smoothLongs(clientMeasuredByteOutList));
			this.clientWastedByteIn.put(clientId, smoothLongs(clientWastedByteInList));
			this.clientWastedByteOut.put(clientId, smoothLongs(clientWastedByteOutList));
			this.clientUnusedByteIn.put(clientId, smoothLongs(clientUnusedByteInList));
			this.clientUnusedByteOut.put(clientId, smoothLongs(clientUnusedByteOutList));
			this.clientByteInRatio.put(clientId, smoothDoubles(clientByteInRatioList));
			this.clientByteOutRatio.put(clientId, smoothDoubles(clientByteOutRatioList));
			this.clientMessageIn.put(clientId, smoothLongs(clientMessageInList));
			this.clientMessageOut.put(clientId, smoothLongs(clientMessageOutList));

			// Do the same for all channels for channel-computed byte in and byte out
			this.clientChannelComputedByteIn.put(clientId, new HashMap<String, Long>());
			this.clientChannelComputedByteOut.put(clientId, new HashMap<String, Long>());
			this.clientChannelSubscribers.put(clientId, new HashMap<String, Integer>());
			this.clientChannelPublishers.put(clientId, new HashMap<String, Integer>());
			this.clientChannelPublications.put(clientId, new HashMap<String, Integer>());
			this.clientChannelSentMessages.put(clientId, new HashMap<String, Integer>());
			
			this.clientChannelPublisherPublications.put(clientId, new HashMap<String, Map<RPubNetworkID, Integer>>());
			
			for (String channel: clientChannelComputedByteInCounter.keySet()) {
				this.clientChannelComputedByteIn.get(clientId).put(channel, smoothLongs(clientChannelComputedByteInCounter.get(channel)));
				this.clientChannelComputedByteOut.get(clientId).put(channel, smoothLongs(clientChannelComputedByteOutCounter.get(channel)));
				this.clientChannelSubscribers.get(clientId).put(channel, smoothInts(clientChannelSubscribersCounter.get(channel)));
				this.clientChannelPublishers.get(clientId).put(channel, smoothInts(clientChannelPublishersCounter.get(channel)));
				this.clientChannelPublications.get(clientId).put(channel, smoothInts(clientChannelPublicationsCounter.get(channel)));
				this.clientChannelSentMessages.get(clientId).put(channel, smoothInts(clientChannelSentMessagesCounter.get(channel)));

				this.clientChannelPublisherPublications.get(clientId).put(channel, new HashMap<RPubNetworkID, Integer>());
				
				// We need to go one step further: for all publishers, smooth ints
				for (RPubNetworkID publisher: clientChannelPublisherPublicationsCounter.get(channel).keySet()) {
					this.clientChannelPublisherPublications.get(clientId).get(channel).put(publisher, smoothInts(clientChannelPublisherPublicationsCounter.get(channel).get(publisher)));
				}
			}
			
		}
		
	}
	
	private long smoothLongs(List<Long> longList) {
		if (longList.isEmpty())
			return 0;
		
		int sum = 0;
		for (Long i: longList) {
			sum += i;
		}
		return sum/longList.size();
	}
	
	private int smoothInts(List<Integer> intList) {
		if (intList.isEmpty())
			return 0;
		
		int sum = 0;
		for (Integer i: intList) {
			sum += i;
		}
		return sum/intList.size();
	}
	
	private double smoothDoubles(List<Double> doubleList) {
		if (doubleList.isEmpty())
			return 0;
		
		double sum = 0;
		for (Double d: doubleList) {
			sum += d;
		}
		return sum/doubleList.size();
	}
}

