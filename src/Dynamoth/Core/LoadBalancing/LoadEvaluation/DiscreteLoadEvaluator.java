package Dynamoth.Core.LoadBalancing.LoadEvaluation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadAnalyzing.Channel;
import Dynamoth.Core.LoadAnalyzing.SliceStats;
import Dynamoth.Core.LoadAnalyzing.SliceStatsPublicationCounter;
import Dynamoth.Core.Util.RPubHostInfo;

public class DiscreteLoadEvaluator extends AbstractLoadEvaluator {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3886455163570600472L;
	protected Map<RPubClientId, RPubHostInfo> hostInfoMap;
	protected Map<RPubClientId, Map<String, Channel>> channels;
	protected Map<RPubClientId, Map<Integer, Long>> measuredByteIn;
	protected Map<RPubClientId, Map<Integer, Long>> measuredByteOut;
	
	// Time point for the last availability of metric data
	protected Map<RPubClientId, Integer> clientLastUpdateTimes;
	
	public DiscreteLoadEvaluator(
			Map<RPubClientId, RPubHostInfo> hostInfoMap,
			Map<RPubClientId, Map<String, Channel>> channels,
			Map<RPubClientId, Map<Integer, Long>> measuredByteIn,
			Map<RPubClientId, Map<Integer, Long>> measuredByteOut,
			Map<RPubClientId, Integer> clientLastUpdateTimes, 
			int time) {
		
		super(time);
		
		this.hostInfoMap = hostInfoMap;
		this.channels = channels;
		this.measuredByteIn = measuredByteIn;
		this.measuredByteOut = measuredByteOut;
		this.clientLastUpdateTimes = clientLastUpdateTimes;
		
		evaluateLoad();
	}

	@Override
	protected void evaluateLoad() {
		// Compute in and out bandwidth for all clients and for all channels
		for (Map.Entry<RPubClientId, Map<String, Channel>> channelsEntry: channels.entrySet()) {
			try
			{
				// Compute load
				Map<String, Channel> channelsMap = channelsEntry.getValue();
				
				long inBandwidth = 0;
				long outBandwidth = 0;
				long cumulativeInBandwidth = 0;
				long cumulativeOutBandwidth = 0;
				long inMessage = 0;
				long outMessage = 0;
				
				// Iterate through each channel and do the sum
				for (Map.Entry<String,Channel> channelEntry: channelsMap.entrySet()) {
					String channelName = channelEntry.getKey();
					Channel channel = channelEntry.getValue();
					// If a slice stats exists for the current time, then take it
					// Otherwise, take the 'latest' slice stats
					int time = this.time;
					if (this.time > channel.getLastTime()) {
						time = channel.getLastTime();
					}
					// Get slice stats at current time
					SliceStats stats = channel.getSliceStats(time);
					
					// Put in-out bandwidth for channel
					if (this.clientChannelSubscribers.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelSubscribers.put(channelsEntry.getKey(), new HashMap<String, Integer>());
					if (this.clientChannelPublishers.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelPublishers.put(channelsEntry.getKey(), new HashMap<String, Integer>());
					if (this.clientChannelSubscriberList.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelSubscriberList.put(channelsEntry.getKey(), new HashMap<String, Set<RPubNetworkID>>());
					if (this.clientChannelPublisherList.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelPublisherList.put(channelsEntry.getKey(), new HashMap<String, Set<RPubNetworkID>>());
					if (this.clientChannelPublisherPublications.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelPublisherPublications.put(channelsEntry.getKey(), new HashMap<String, Map<RPubNetworkID, Integer>>());
					if (this.clientChannelPublications.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelPublications.put(channelsEntry.getKey(), new HashMap<String, Integer>());
					if (this.clientChannelSentMessages.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelSentMessages.put(channelsEntry.getKey(), new HashMap<String, Integer>());
					if (this.clientChannelComputedByteIn.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelComputedByteIn.put(channelsEntry.getKey(), new HashMap<String, Long>());
					if (this.clientChannelComputedByteOut.containsKey(channelsEntry.getKey()) == false)
						this.clientChannelComputedByteOut.put(channelsEntry.getKey(), new HashMap<String, Long>());
					this.clientChannelSubscribers.get(channelsEntry.getKey()).put(channelEntry.getKey(), stats.getSubscribers());
					this.clientChannelPublishers.get(channelsEntry.getKey()).put(channelEntry.getKey(), stats.getPublishers().size());
					// Copy list of subscribers and publishers
					this.clientChannelSubscriberList.get(channelsEntry.getKey()).put(channelEntry.getKey(), new HashSet<RPubNetworkID>(stats.getSubscriberList()));
					this.clientChannelPublisherList.get(channelsEntry.getKey()).put(channelEntry.getKey(), new HashSet<RPubNetworkID>(stats.getPublishers().keySet()));
					Map<RPubNetworkID,Integer> publisherPublications = new HashMap<RPubNetworkID, Integer>();
					this.clientChannelPublisherPublications.get(channelsEntry.getKey()).put(channelEntry.getKey(), publisherPublications);
					// Copy each publication counter for each publisher
					for (Map.Entry<RPubNetworkID,SliceStatsPublicationCounter> publisherEntry: stats.getPublishers().entrySet()) {
						publisherPublications.put(publisherEntry.getKey(), publisherEntry.getValue().getPublications());
					}
					// Then copy other stuff...
					this.clientChannelPublications.get(channelsEntry.getKey()).put(channelEntry.getKey(), stats.getPublicationStats().getPublications());
					this.clientChannelSentMessages.get(channelsEntry.getKey()).put(channelEntry.getKey(), stats.getPublicationStats().getSentMessages());
					this.clientChannelComputedByteIn.get(channelsEntry.getKey()).put(channelEntry.getKey(), stats.getPublicationStats().getByteIn());
					this.clientChannelComputedByteOut.get(channelsEntry.getKey()).put(channelEntry.getKey(), stats.getPublicationStats().getByteOut());
					
					// Sum in-out bandwidth
					inBandwidth += stats.getPublicationStats().getByteIn();
					outBandwidth += stats.getPublicationStats().getByteOut();
					cumulativeInBandwidth += stats.getCumulativeByteIn();
					cumulativeOutBandwidth += stats.getCumulativeByteOut();
					inMessage += stats.getPublicationStats().getPublications();
					outMessage += stats.getPublicationStats().getSentMessages();
				}
				
				// Put in map
				this.clientComputedByteIn.put(channelsEntry.getKey(), inBandwidth);
				this.clientComputedCumulativeByteIn.put(channelsEntry.getKey(), cumulativeInBandwidth);
				this.clientComputedByteOut.put(channelsEntry.getKey(), outBandwidth);
				this.clientComputedCumulativeByteOut.put(channelsEntry.getKey(), cumulativeOutBandwidth);
				this.clientMessageIn.put(channelsEntry.getKey(), inMessage);
				this.clientMessageOut.put(channelsEntry.getKey(), outMessage);
				
				// Increase total in-out bandwidth
				this.totalComputedByteIn += inBandwidth;
				this.totalComputedCumulativeByteIn += cumulativeInBandwidth;
				this.totalComputedByteOut += outBandwidth;
				this.totalComputedCumulativeByteOut += cumulativeOutBandwidth;
			} catch (NullPointerException ex) {
				// Do nothing
				ex.printStackTrace();
			}
		}
		
		// Extract measured bytein and byteout for the current time
		for (RPubClientId clientId: this.channels.keySet()) {
			Map<Integer,Long> measuredByteInTime = this.measuredByteIn.get(clientId);
			Map<Integer,Long> measuredByteOutTime = this.measuredByteOut.get(clientId);
			Integer clientLastUpdateTime = clientLastUpdateTimes.get(clientId);
			long mByteIn  = 0;
			long mByteOut = 0;
			// If variables is null then we assume mbytein = 0, mbyteout = 0 to prevent NullPointerExceptions
			if (	measuredByteInTime != null &&
					measuredByteOutTime != null &&
					clientLastUpdateTime != null &&
					measuredByteInTime.get(clientLastUpdateTime) != null &&
					measuredByteOutTime.get(clientLastUpdateTime) != null) {
				
				mByteIn = measuredByteInTime.get(clientLastUpdateTime);
				mByteOut = measuredByteOutTime.get(clientLastUpdateTime);
			}
			this.clientMeasuredByteIn.put(clientId, mByteIn);
			this.clientMeasuredByteOut.put(clientId, mByteOut);
		}
		
		// Compute Wasted bandwidth (simple) -> W=M-C
		for (RPubClientId clientId: clientComputedByteIn.keySet()) {
			//Bytein
			clientWastedByteIn.put(clientId, clientMeasuredByteIn.get(clientId) - clientComputedByteIn.get(clientId));
			//Byteout
			clientWastedByteOut.put(clientId, clientMeasuredByteOut.get(clientId) - clientComputedByteOut.get(clientId));
		}
		
		// Compute Unused bandwidth (simple) -> U=T-C-W or U=T-M
		for (RPubClientId clientId: clientMeasuredByteIn.keySet()) {
			// Lookup in hostinfomap
			
			//Bytein
			clientUnusedByteIn.put(clientId, hostInfoMap.get(clientId).getMaxByteIn() - clientMeasuredByteIn.get(clientId));
			//Byteout
			clientUnusedByteOut.put(clientId, hostInfoMap.get(clientId).getMaxByteOut() - clientMeasuredByteOut.get(clientId));
		}
		
		// Compute byte-in and byte-out ratios
		for (RPubClientId clientId: clientMeasuredByteIn.keySet()) {
			//Bytein
			clientByteInRatio.put(clientId, clientMeasuredByteIn.get(clientId) * 1.0 / hostInfoMap.get(clientId).getMaxByteIn());
			//Byteout
			clientByteOutRatio.put(clientId, clientMeasuredByteOut.get(clientId) * 1.0 / hostInfoMap.get(clientId).getMaxByteOut());
		}
	}
	
}
