package Dynamoth.Core.LoadBalancing.LoadEvaluation;

import java.util.Map;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanDiff;
import Dynamoth.Core.Manager.Plan.PlanDiffImpl;
import Dynamoth.Core.Util.RPubHostInfo;

public class NewPlanEstimatedLoadEvaluator extends AbstractLoadEvaluator {

	private Plan currentPlan = null;
	private Plan proposedPlan = null;
	private LoadEvaluator currentLoadEvaluator = null;
	private Map<RPubClientId, RPubHostInfo> hostInfoMap = null;
	
	public NewPlanEstimatedLoadEvaluator(Plan currentPlan, Plan proposedPlan,
			LoadEvaluator currentLoadEvaluator, Map<RPubClientId, RPubHostInfo> hostInfoMap, int time) {
		super(time);
		this.currentPlan = currentPlan;
		this.proposedPlan = proposedPlan;
		this.currentLoadEvaluator = currentLoadEvaluator;
		this.hostInfoMap = hostInfoMap;
		
		evaluateLoad();
	}

	@Override
	protected void evaluateLoad() {
		// Compute planDiff
		PlanDiff planDiff = new PlanDiffImpl(this.currentPlan, this.proposedPlan, 0);
		
		// Copy 'fixed' stuff from current evaluator
		((AbstractLoadEvaluator)(this.currentLoadEvaluator)).copyTo(this);
		
		// The magic happens here: for all "changing" channels (between rpub clients),
		// add and remove them from the proper rpub clients and adjust the bytein/byteout counters
		
		for (String channel: planDiff.getChannels()) {
			// Right now we assume no sharding 
			// TODO: PROPERLY EVALUATE LOAD TAKING INTO ACCOUNT SHARDING (!)
			
			if (planDiff.getOldMapping(channel).getShards().length > 1 || planDiff.getNewMapping(channel).getShards().length > 1) {
				continue;
			}
			
			// Get old and new rub client ids
			RPubClientId oldClientId = planDiff.getOldMapping(channel).getShards()[0];
			RPubClientId newClientId = planDiff.getNewMapping(channel).getShards()[0];
			
			// TODO: correctly handle channels which are not in the stats
			// Will crash at this point!
			// Ex: if channel "Bonjour" does not exist, then it will crash at the next 2 lines
			
			// Get the current current load for that channel
			long channelByteIn = this.getClientChannelComputedByteIn(oldClientId, channel);
			long channelByteOut = this.getClientChannelComputedByteOut(oldClientId, channel);
			int channelSubscribers = this.getClientChannelSubscribers(oldClientId, channel);
			int channelPublishers = this.getClientChannelPublishers(oldClientId, channel);
			int channelPublications = this.getClientChannelPublications(oldClientId, channel);
			int channelSentMessages = this.getClientChannelSentMessages(oldClientId, channel);

			// Substract the bytein and byteout from the old client ID and add it to the new client id
			
			this.clientChannelComputedByteIn.get(oldClientId).put(channel, 0L);
			this.clientChannelComputedByteIn.get(newClientId).put(channel, channelByteIn);
			
			this.clientChannelComputedByteOut.get(oldClientId).put(channel, 0L);
			this.clientChannelComputedByteOut.get(newClientId).put(channel, channelByteOut);
			
			this.clientChannelSubscribers.get(oldClientId).put(channel, 0);
			this.clientChannelSubscribers.get(newClientId).put(channel, channelSubscribers);
			
			this.clientChannelPublishers.get(oldClientId).put(channel, 0);
			this.clientChannelPublishers.get(newClientId).put(channel, channelPublishers);
			
			this.clientChannelPublications.get(oldClientId).put(channel, 0);
			this.clientChannelPublications.get(newClientId).put(channel, channelPublications);
			
			this.clientChannelSentMessages.get(oldClientId).put(channel, 0);
			this.clientChannelSentMessages.get(newClientId).put(channel, channelSentMessages);

			// Compute measured-to-computed multipliers
			
			double measuredToComputedRatioOldIn = 1.0;
			if (this.clientComputedByteIn.get(oldClientId) != 0)
				measuredToComputedRatioOldIn = 1.0 * this.clientMeasuredByteIn.get(oldClientId) / this.clientComputedByteIn.get(oldClientId);
			double measuredToComputedRatioOldOut = 1.0;
			if (this.clientComputedByteOut.get(oldClientId) != 0)
				measuredToComputedRatioOldOut = 1.0 * this.clientMeasuredByteOut.get(oldClientId) / this.clientComputedByteOut.get(oldClientId);
			double measuredToComputedRatioNewIn = 1.0;
			if (this.clientComputedByteIn.get(newClientId) != 0)
				measuredToComputedRatioNewIn = 1.0 * this.clientMeasuredByteIn.get(newClientId) / this.clientComputedByteIn.get(newClientId);
			double measuredToComputedRatioNewOut = 1.0;
			if (this.clientComputedByteOut.get(newClientId) != 0)
				measuredToComputedRatioNewOut = 1.0 * this.clientMeasuredByteOut.get(newClientId) / this.clientComputedByteOut.get(newClientId);
			
			// Computed byte-in and byte-out
			this.clientComputedByteIn.put(oldClientId, this.clientComputedByteIn.get(oldClientId) - channelByteIn);
			this.clientComputedByteOut.put(oldClientId, this.clientComputedByteOut.get(oldClientId) - channelByteOut);
			this.clientComputedByteIn.put(newClientId, this.clientComputedByteIn.get(newClientId) + channelByteIn);
			this.clientComputedByteOut.put(newClientId, this.clientComputedByteOut.get(newClientId) + channelByteOut);
			
			// Measured bytein and byteout - also de/increment them because here we assume that the difference in
			// measured bytein/out will be roughly the same as the difference in computed bytein/out
			// NOT TRUE ANYMORE: multiply those ratios by one of our measured-to-computed ratios
			this.clientMeasuredByteIn.put(oldClientId, this.clientMeasuredByteIn.get(oldClientId) - (long) (channelByteIn * measuredToComputedRatioOldIn));
			this.clientMeasuredByteOut.put(oldClientId, this.clientMeasuredByteOut.get(oldClientId) - (long) (channelByteOut * measuredToComputedRatioOldOut));
			this.clientMeasuredByteIn.put(newClientId, this.clientMeasuredByteIn.get(newClientId) + (long) (channelByteIn * measuredToComputedRatioNewIn));
			this.clientMeasuredByteOut.put(newClientId, this.clientMeasuredByteOut.get(newClientId) + (long) (channelByteOut * measuredToComputedRatioNewOut));			
			
			// Same for unused but it is the contrary, ie if we add to measured then we remove from unused
			// NOT TRUE ANYMORE: multiply those ratios by one of our measured-to-computed ratios
			this.clientUnusedByteIn.put(oldClientId, this.clientUnusedByteIn.get(oldClientId) + (long) (channelByteIn * measuredToComputedRatioOldIn));
			this.clientUnusedByteOut.put(oldClientId, this.clientUnusedByteOut.get(oldClientId) + (long) (channelByteOut * measuredToComputedRatioOldOut));
			this.clientUnusedByteIn.put(newClientId, this.clientUnusedByteIn.get(newClientId) - (long) (channelByteIn * measuredToComputedRatioNewIn));
			this.clientUnusedByteOut.put(newClientId, this.clientUnusedByteOut.get(newClientId) - (long) (channelByteOut * measuredToComputedRatioNewOut));			
			
			// (We assume wasted will remain the same...)
			
			// Recompute ratios - trivial
			this.clientByteInRatio.put(oldClientId, this.getClientMeasuredByteIn(oldClientId) * 1.0 / hostInfoMap.get(oldClientId).getMaxByteIn());
			this.clientByteOutRatio.put(oldClientId, this.getClientMeasuredByteOut(oldClientId) * 1.0 / hostInfoMap.get(oldClientId).getMaxByteOut());
			this.clientByteInRatio.put(newClientId, this.getClientMeasuredByteIn(newClientId) * 1.0 / hostInfoMap.get(newClientId).getMaxByteIn());
			this.clientByteOutRatio.put(newClientId, this.getClientMeasuredByteOut(newClientId) * 1.0 / hostInfoMap.get(newClientId).getMaxByteOut());
			
			// Let's assume list of subscribers and publishers remain the same -- nothing to change :-)
		}
	}

}
