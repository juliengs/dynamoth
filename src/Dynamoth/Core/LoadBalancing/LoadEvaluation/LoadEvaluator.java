package Dynamoth.Core.LoadBalancing.LoadEvaluation;

import java.io.Serializable;
import java.util.Set;

import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.Plan;

public interface LoadEvaluator extends Serializable {

	Set<RPubClientId> getRPubClients();

	Set<String> getClientChannels(RPubClientId client);
	
	int getClientChannelSubscribers(RPubClientId client, String channel);
	
	int getClientChannelPublishers(RPubClientId client, String channel);
	
	Set<RPubNetworkID> getClientChannelSubscriberList(RPubClientId client, String channel);
	
	Set<RPubNetworkID> getClientChannelPublisherList(RPubClientId client, String channel);
	
	int getClientChannelPublisherPublications(RPubClientId client, String channel, RPubNetworkID publisher);
	
	int getClientChannelPublications(RPubClientId client, String channel);
	
	int getClientChannelSentMessages(RPubClientId client, String channel);
	
	long getClientChannelComputedByteIn(RPubClientId client, String channel);
	
	long getClientChannelComputedByteOut(RPubClientId client, String channel);
	
	long getClientComputedByteIn(RPubClientId client);

	long getClientComputedByteOut(RPubClientId client);

	long getClientComputedCumulativeByteIn(RPubClientId client);

	long getClientComputedCumulativeByteOut(RPubClientId client);

	long getClientMeasuredByteIn(RPubClientId client);

	long getClientMeasuredByteOut(RPubClientId client);

	long getClientWastedByteIn(RPubClientId client);

	long getClientWastedByteOut(RPubClientId client);

	long getClientUnusedByteIn(RPubClientId client);

	long getClientUnusedByteOut(RPubClientId client);

	double getClientByteInRatio(RPubClientId client);

	double getClientByteOutRatio(RPubClientId client);
	
	long getClientMessageIn(RPubClientId client);
	
	long getClientMessageOut(RPubClientId client);
	
	RPubClientId getClientHighestByteOut();
	
	RPubClientId getClientHighestByteOut(Set<RPubClientId> activeHosts);
	
	RPubClientId getClientLowestByteOut();
	
	RPubClientId getClientLowestByteOut(Set<RPubClientId> activeHosts);
	
	String getClientChannelHighestByteOut(RPubClientId client);
	
	String getClientChannelHighestByteOut(RPubClientId client, Set<String> ignoreChannels);
	
	String getClientChannelHighestByteOut(RPubClientId client, Set<String> ignoreChannels, Plan plan);
	

}