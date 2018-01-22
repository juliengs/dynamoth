package Dynamoth.Core.Game.Replication.test;

import java.util.ArrayList;
import java.util.List;

import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClient;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Core.Manager.Plan.Plan;

public class DynamothRPubManagerStub extends DynamothRPubManager {

	private List<Plan> history;
	
	public DynamothRPubManagerStub(RPubNetworkID networkID, List<Plan> history) {
		super(networkID, null);
		this.history = history;
	}
	
	public DynamothRPubManagerStub(RPubNetworkID networkID) {
		super(networkID, null);
		this.history = new ArrayList<Plan>();
	}
	
	public List<Plan> getPlanHistory() {
		return this.history;
	}
	
	public void setPlanHistory(List<Plan> history) {
		this.history = history;
	}
	
	public Plan getCurrentPlan() {
		return this.history.get(this.history.size() - 1);
	}
	
	public void publishToShards(RPubClient[] shards, String channelName, RPubMessage message) {
		System.out.println("===============");
		for (RPubClient client : shards) {
			System.out.println("Publishing to shard " + client.getId().getId() + " the following message " + message.getClass().getCanonicalName());
		}
		System.out.println("===============");
	}
	
	public void subscribeClientToChannel(RPubNetworkID id, String channel, boolean b) {
		System.out.println("Subscribe to "+ channel + " on " + id);
	}
	
	public void unSubscribeClientToChannel(RPubNetworkID id, String channel, boolean b) {
		System.out.println("Unsubscribe to "+ channel + " on " + id);
	}
	
	public void createChannel(String channel) {
		System.out.println("Creating channel " + channel);
	}
}
