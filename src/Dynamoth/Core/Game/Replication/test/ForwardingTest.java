package Dynamoth.Core.Game.Replication.test;

import java.util.ArrayList;
import java.util.List;

import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.RPubPublishMessage;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadAnalyzing.LocalLoadAnalyzer;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanId;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;


public class ForwardingTest {

	public static void main(String[] args) {
		ForwardingTest test = new ForwardingTest();
		test.planDiffTest();
	}

	private RPubNetworkID id;
	private DynamothRPubManagerStub manager;
	private LocalLoadAnalyzer analyzer;
	
	public ForwardingTest() {
		this.id = new RPubNetworkID();
		this.manager = new DynamothRPubManagerStub(this.id, new ArrayList<Plan>());
		this.analyzer = new LocalLoadAnalyzer(new RPubClientId(0), new RPubNetworkEngineStub(manager));
	}
	
	public List<Plan> createPlanHistory() {
		List<Plan> plans = new ArrayList<Plan>();
		plans.add(createPlan(0, "hello", new RPubClientId[]{new RPubClientId(0)}, PlanMappingStrategy.DEFAULT_STRATEGY));
		plans.add(createPlan(1, "hello", new RPubClientId[]{new RPubClientId(5), new RPubClientId(0), new RPubClientId(3)}, PlanMappingStrategy.SUBSCRIBERS_FULLY_CONNECTED));
		return plans;
	}
	
	public Plan createPlan(int id, String channel, RPubClientId[] shards, PlanMappingStrategy strategy) {
		PlanId pid = new PlanId(id);
		PlanImpl plan = new PlanImpl(pid);
		PlanMappingImpl map = new PlanMappingImpl(pid, channel, shards, strategy);
		plan.setMapping("hello", map);
		return plan;
	}
	
	public void forwardTest1() {
		//set message
		RPubMessage message = new RPubPublishMessage(id, "Hello");
		message.setPlanID(new PlanId(0));
		//set history
		List<Plan> history = new ArrayList<Plan>();
		history.add(createPlan(0, "hello", new RPubClientId[]{new RPubClientId(0)}, PlanMappingStrategy.DEFAULT_STRATEGY));
		history.add(createPlan(1, "hello", new RPubClientId[]{new RPubClientId(0), new RPubClientId(1)}, PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED));
		this.manager.setPlanHistory(history);
		//analyzer.forwardPublicationWithReplication("hello", message);
	}
	
	public void planDiffTest() {
		Plan oldPlan = createPlan(0, "hello", new RPubClientId[]{new RPubClientId(0)}, PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED);
		manager.setCurrentPlan(oldPlan);
		Plan newPlan = createPlan(1, "hello", new RPubClientId[]{new RPubClientId(0), new RPubClientId(1)}, PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED);
		manager.applyPlan(newPlan);
		manager.applyPlan(newPlan);
		manager.applyPlan(newPlan);
		manager.applyPlan(newPlan);


	}
}
