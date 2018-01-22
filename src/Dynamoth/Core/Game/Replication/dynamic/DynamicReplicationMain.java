package Dynamoth.Core.Game.Replication.dynamic;

import java.io.Serializable;
import java.util.Scanner;

import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.RPubPublishMessage;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.ControlMessages.ChangePlanControlMessage;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Core.Manager.Plan.PlanId;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Util.Message.Handler;
import Dynamoth.Util.Message.Message;
import Dynamoth.Util.Message.Reactor;

public class DynamicReplicationMain {
	
	private static final String PLAN_PUSH_CHANNEL = "plan-push-channel-lla";
	private static final String DYNAMIC_REPLICATION_CHANNEL = "dynamic-replication-test";
	private static RPubNetworkEngine engine;
	private static Reactor reactor;

	static class TestString implements Message, Serializable {
		public String toString() {
			return "hello";
		}
	}
	
	public static void main(String[] args) throws Exception {
		int id = Integer.valueOf(args[0]);
		engine = new RPubNetworkEngine();
		engine.connect();
		reactor = new Reactor("check", engine);
		reactor.register(RPubPublishMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				System.out.println(msg);
			}
		});
		
    	DynamothRPubManager manager = (DynamothRPubManager) engine.getRPubManager();
    	Scanner scanner = new Scanner(System.in);
    	while(true) {
    		System.out.print(">> ");
    		String line = scanner.nextLine();
    		String[] cmdArgs = line.split(",");
    		if(cmdArgs[0].equals("subscribe")) {
    			engine.subscribeChannel(DYNAMIC_REPLICATION_CHANNEL, engine.getId());
    		} else if(cmdArgs[0].equals("publish")) {
    			engine.send(DYNAMIC_REPLICATION_CHANNEL, new TestString());
    		} else if(cmdArgs[0].equals("change")) {
    	    	PlanImpl newPlan = new PlanImpl((PlanImpl) manager.getCurrentPlan());
    			String[] shards = cmdArgs[1].split("-");
    			RPubClientId[] newShards = new RPubClientId[shards.length];
    			for(int i = 0; i < newShards.length; i++) {
    				newShards[i] = new RPubClientId(Integer.valueOf(shards[i]));
    			}
    			PlanMappingStrategy strategy = PlanMappingStrategy.DEFAULT_STRATEGY;
    			if(cmdArgs[2].equals("pfc")) {
    				strategy = PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED;
    			} else if(cmdArgs[2].equals("sfc")) {
    				strategy = PlanMappingStrategy.SUBSCRIBERS_FULLY_CONNECTED;
    			}
    			System.out.println(newPlan.getMapping("plan-push-channel").getPlanId().getId());
    			newPlan.setPlanId(new PlanId(1));

        		newPlan.setMapping(DYNAMIC_REPLICATION_CHANNEL, new PlanMappingImpl(new PlanId(1), DYNAMIC_REPLICATION_CHANNEL, newShards, strategy));
        		engine.send(PLAN_PUSH_CHANNEL, new ChangePlanControlMessage(newPlan));
    		} else if(cmdArgs[0].equals("show")) {
    			System.out.println("Current plan: " + manager.getCurrentPlan().getMapping(DYNAMIC_REPLICATION_CHANNEL).getPlanId().getId());
    		}
    	}
		
	}
}
