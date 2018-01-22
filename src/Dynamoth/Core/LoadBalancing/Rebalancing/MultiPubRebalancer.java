package Dynamoth.Core.LoadBalancing.Rebalancing;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.ShardsSelector.DynaWANLocalShardSelector;
import Dynamoth.Core.Util.RPubHostInfo;
import Dynamoth.Core.Util.RPubUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Julien Gascon-Samson
 */
public class MultiPubRebalancer extends LoadBasedRebalancer {

	public MultiPubRebalancer(Plan currentPlan, int currentTime, LoadEvaluator currentLoadEvaluator, Map<RPubClientId, RPubHostInfo> hostInfoMap) {
		super(currentPlan, currentTime, currentLoadEvaluator, hostInfoMap);
	}
	
	public static void main(String[] args) {
		MultiPubRebalancer reb = new MultiPubRebalancer(null, 0, null, null);
		reb.processIteration();
	}
	
	protected void writeSolverXML(String solverPath) {
		try {
			FileWriter writer = new FileWriter(solverPath + "/solver.xml");
			
			writer.write("<MultiPubExperiments>\n");
			writer.write("	<Experiments>\n");
			writer.write("		<Experiment name='solver'>\n");
			writer.write("			<Topics>\n");
			writer.write("				<Topic name='T1'>\n");
			writer.write("					<Publishers>\n");
			writer.write("					<Repeat count='1'>\n");
			
			// For every rpub client
			Set<RPubNetworkID> publishers = new HashSet<RPubNetworkID>();
			Set<RPubNetworkID> subscribers = new HashSet<RPubNetworkID>();
			for (RPubClientId client : currentLoadEvaluator.getRPubClients()) {
				
				// For every publisher
				for (RPubNetworkID publisher : currentLoadEvaluator.getClientChannelPublisherList(client, "tile_0_0")) {
					publishers.add(publisher);
				}
				
				// For every subscriber
				for (RPubNetworkID subscriber : currentLoadEvaluator.getClientChannelSubscriberList(client, "tile_0_0")) {
					subscribers.add(subscriber);
				}
			}
			for (RPubNetworkID publisher : publishers) {
				String domain = publisher.getDomain();
				if (domain.equals("")) {
					domain = DynaWANLocalShardSelector.defaultRegion;
				}
				writer.write("						<Publisher region='" + domain + "' publications='1' publicationSize='102400' />\n");
			}
			writer.write("					</Repeat>\n");			
			writer.write("					</Publishers>\n");
			writer.write("					<Subscribers>\n");
			writer.write("					<Repeat count='1'>\n");

			// For every publisher
			for (RPubNetworkID subscriber : subscribers) {
				String domain = subscriber.getDomain();
				if (domain.equals("")) {
					domain = DynaWANLocalShardSelector.defaultRegion;
				}
				writer.write("						<Subscriber region='" + domain + "' />\n");
			}

			writer.write("					</Repeat>\n");			
			writer.write("					</Subscribers>\n");
			writer.write("				</Topic>\n");
			writer.write("			</Topics>\n");
			writer.write("		</Experiment>\n");
			writer.write("	</Experiments>\n");
			writer.write("</MultiPubExperiments>\n");
			
			writer.flush();
			writer.close();
			
		} catch (IOException ex) {
			Logger.getLogger(MultiPubRebalancer.class.getName()).log(Level.SEVERE, null, ex);
		}
		
	}

	@Override
	protected void processIteration() {
		// Make sure we can set a new plan
		if (this.canSetNewPlan() == false)
			return;
		
		String[] regions = null;
		int cost, latency;
		
		String solverPath = RPubUtil.stringProperty("multipub.solver.path");
		
		// Write solver XML
		writeSolverXML(solverPath);
		
		// Build our python process to run the MultiPub Solver
		ProcessBuilder ps=new ProcessBuilder("python3", "solver.py");
		ps.directory(new File(solverPath));
		ps.redirectErrorStream(true);
		Process pr;
				
		try {
			pr = ps.start();
			BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				if (line.startsWith("REGIONS")) {
					regions = line.split(":")[1].split(";");
				} else if (line.startsWith("METRICS")) {
					cost = Integer.parseInt(line.split(":")[1].split(";")[0]);
					latency = Integer.parseInt(line.split(":")[1].split(";")[0]);
				}
				//System.out.println(line);
			}
			pr.waitFor();

			in.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
		
		if (regions == null)
			return;
		
		// Just print all regions as chosen by solver
		for (String region : regions) {
			System.out.println(region);
		}
		
		// Build our new proposed plan
		PlanImpl proposedPlan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
		
		// Build new planMapping
		RPubClientId[] shards = new RPubClientId[regions.length];
		int i=0;
		for (String region : regions) {
			shards[i] = new RPubClientId(DynaWANLocalShardSelector.regionToShard(region));
			i++;
		}
		PlanMappingImpl mapping = new PlanMappingImpl(proposedPlan.getPlanId(), "tile_0_0", shards, PlanMapping.PlanMappingStrategy.DYNAWAN_ROUTING);
		
		// Only alter topic tile_0_0
		proposedPlan.setMapping("tile_0_0", mapping);
		
		// Set the new plan
		System.out.println("SETTING_PLAN");
		setNewPlan(proposedPlan);
	}
	
}
