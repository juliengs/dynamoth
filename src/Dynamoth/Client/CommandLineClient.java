package Dynamoth.Client;

import Dynamoth.Core.ExternalClient.ExternalClient;
import Dynamoth.Core.RPubHubWrapper;
import Dynamoth.Core.Game.RMain;
import Dynamoth.Core.Game.RServer;
import Dynamoth.Core.Game.Replication.RReplicationMain;
import Dynamoth.Core.Game.Replication.dynamic.DReplicationMain;
import Dynamoth.Core.LoadBalancing.LoadBalancer;
import Dynamoth.Core.Util.KingDataset;
import Dynamoth.Core.Util.MaxThreadsMain;
import Dynamoth.Core.Util.RawKingDataset;

public class CommandLineClient {

	public static void main(String[] args) throws Exception {

		// Echo ec2.region JVM property
		String ec2Region = System.getProperty("ec2.region", "");
		System.out.println("EC2-Region: " + ec2Region);
		
		if (args.length < 0) {
			System.err.println("Invalid arguments");
		}

		String mode = args[0];

		if (mode.equals("rpubhub")) {
			String hubId = "0";
			if (args.length>0) {
				// We might have the HUB-id... pass it
				hubId = args[1];
			}
			RPubHubWrapper.main(new String[]{hubId});			
		} else if (mode.equals("rgame")) {
			// Recopy all args
			String[] rgameArgs = new String[args.length - 1];
			for (int i=1; i<args.length; i++) {
				rgameArgs[i-1] = args[i];
			}
			RMain.main(rgameArgs);
		} else if (mode.equals("rserver")) {
			RServer.main(new String[]{""});
		} else if (mode.equals("loadbalancer")) {
			LoadBalancer.main(new String[]{""});
		} else if (mode.equals("replication-test")) {
			String[] rRepArgs = new String[args.length - 1];
			for (int i=1; i<args.length; i++) {
				rRepArgs[i-1] = args[i];
			}
			RReplicationMain.main(rRepArgs);
		} else if (mode.equals("dynamic-replication-test")) {
			String[] rRepArgs = new String[args.length - 1];
			for (int i=1; i<args.length; i++) {
				rRepArgs[i-1] = args[i];
			}
			DReplicationMain.main(rRepArgs);
		} else if (mode.equals("kingtest")) {
			KingDataset.main(new String[] {});
		} else if (mode.equals("rawkingtest")) {
			RawKingDataset.main(new String[] {});
		} else if (mode.equals("maxthreadsmain")) {
			MaxThreadsMain.main(new String[] {});
		} else if (mode.equals("externalclient")) {
			ExternalClient.main(new String[] {});
		}
		
	}
}
