package Dynamoth.Core.Game.Replication.dynamic;


public class DReplicationMain {

	public static final String TEST_CHANNEL = "replication-test";

	public DReplicationMain() {
		
	}
	
	public static void main(String[] args) {
		String channelSuffix = "default";
		int numPublishers = 0;
		int numSubscribers = 0;
		int delay = 0; // in ms
		int burst = 0;
		boolean decrease = false;
		if (args.length >= 6) {
			numPublishers = Integer.valueOf(args[0]);
			numSubscribers = Integer.valueOf(args[1]);
			delay = Integer.valueOf(args[2]);
			burst = Integer.valueOf(args[3]);
			decrease = Boolean.valueOf(args[4]);
			channelSuffix = args[5];
		} else {
			System.out.println("Arguments error.");
			return;
		}
		String channelName = TEST_CHANNEL + "-" + channelSuffix;

		// Launch RClient
		DReplicationClient client = new DReplicationClient(channelName, numPublishers, numSubscribers, delay, burst, decrease);
		client.connectPotentialPlayers();
		client.launch();
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
