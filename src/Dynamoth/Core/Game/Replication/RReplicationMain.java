package Dynamoth.Core.Game.Replication;


public class RReplicationMain {

	public static final String TEST_CHANNEL = "replication-test";

	public RReplicationMain() {
		
	}
	
	public static void main(String[] args) {
		String channelName = TEST_CHANNEL;
		int dataSize = 0;
		int numPublishers = 0;
		int numSubscribers = 0;
		if (args.length >= 4) {
			channelName += "-" + args[0];
			dataSize = Integer.parseInt(args[1]);
			numPublishers = Integer.parseInt(args[2]);
			numSubscribers = Integer.parseInt(args[3]);
		} else {
			System.out.println("Arguments error.");
			return;
		}
		
		// Launch RClient
		RReplicationClient client = new RReplicationClient(channelName, dataSize, numPublishers, numSubscribers);
		client.launch();
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
