package Dynamoth.Core.Game;

public class RMain {

	public RMain() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Parse args
		int playerStart = 0, playerCount = 1, totalPlayerCount = 1;
		if (args.length >= 3) {
			// PlayerStart, PlayerCount, TotalPlayerCount
			playerStart = Integer.parseInt(args[0]);
			playerCount = Integer.parseInt(args[1]);
			totalPlayerCount = Integer.parseInt(args[2]);
		}
		
		// Launch RClient
		RClient client = new RClient(playerStart, playerCount, totalPlayerCount);
		client.launch();
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
