package Dynamoth.Core.Util;

import Dynamoth.Client.Client;
import Dynamoth.Util.Properties.PropertyManager;
import java.util.Properties;
import java.util.Random;

public class RPubUtil {

	private static Random random = new Random();
	
	public static int parseRPubClientId(String server) {
		return Integer.parseInt(server.split(":")[0]);
	}
	
	public static String parseRPubHostName(String server) {
		return server.split(":")[1];
	}
	
	public static String parseRPubHostDomain(String server) {
		return server.split(":")[2];
	}
	
	public static int parseRPubHostPort(String server) {
		String hostPort = server.split(":")[3];
		if (hostPort.equals("")) {
			return 6379;
		} else {
			return Integer.parseInt(hostPort);
		}
	}
	
	public static int parseRPubHostKByteIn(String server) {
		return Integer.parseInt(server.split(":")[4]);
	}
	
	public static int parseRPubHostKByteOut(String server) {
		return Integer.parseInt(server.split(":")[5]);
	}
	
	public static long kiloBytesToBytes(long bytes) {
		return bytes * 1024;
	}
	
	public static int getCurrentSystemTime() {
		return (int) (System.currentTimeMillis() / 1000);
	}
	
	public static Random getRandom() {
		return random;
	}
	
	private static Properties getProperties() {
		return PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
	}
	
	public static int intProperty(String name) {
		return intProperty(name, 0);
	}
	
	public static int intProperty(String name, int defaultValue) {
		return Integer.parseInt(getProperties().getProperty(name, Integer.toString(defaultValue)));
	}
	
	public static boolean boolProperty(String name) {
		return boolProperty(name, false);
	}
	
	public static boolean boolProperty(String name, boolean defaultValue) {
		return Boolean.parseBoolean(getProperties().getProperty(name, Boolean.toString(defaultValue)));
	}
	
	public static double doubleProperty(String name) {
		return doubleProperty(name, 0.0);
	}
	
	public static double doubleProperty(String name, double defaultValue) {
		return Double.parseDouble(getProperties().getProperty(name, Double.toString(defaultValue)));
	}
	
	public static String stringProperty(String name) {
		return stringProperty(name, "");
	}
	
	public static String stringProperty(String name, String defaultValue) {
		return getProperties().getProperty(name, defaultValue);
	}
}
