package Dynamoth.Util.Properties;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

import Dynamoth.Util.ResourceFinder;


public class PropertyManager {

	private static HashMap<String, Properties> propertyFiles = new HashMap<String, Properties>();

	public static Properties getProperties(String file) {
		return getProperties(file, false);
	}

	public static Properties getProperties(String file, boolean disableDbProperties) {

		Properties props;

		props = PropertyManager.propertyFiles.get(file);

		if (props == null) {
			InputStream is = null;

			props = new Properties();

			try {
				
				// First try to load the properties file from disk - current directory
				
				URL propFileUrl = ResourceFinder.find(file);
				try {
		            is = propFileUrl.openStream();
		        } catch (IOException e) {
		            // Do nothing
		        }
							
				System.out.println("Loading Mammoth properties file from: " + file);

				if(is == null){
					throw new PropertyFileException("Property file was not found." + file);
				}

				props.load(is);
				
				// We cannot close is in a finally clause,
				// because of the exceptions thrown in
				// the catch clauses.
				is.close();
			} catch (FileNotFoundException e) {
				if (is != null) try {
					is.close();
				} catch (IOException e1) {
					e.printStackTrace();
				}
				
				throw new PropertyFileException("Property file was not found." + file);
			} catch (IOException e) {
				if (is != null) try {
					is.close();
				} catch (IOException e1) {
					e.printStackTrace();
				}
				
				throw new PropertyFileException("Unable to open property file. " + file);
			}

			PropertyManager.propertyFiles.put(file, props);
		}

		if(file.equals("mammoth.properties") && !disableDbProperties) {
			boolean useDb = Boolean.parseBoolean(
					props.getProperty("properties.db.enable").trim());

			if (useDb) {
				/*Properties dbProps = new DbProperties(props);
				PropertyManager.propertyFiles.put(file, dbProps);
				return dbProps;*/
			}
		}

		return props;
	}	
}
