package Dynamoth.Core.Util;

import java.lang.reflect.Field;
import java.util.Properties;

import Dynamoth.Client.Client;
import Dynamoth.Util.Properties.PropertyManager;

public class SigarUtil {

	private SigarUtil() {}

	// Mega hack to setup sigar natives correctly using the ONEJAR launcher and using Eclipse
	public static void setupSigarNatives() {
		
		// Get (extra) native paths
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);		
		String nativePathsString  = props.getProperty("resourcefinder.native_paths", "");
		String[] nativePaths = nativePathsString.split(";");
		
		// Get the property
		String property = System.getProperty("java.library.path");
		String[] platforms = new String[] {"linux32", "linux64", "macosx", "win32", "win64"};
		for (String platform : platforms) {	
			// Add extra native paths
			if (nativePathsString.equals("") == false) {
				for (String nativePath : nativePaths) {
					property = property + ":" + nativePath + platform;	
				}
			}
			
			// Add local native path
			property = property + ":" + "lib/native/" + platform;			
		}
		
		System.setProperty( "java.library.path", property );
		 
		// And now comes the hack: blank out the current cached path
		// http://blog.cedarsoft.com/2010/11/setting-java-library-path-programmatically/
		Field fieldSysPath;
		try {
			fieldSysPath = ClassLoader.class.getDeclaredField( "sys_paths" );
			fieldSysPath.setAccessible( true );
			fieldSysPath.set( null, null );
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Hacked natived libraries (Sigar), current value is: " + System.getProperty("java.library.path"));
	}

}
