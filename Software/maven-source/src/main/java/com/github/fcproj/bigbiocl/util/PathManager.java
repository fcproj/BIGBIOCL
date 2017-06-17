package com.github.fcproj.bigbiocl.util;

/**
 * Manage path prefixes (file: and hdfs:)
 * @author fabrizio
 *
 */
public class PathManager {
	
	//singleton
	private static PathManager instance;
	public static PathManager getInstance(){
		if(instance==null)
			instance = new PathManager();
		return instance;
	}
	
	/**
	 * Add the defaultPrefic to the path, if there is no prefix
	 * @param path the path to check
	 * @param defaultPrefix the default prefix, without colon and slashes
	 * @return the original path plus, eventually, the defaultPrefix
	 */
	public String checkPathWithDefault(String path, String defaultPrefix){
		if(defaultPrefix!=null && path!=null){
			if(!path.startsWith("hdfs:") && !path.startsWith("file:"))
				path = defaultPrefix + "://" + path;
		}
		return path;
	}

}
