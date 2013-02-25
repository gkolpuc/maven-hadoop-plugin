package org.gkolpu.hadoop.mvn.plugin.jar;

import org.apache.maven.project.MavenProject;

public class JarFileHelper {

	
	public static String getJarLocation(MavenProject project){
		
		StringBuilder builder = new StringBuilder();
		builder.append(project.getBasedir());
		builder.append("\\target\\");
		builder.append(project.getName());
		builder.append("-");
		builder.append(project.getVersion());
		builder.append(".jar");
		
		return builder.toString();
	}
}
