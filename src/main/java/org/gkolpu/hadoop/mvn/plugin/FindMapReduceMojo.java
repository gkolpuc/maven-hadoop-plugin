package org.gkolpu.hadoop.mvn.plugin;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugin.MojoFailureException;

@Mojo(name = "find")
public class FindMapReduceMojo extends AbstractMojo {

	public void execute() throws MojoExecutionException, MojoFailureException {
		
		getLog().info("Hello, world.");
		getLog().info("Executing hadoop plugin GOAL");

	}

}
