package org.gkolpu.hadoop.mvn.plugin;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Vector;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.gkolpu.hadoop.mvn.plugin.jar.JarFileHelper;
import org.gkolpu.hadoop.mvn.plugin.ssh.SSHCommandExecutor;
import org.gkolpu.util.ftp.SFTPClientUtil;

import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

@Mojo(name = "execute")
public class ExecuteMapReduceMojo extends AbstractMojo {

	@Parameter(property = "jarFolder", required = false)
	private String jarFolder;
	@Parameter(property = "host", required = true)
	private String host;
	@Parameter(property = "login", required = true)
	private String login;
	@Parameter(property = "password", required = true)
	private String password;
	@Parameter(property = "hdfsOutputDir", required = true)
	private String hdfsOutputDir;
	@Parameter(property = "hdfsInputDir", required = true)
	private String hdfsInputDir;

	@Parameter(property = "workingDirectory", defaultValue = "MapReduce-TESTS")
	private String workingDirectory;

	@Parameter(property = "className", required = true)
	private String className;

	@Parameter(property = "jarName", defaultValue = "MapReduce.jar")
	private String jarName;

	// TODO
	@Parameter(property = "outputDir", required = true)
	private String outputDir;

	@Component
	private MavenProject project;

	public void execute() throws MojoExecutionException, MojoFailureException {

		// TODO
		// parameters validation
		
		
		 

		getLog().info("Starting MapReduce execution...");
		String jarFileLocation = JarFileHelper.getJarLocation(project);
		getLog().info("Copying JarFile : " + jarFileLocation);

		SFTPClientUtil sftp = new SFTPClientUtil();
		try {
			sftp.connect(host, login, password, 22);
//			 SSHCommandExecutor.execute(host, login, password, "mkdir  "
//			 + workingDirectory);
			String workingPath = sftp.getWorkingDirectory() + "/"
					+ workingDirectory;

			Boolean fileUploaded = sftp.uploadFile(jarFileLocation,
					workingPath + "/" + jarName);

			if (fileUploaded) {
				getLog().info("jar file copied!");
			} else {
				// TODO
				// throw new FTPException();
			}

			String command = "hadoop jar " + workingPath + "/" + jarName
					+ " " + className + " " + hdfsInputDir + " "
					+ hdfsOutputDir;

			getLog().info("Submitting MapReduce Job...(" + command + ")");

			SSHCommandExecutor.execute(host, login, password, command);
			getLog().info("MapReduce job Done!");

			getLog().info(
					"get output from hdfs..." + "(hadoop fs -copyToLocal "
							+ hdfsOutputDir +" "+ workingPath + "/)");

			SSHCommandExecutor.execute(host, login, password,
					"hadoop fs -copyToLocal " + hdfsOutputDir+" "
							+ workingPath + "/");

			// TODO
			// add param to config
			String outputTargetDir = project.getBasedir()
					+ "\\target\\MapReduceOutput";

			String localOutputDir = outputDir+getDirPostfix();
			boolean success = (new File(outputTargetDir)).mkdir();
			if (success) {
				getLog().info("Directory: " + outputTargetDir + " created");
			}

			success = (new File(outputTargetDir + "\\" + localOutputDir)).mkdir();
			if (success) {
				getLog().info(
						"Directory: " + outputTargetDir + "\\" + localOutputDir
								+ " created");
			}

			Vector<LsEntry> ls = sftp.getCommand().ls(
					workingPath + "/" + outputDir);

			for (LsEntry object : ls) {
				String file = object.getFilename();
				getLog().info(file);
				if (file.contains("part")) {
					String remotePath = workingPath + "/" + outputDir
							+ "/" + file;

					String localPath = outputTargetDir + "\\" + localOutputDir
							+ "\\" + file;
					getLog().info(
							"Copying from remote path (" + remotePath + ")");

					Boolean fileDownloaded = sftp.downloadFile(remotePath,
							localPath);
					if (fileDownloaded) {
						getLog().info(
								file + " File copied to local :" + localPath);

					}
				}
			}

			getLog().info("cleaning...");
			 SSHCommandExecutor.execute(host, login, password, "rm -r "
			 + workingDirectory+"/*");
			SSHCommandExecutor.execute(host, login, password, "hadoop fs -rmr "
					+ hdfsOutputDir);

		} catch (IOException e) {

			e.printStackTrace();
		} catch (JSchException e) {
			getLog().info("copying file failed");
			e.printStackTrace();
		} catch (SftpException e) {
			e.printStackTrace();
		} finally {
			sftp.disconnect();

		}

	}

	private String getDirPostfix() {
		Calendar cal = Calendar.getInstance();
		return "-"+cal.getTime().getHours()+"-"+cal.getTime().getMinutes();
//		return cal.gett
	}

	public String getJarFolder() {
		return jarFolder;
	}

	public void setJarFolder(String jarFolder) {
		this.jarFolder = jarFolder;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHdfsOutputDir() {
		return hdfsOutputDir;
	}

	public void setHdfsOutputDir(String hdfsOutputDir) {
		this.hdfsOutputDir = hdfsOutputDir;
	}

	public String getHdfsInputDir() {
		return hdfsInputDir;
	}

	public void setHdfsInputDir(String hdfsInputDir) {
		this.hdfsInputDir = hdfsInputDir;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getJarName() {
		return jarName;
	}

	public void setJarName(String jarName) {
		this.jarName = jarName;
	}

	public String getWorkingDirectory() {
		return workingDirectory;
	}

	public void setWorkingDirectory(String workingDirectory) {
		this.workingDirectory = workingDirectory;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

}
