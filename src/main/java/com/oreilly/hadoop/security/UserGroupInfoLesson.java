package com.oreilly.hadoop.security;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import java.io.PrintWriter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.security.PrivilegedAction;

public class UserGroupInfoLesson extends Configured implements Tool {
	
  public void dumpBasicInfo() throws Exception {
      Configuration config = this.getConf();
  	  UserGroupInformation.setConfiguration(config); // initialize UGI
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      System.out.println("hadoop.security.authentication (authentication method set on cluster):" + config.get("hadoop.security.authentication"));
      System.out.println("Short user name: " + ugi.getShortUserName());
      System.out.println("Has Kerberos credentials: " + ugi.hasKerberosCredentials());
  }
  
  public void dumpConfig() throws Exception {
      Configuration config = this.getConf();
      Configuration.dumpConfiguration(config, new PrintWriter(System.out));
  }
	
  public void loginFromKeyTab(String user, String path) throws Exception {
	  Configuration config = this.getConf();
	  UserGroupInformation.setConfiguration(config);
      UserGroupInformation.loginUserFromKeytab(user, path);
      dumpBasicInfo();
      /* After a successful login, you can do HDFS or Hadoop operations */ 
  }
 
  public void proxyUser(String user) throws Exception {
	  final Configuration config = this.getConf();
	  UserGroupInformation.setConfiguration(config);
	  UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
	  UserGroupInformation proxy = UserGroupInformation.createProxyUser(user,  ugi);
	  System.out.println("Proxy user: " + proxy.getUserName());
	  System.out.println("Real user: " + proxy.getRealUser());
	  dumpBasicInfo();
	  proxy.doAs(new PrivilegedAction<Void> () {
		  public Void run() throws RuntimeException {
			  try {
			  FileSystem fs = FileSystem.get(config);
			  fs.mkdirs(new Path("/tmp/proxy-user-test"));
			  return null;
			  } catch (Exception e) {
				  throw new RuntimeException(e);
			  }
		  }
	  });
	  FileSystem fs = FileSystem.get(config);
	  fs.mkdirs(new Path("/tmp/proxy-user-test"));
  }
 
  public int run(String[] args) throws Exception {
      if (args[0].equals("--dumpConfig")) {
    	  dumpConfig();
      }
      if (args[0].equals("--loginFromKeytab")) {
    	      System.out.println("logging in " + args[1] + "args[2] " + args[2]);
    		  loginFromKeyTab(args[1], args[2]);
      }
      if (args[0].equals("--basicInfo")) {
    	  dumpBasicInfo();
      }      
      if (args[0].equals("--proxyUser")) {
    	  proxyUser(args[1]);
      }
    return 0;
  }
  
  public static void main(String [] args) throws Exception {
	  System.setProperty("sun.security.krb5.debug", "true");
	  ToolRunner.run(new UserGroupInfoLesson (), args);
  }
  
}
