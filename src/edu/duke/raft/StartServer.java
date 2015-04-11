package edu.duke.raft;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class StartServer {
  public static void main (String[] args) {
    if (args.length != 4) {
      System.out.println ("usage: java edu.duke.raft.StartServer -Djava.rmi.server.codebase=<codebase url> <int: rmiregistry port> <int: server id> <log file dir> <config file dir>");
      System.exit(1);
    }
    int port = Integer.parseInt (args[0]);    //得到port和id
    int id = Integer.parseInt (args[1]);
    String logPath = args[2] + "/" + id + ".log";    //定义本地文件路径
    String configPath = args[3] + "/" + id + ".config";
    
    String url = "rmi://localhost:" + port + "/S" + id;  //定义url
//    System.out.println ("Starting S" + id);
//    System.out.println ("Binding server on rmiregistry " + url);

    RaftConfig config = new RaftConfig (configPath);  //建立实例
    RaftLog log = new RaftLog (logPath);
    int lastApplied = log.getLastIndex ();  //得到最新index
    RaftResponses.init (config.getNumServers (), log.getLastTerm ());  //开始response（static）

    try {
      RaftMode.initializeServer (config,
				 log,
				 lastApplied, 
				 port, 
				 id);
      RaftServerImpl server = new RaftServerImpl (id);  //server实例
      RaftServerImpl.setMode (new FollowerMode ());  //初始化mode
      
      Naming.rebind(url, server);
    } catch (MalformedURLException me) {
      System.out.println ("S" + id + ": " + me.getMessage());
      me.printStackTrace ();
    } catch (RemoteException re) {
      System.out.println ("S" + id +  ": " + re.getMessage());
      re.printStackTrace ();
    } catch (Exception e) {
      System.out.println ("S" + id +  ": " +  e.getMessage());
      e.printStackTrace ();
    }
  }  
}

  
