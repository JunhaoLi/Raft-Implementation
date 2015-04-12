package edu.duke.raft;

import java.rmi.Naming;
import java.util.Timer;

public class LeaderMode extends RaftMode {
	
	private Timer heartTimer,mTimer; //heartbeat and appendrequest
	
	public void go () {
    synchronized (mLock) {
      //int term = 0;
      int term = mConfig.getCurrentTerm(); //already +1 in candidate mode
      System.out.println ("S" + 
			  mID + 
			  "." + 
			  term + 
			  ": switched to leader mode.");
      RaftResponses.setTerm(term);  //should be modified in candidate
      RaftResponses.clearVotes(term);
      RaftResponses.clearAppendResponses(term);
      heartTimer =  scheduleTimer (HEARTBEAT_INTERVAL, mID);  //start send heartbeat
    }
  }

  // @param candidate鈥檚 term
  // @param candidate requesting vote
  // @param index of candidate鈥檚 last log entry
  // @param term of candidate鈥檚 last log entry
  // @return 0, if server votes for candidate; otherwise, server's
  // current term
  public int requestVote (int candidateTerm,
			  int candidateID,
			  int lastLogIndex,
			  int lastLogTerm) {
    synchronized (mLock) {
      int term = mConfig.getCurrentTerm ();
      int vote = term;
      //if leader recover from failure, it is in follower mode
      //weird to receive such request in leader mode
      System.out.println("Leader get requeste vote");
      return vote; //always say no
    }
  }
  

  // @param leader鈥檚 term
  // @param current leader
  // @param index of log entry before entries to append
  // @param term of log entry before entries to append
  // @param entries to append (in order of 0 to append.length-1)
  // @param index of highest committed entry
  // @return 0, if server appended entries; otherwise, server's
  // current term
  public int appendEntries (int leaderTerm,
			    int leaderID,
			    int prevLogIndex,
			    int prevLogTerm,
			    Entry[] entries,
			    int leaderCommit) {
    synchronized (mLock) {
    	//set append round time
      mTimer = scheduleTimer (HEARTBEAT_INTERVAL, mID+1);
      int num = mConfig.getNumServers();
      int prevIndex = mLog.getLastIndex()-1;
      int lastTerm = mLog.getLastTerm();
      int currTerm = mConfig.getCurrentTerm();
      int prevTerm = mLog.getEntry(prevIndex).term;
      //如何维持与不同follower的entries，index记录?
      for (int i = 1; i<=num;i++)
      {
    	 this.remoteAppendEntries(i,currTerm, mID, prevIndex, prevTerm, entries, mCommitIndex);
      }
      int term = mConfig.getCurrentTerm ();
      int result = term;
      return result;  //meaningless for leader response
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
    	int num = mConfig.getNumServers();  //number of servers
    	if (timerID == mID)  //heartbeat time out
    	{
        	for (int i = 0; i<=num;i++)
        	{
        		if (num != mID)
        		{
        			this.appendEntries(mConfig.getCurrentTerm(), mID, 0, 0, null, 0);  //send heartbeat to every follower
        		}
        	}
        	heartTimer = scheduleTimer (HEARTBEAT_INTERVAL, mID);
    	}
    	else if (timerID == mID+1)  //append time out
    	{
    		mTimer.cancel();
    		//check response
    		int[] tResponses = RaftResponses.getAppendResponses(mConfig.getCurrentTerm());
    		foreach (int r  in tResponses)
    		{
    			if (r == -1) //follower did not response(fail?)
    			{
    				continue;
    			}
    			else if (r == 0) //successful
    			{
    				continue;
    			}
    			else  //reject append
    			{
    	    		//如何send不同的entries，previndex给follower
    			}
    		}
    		mTimer = scheduleTimer (HEARTBEAT_INTERVAL, mID+1);
    	}
    	else
    	{
    		System.out.println("Unknown time out");
    	}
    }
  }
}
