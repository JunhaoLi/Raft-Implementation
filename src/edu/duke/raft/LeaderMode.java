package edu.duke.raft;

import java.rmi.Naming;
import java.util.Arrays;
import java.util.Timer;

public class LeaderMode extends RaftMode {
	
	private Timer heartTimer; //heartbeat and appendrequest
	private Timer appendTimer; //check and send append request to follower periodically 
	private int[] nextIndex;
	private int[] matchIndex;
	
	public void go () {
    synchronized (mLock) {
      //int term = 0;
      int term = mConfig.getCurrentTerm(); //already +1 in candidate mode
      System.out.println ("S" + 
			  mID + 
			  "." + 
			  term + 
			  ": switched to leader mode.");
      //clear vote and append history
      RaftResponses.setTerm(term); 
      RaftResponses.clearVotes(term);
      RaftResponses.clearAppendResponses(term);
      //send heartbeat to each follower
      int num = mConfig.getNumServers();
      for (int i = 1; i<=num;i++)
      {
    	  if (mID == i)
    	  {
    		  continue;
    	  }
    	  remoteAppendEntries(i, 0, mID, 0, 0, null, 0);
      }
      //reset array
      nextIndex = new int[num+1];
      matchIndex = new int[num+1];
      int lastIndex = mLog.getLastIndex();
      for (int i =1; i<=num;i++)
      {
    	  nextIndex[i] = lastIndex+1;
    	  matchIndex[i] = 0;
      }
      //start heartbeat period
      heartTimer =  scheduleTimer (HEARTBEAT_INTERVAL, mID);
      appendTimer =scheduleTimer (HEARTBEAT_INTERVAL, mID+1); //will not affect other servers
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
      appendTimer.cancel();
      int term = mConfig.getCurrentTerm ();
      int vote = term;
      //if leader recover from failure, it is in follower mode
      if (candidateTerm<=term)
      {
    	  //receive request from stale candidate
    	  //tell him to quit
    	  this.remoteAppendEntries(candidateID, term, mID, 0, 0, null, 0);
      }
      else  // I am stale leader, go back to follower
      {
    	  //就算本次不投票导致那个leader得不到多数超时
    	  //总会再次开始election，而最终过时的leader都会以follower参与投票
    	  heartTimer.cancel();
    	  RaftMode  mode = new FollowerMode();
    	  RaftServerImpl.setMode(mode);
      }
      appendTimer =scheduleTimer (HEARTBEAT_INTERVAL, mID+1); 
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
    	appendTimer.cancel();
    	int term = mConfig.getCurrentTerm();
    	
    	if (leaderID == mID)  //client requests
    	{
    		mLog.append(entries);
    		//heartbeat will check new lastIndex later
    	}
    	else //from leader(stale or new)
    	{
    		if (leaderTerm>=term) // i am stale
    		{
    			heartTimer.cancel();
    			RaftMode mode = new FollowerMode();
    			RaftServerImpl.setMode(mode);  //next time i will append contents as a follower
    		}
    		else //"leader" is stale
    		{
    			//tell him to quit
    			this.remoteAppendEntries(leaderID, 0, mID, 0, 0,null,0);
    		}
    	}
    	appendTimer =scheduleTimer (HEARTBEAT_INTERVAL, mID+1); 
    	return term;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
    	if (timerID == mID)  //heartbeat time out
    	{
    		int num = mConfig.getNumServers();
    	      for (int i = 1; i<=num;i++)
    	      {
    	    	  if (mID == i)
    	    	  {
    	    		  continue;
    	    	  }
    	    	  remoteAppendEntries(i, 0, mID, 0, 0, null, 0);
    	      }
    	      //start heartbeat period
    	      heartTimer =  scheduleTimer (HEARTBEAT_INTERVAL, mID);
    	}
    	else if (timerID == mID+1) //append time out, check and send
    	{
    		int term = mConfig.getCurrentTerm();
    		int num = mConfig.getNumServers();
    		int currentLast = mLog.getLastIndex();  //may not same between last append

    		//check and send new append request to each follower
    		for (int i = 1; i<=num;i++)
    		{
    			while (currentLast>=nextIndex[i]) //need to send
    			{
    				Entry[] sendEntry = new Entry[currentLast-nextIndex[i]+1];
    				for (int j = nextIndex[i];j<=currentLast;j++)
    				{
    					sendEntry[j] = mLog.getEntry(j);
    				}
    				//send
    				int prevIndex = nextIndex[i]-1;
    				int prevTerm = mLog.getEntry(prevIndex).term;
    				this.remoteAppendEntries(i, term, mID,prevIndex,prevTerm, sendEntry, mCommitIndex);
    				//check response
    				int currentRespons = RaftResponses.getAppendResponses(term)[i];
    				if (currentRespons == -1)  //follower fail, wait until next appent timeout
    				{
    					break;
    				}
    				else if (currentRespons >0)  //inconsistency
    				{
    					nextIndex[i]--;  //until -1
    				}
    				else  //success, update
    				{
    					nextIndex[i] = currentLast+1;
    					matchIndex[i] = currentLast;
    				}
    			}	
    		}
    		//update commitIndex, median of match index
    		int[] tempMatch = new int[num];
    		for (int i = 0; i<tempMatch.length;i++)
    		{
    			tempMatch[i] = matchIndex[i+1];
    		}
    		Arrays.sort(tempMatch);
    		mCommitIndex = tempMatch[num/2];  //careful for index
    		//reset append timer
            appendTimer =scheduleTimer (HEARTBEAT_INTERVAL, mID+1);
    	}//end else	
    }//end sync
  }//end handle
}
 