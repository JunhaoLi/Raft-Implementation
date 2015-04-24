package edu.duke.raft;

import java.rmi.Naming;
import java.util.Arrays;
import java.util.Timer;

public class LeaderMode extends RaftMode {
	
	private Timer heartTimer; //heartbeat and appendrequest
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
    	  remoteAppendEntries(i, term, mID, 0, 0, null, 0);
      }
      //check immediately
      int[] currentResponse = RaftResponses.getAppendResponses(term);
      for (int i = 1; i<=num;i++)
      {
    	  if (currentResponse[i]>term)
    	  {
		  System.out.println("Leader become follower because higher term");
    		  RaftMode mode =new FollowerMode();
    		  RaftServerImpl.setMode(mode);
    	  }
      }
      //reset follower status
      nextIndex = new int[num+1];
      matchIndex = new int[num+1];
      int lastIndex = mLog.getLastIndex();
      for (int i =1; i<=num;i++)
      {
    	  nextIndex[i] = lastIndex+1;
    	  matchIndex[i] = 0;
      }
      matchIndex[mID] = lastIndex;
      //start heartbeat period
      heartTimer =  scheduleTimer (HEARTBEAT_INTERVAL, mID);
    }
  }

  // @param candidate’s term
  // @param candidate requesting vote
  // @param index of candidate’s last log entry
  // @param term of candidate’s last log entry
  // @return 0, if server votes for candidate; otherwise, server's
  // current term
  public int requestVote (int candidateTerm,
			  int candidateID,
			  int lastLogIndex,
			  int lastLogTerm) {
    synchronized (mLock) {
    	//System.out.println("server "+mID+"in leader requestVote");
    	int term = mConfig.getCurrentTerm ();
    	int vote = term;
    	//if leader recover from failure, it is in follower mode, should not happen but we consider it
    	if (candidateTerm<=term)
    	{
    		//receive request from stale candidate, return higher term
    		return term;
    	}
    	else  // I am stale leader, go back to follower
    	{
    		heartTimer.cancel();
    		RaftMode  mode = new FollowerMode();
    		RaftServerImpl.setMode(mode);
    	}
    	return vote; 
    	}
  	}
  

  // @param leader’s term
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
    	//System.out.println("server "+mID+"in leader appendEntries");
    	int term = mConfig.getCurrentTerm();
    	
    	if (leaderID == mID)  //client(follower) requests
    	{
    		//System.out.println("hahahaha"+entries[0].action);
    		//set all entry term to leader's term
    		for(Entry entry:entries) 
    		{
    			entry.term=term;
    		}
    		mLog.append(entries);
    		matchIndex[mID] = mLog.getLastIndex();
    	}
    	else //from leader(stale or new)
	    {
    		if (leaderTerm>=term) // i am stale
		    {
    			heartTimer.cancel();
    			RaftMode mode = new FollowerMode();
    			RaftServerImpl.setMode(mode);  //next time i will append contents as a follower
    			return term;
		    }
    		else //"leader" is stale
		    {
    			return term;
		    }
	    }
    	return term;
    }
  }

  // @param id of the timer that timed out
    public void handleTimeout (int timerID) {
    synchronized (mLock) {
	//System.out.println("server "+mID+"in leader handtimeout");
	heartTimer.cancel();
	int term = mConfig.getCurrentTerm();
	int num = mConfig.getNumServers();
	int currentLast = mLog.getLastIndex();  //may not same between last append
	
	//check and send new append request to each follower
	for (int i = 1; i<=num;i++)
	{
		if (mID == i)
		{
			continue;
		}
		else if(matchIndex[i]==currentLast) 
		{
		    //System.out.println("in handtimeout, seeding heart beat to server "+i);
		    remoteAppendEntries(i, term, mID, 0, 0, null, 0);
		}
		else 
		{    
		    //true append
		    int start = nextIndex[i];
		    int end = currentLast;
		    Entry[] currentEntry = new Entry[end-start+1];
		    //System.out.println("send length: "+(end-start+1));
		    for (int j= 0; j<currentEntry.length;j++)
			{
			    currentEntry[j] = mLog.getEntry(start+j);
			}
		    int prevIndex = nextIndex[i]-1;
		    int prevTerm = prevIndex == -1?0: mLog.getEntry(prevIndex).term;
		    //System.out.println("prevIndex:  "+prevIndex+" prevTerm: "+prevTerm);
		    remoteAppendEntries (i,term,mID,prevIndex,prevTerm,currentEntry,mCommitIndex);
		    //check
		    int iResponse = RaftResponses.getAppendResponses(term)[i];
		    if (iResponse == -1)  //fail
			{
			    //System.out.println("response -1");
			    break;
			}
		    else if (iResponse == 0)  //success
			{
			    //System.out.println("response 0");
			    matchIndex[i] = currentLast;
			    nextIndex[i] = currentLast+1;
			}
		    else  //error
			{
			    //System.out.println(mID+"response: "+iResponse);
			    nextIndex[i]--;
			}	 
		}
	}//end for
	
	//now update commitIndex, median of match index
	int[] tempMatch = new int[num];
	for (int i = 0; i<tempMatch.length;i++)
	{
		tempMatch[i] = matchIndex[i+1];
    }
	Arrays.sort(tempMatch);
    mCommitIndex = tempMatch[num/2];  //careful for index
    heartTimer =  scheduleTimer (HEARTBEAT_INTERVAL, mID);
    }//end sync
  }//end handle
}

