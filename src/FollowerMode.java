package edu.duke.raft;

import java.util.Timer;


public class FollowerMode extends RaftMode {
	private Timer mTimer;
	
	public void go () {
    synchronized (mLock) {
      //int term = 0;
      int term = mConfig.getCurrentTerm();
      System.out.println ("S" + 
			  mID + 
			  "." + 
			  term + 
			  ": switched to follower mode.");
      //set a time to handle timeout
      mTimer = this.scheduleTimer(HEARTBEAT_INTERVAL,mID);
    }
      //got heartbeat or appendrequests
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
			  int lastLogTerm) {   //知道candidate的信息，对比自己并且set response
    synchronized (mLock) {
      
      int term = mConfig.getCurrentTerm ();
      int vote = term;
      if (candidateTerm<=term || mConfig.getVotedFor() != 0)  //already voted
      {
    	  return vote;
      }
      else if (lastLogTerm>term || (lastLogTerm == term && lastLogIndex>=mLog.getLastIndex()))
      {
        //say yes
    	vote = 0;
    	mConfig.setCurrentTerm(candidateTerm, candidateID); //set current term and voted for
    	return 0;
      }
      //default say no 
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
    
    	//cancel local timer
    	mTimer.cancel();

      int term = mConfig.getCurrentTerm ();
      int result = term;
      if (entries == null)  //is heartbeat, no action
      {
    	  mTimer = this.scheduleTimer(HEARTBEAT_INTERVAL,mID);
    	  return term;
      }
      
      if (prevLogIndex == -1)  //should append from start
      {
    	  mLog.insert(entries, -1, prevLogTerm);
      }
      else
      {
    	  Entry testEntry = mLog.getEntry(prevLogIndex);
          if (testEntry != null && prevLogTerm == testEntry.term)
          {
        	  //append, say yes and setCurrentTerm
        	  mLog.insert(entries, prevLogIndex, prevLogTerm);
        	  result = 0;
        	  mConfig.setCurrentTerm(leaderTerm, leaderID);
          }
      }
      //set a new timer
      mTimer = this.scheduleTimer(HEARTBEAT_INTERVAL,mID);
      return result;
    }
  }  

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
		Thread.sleep((long)Math.random()*1000);  //prepare to become a candidate
    	RaftMode mode = new CandidateMode();
    	RaftServerImpl.setMode (mode);
    }
  }
}