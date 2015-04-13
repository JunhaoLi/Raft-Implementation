package edu.duke.raft;

import java.util.Timer;


public class FollowerMode extends RaftMode {
	private Timer mTimer;
	private int 	ELECTION_TIMEOUT;
	
	public void go () {
    synchronized (mLock) {
      int term = 0;
      System.out.println ("S" + 
			  mID + 
			  "." + 
			  term + 
			  ": switched to follower mode.");
      //set a time to handle timeout
     ELECTION_TIMEOUT =  (int)(((double)ELECTION_TIMEOUT_MAX-(double)ELECTION_TIMEOUT_MIN)*Math.random())+ELECTION_TIMEOUT_MIN; 
     mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
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
			  int lastLogTerm) {
    synchronized (mLock) {
      mTimer.cancel();
      int term = mConfig.getCurrentTerm ();
      int vote = term;
      
      if (candidateTerm<=term)  
      {
    	  return vote;
      } 
      //candidateTerm>term
      if (lastLogTerm>term || (lastLogTerm == term && lastLogIndex>=mLog.getLastIndex()))
      {
        //say yes, update local term
    	vote = 0;
    	mConfig.setCurrentTerm(candidateTerm, candidateID); //set current term and voted for
    	//possible update
    	 mLastApplied = Math.max(mLastApplied, mCommitIndex);
      }
      //default say no 
      mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID); 
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

      if (entries == null)  //is heartbeat, no append, just update term and lastApplied
      {
    	  mConfig.setCurrentTerm(Math.max(term, prevLogTerm), 0);
    	  mLastApplied = Math.max(mLastApplied, mCommitIndex);
    	  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
    	  return term;
      }
      //client send request to me, should forward to leader
      if (leaderID == mID)
      {
    	  //how???
      }
      //true append
      if (prevLogIndex == -1)  //should append from start
      {
    	  mLog.insert(entries, -1, prevLogTerm);
    	  result =0;
    	  if (leaderCommit>mCommitIndex)  //only effecive when we append something
          {
        	  mCommitIndex = Math.min(leaderCommit, mLog.getLastIndex());
          }
      }
      else
      {
    	  Entry testEntry = mLog.getEntry(prevLogIndex);
          if (testEntry != null &&testEntry.term == prevLogTerm)
          {
        	  //append, say yes and setCurrentTerm
        	  mLog.insert(entries, prevLogIndex, prevLogTerm);
        	  result = 0;
        	  if (leaderCommit>mCommitIndex)
              {
            	  mCommitIndex = Math.min(leaderCommit, mLog.getLastIndex());
              }
          }  
      }
     mConfig.setCurrentTerm(Math.max(term, prevLogTerm), 0);
     mLastApplied = Math.max(mLastApplied, mCommitIndex);
      //set a new timer
      mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID); 
      return result;
    }
  }  

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
    	RaftMode mode = new CandidateMode();
    	RaftServerImpl.setMode (mode);
    }
  }
}