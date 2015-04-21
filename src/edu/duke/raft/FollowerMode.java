package edu.duke.raft;

import java.util.Timer;


public class FollowerMode extends RaftMode {
	
	private Timer mTimer;
	private int ELECTION_TIMEOUT;
	
	public void go () {
	    synchronized (mLock) {
		int term = mConfig.getCurrentTerm()+1;
		System.out.println ("S" + 
				    mID + 
				    "." + 
				    term + 
				    ": switched to follower mode.");
		//calculate ramdomized timeout
		ELECTION_TIMEOUT =  (int)(((double)ELECTION_TIMEOUT_MAX-(double)ELECTION_TIMEOUT_MIN)*Math.random())+ELECTION_TIMEOUT_MIN; 
		//initial status
		mConfig.setCurrentTerm(term, 0);
		RaftResponses.setTerm(term);
		RaftResponses.clearVotes(term);
		RaftResponses.clearAppendResponses(term);
		mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
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
    	  mTimer.cancel();
    	  System.out.println("server "+mID+" in follower requestVote");
    	  int term = mConfig.getCurrentTerm ();
    	  int vote = term;
    	  int voteFor = mConfig.getVotedFor();
    	  int lastIndex = mLog.getLastIndex();
    	  int lastTerm = mLog.getLastTerm();
    	  
    	  
    	  /**********************vote policy*****************************/
    	  //lower term or already voted
    	  if (candidateTerm<=term ||voteFor!=0 )  
	      {
    		  System.out.println("server "+mID+" in term "+term+" does not vote to server "+candidateID);
    		  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
    		  return vote;
	      } 

    	  //candidateTerm>term
    	  //higher term or, higher index with same term, say yes, update my term(possible)
    	  if (lastLogTerm>lastTerm || (lastLogTerm == lastTerm && lastLogIndex>=lastIndex))
    	  {
    		  System.out.println("server "+mID+" in term "+term+" vote to server "+candidateID);
    		  mConfig.setCurrentTerm(candidateTerm, candidateID); 
    		  vote = 0;
    	  }
    	  else //lower term or same term with lower index
    	  {
    		  System.out.println("server "+mID+"does not vote to server "+candidateID);
    	  }
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
      	  mTimer.cancel();
      	  System.out.println("server "+mID+" in follower appendEntries");
      	  int term = mConfig.getCurrentTerm ();
      	  int result = term;
      	  
      	  //request from stale leader, say no
      	  if (term>leaderTerm)  
	      {
      		  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID); 
      		  return result;  //leader will check return value and turn to follower
	      }
      	  
      	  //is heartbeat, just update status if possible
      	  mConfig.setCurrentTerm(Math.max(term, leaderTerm), 0);
      	  if (entries == null)
	      { 
      		  mLastApplied = Math.max(mLastApplied, mCommitIndex);
      		  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
      		  return result;
	      }
      	  else  //true append
	      {
      		  /* what if client sent to me?  --store in local, remote append to leader when i know who it is
      		  if (leaderID == mID)
      		  {
			  } */
      		  
		      if (prevLogIndex == -1)  // append from start
			  {
			      mLog.insert(entries, -1, prevLogTerm);
			      result =0;
			      if (leaderCommit>mCommitIndex)
				  {
				      mCommitIndex = Math.min(leaderCommit, mLog.getLastIndex());
				      mLastApplied = Math.max(mLastApplied, mCommitIndex);
				  }
			  }
		      else  //possible append from somewhere
			  {
			      Entry testEntry = mLog.getEntry(prevLogIndex);
			      if (testEntry != null && testEntry.term == prevLogTerm) //same index, same term, should append
				  {
				      mLog.insert(entries, prevLogIndex, prevLogTerm);
				      result = 0;
				      if (leaderCommit>mCommitIndex)
					  {
					      mCommitIndex = Math.min(leaderCommit, mLog.getLastIndex());
					      mLastApplied = Math.max(mLastApplied, mCommitIndex);
					  }
				  }
			      else  //wrong entry, does not append
			      {
		      		  mLastApplied = Math.max(mLastApplied, mCommitIndex);
			      }
			  }
	  }
      //append start/somewhere/wrong entry
	  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID); 
	  return result; 
      }
  }  
    
    // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
    	mTimer.cancel();
        System.out.println("server "+mID+" in follower handletimeout and send remote request");
        //ready to switch to candidate
    	RaftMode mode = new CandidateMode();
    	RaftServerImpl.setMode (mode);
  }
}
