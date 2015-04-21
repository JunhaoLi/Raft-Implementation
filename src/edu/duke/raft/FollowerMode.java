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
		//set a time to handle timeout
		ELECTION_TIMEOUT =  (int)(((double)ELECTION_TIMEOUT_MAX-(double)ELECTION_TIMEOUT_MIN)*Math.random())+ELECTION_TIMEOUT_MIN; 
                //ELECTION_TIMEOUT =  ELECTION_TIMEOUT_MAX;
		//clear status
		//System.out.println ("Timeout time" +ELECTION_TIMEOUT+" test"); 
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
	//System.out.println("S"+mID+" in requestVote candidateTerm: "+candidateTerm+" ID:"+candidateID);
	  if (candidateTerm<=term ||voteFor!=0 )  
	      {
		  //mConfig.setCurrentTerm(candidateTerm, 0); //set current term and voted for
		  //System.out.println("server "+mID+" follower, request vote, "+voteFor);
		  //System.out.println("server "+mID+" follower, request vote, case one");
		  System.out.println("server "+mID+" in term "+term+" does not vote to server "+candidateID);
		  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
		  return vote;
	      } 
	  //candidateTerm>term
	  //already voted in current term
	  //System.out.println("candidateTerm>term");
	  else if (lastLogTerm>lastTerm || (lastLogTerm == lastTerm && lastLogIndex>=lastIndex))
	    {
		//say yes, update local term
		System.out.println("server "+mID+" in term "+term+" vote to server "+candidateID);
		mConfig.setCurrentTerm(candidateTerm, candidateID); //DO NOT UPDATE term, but set vote
		//System.out.println("server "+mID+" set vote "+mConfig.getVotedFor());
		vote = 0;
	    }
	  else {
	      //candidate has lower logTerm
	      System.out.println("server "+mID+"does not vote to server "+candidateID);
	  }
	  //mConfig.setCurrentTerm(candidateTerm, ); //set current term and voted for	
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
	  System.out.println("server "+mID+" in follower appendEntries");
      
	  int term = mConfig.getCurrentTerm ();
	  int result = term;
	  
	  if (term>leaderTerm)  //request from stale leader
	      {
		  //tell him to quit
		  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID); 
		  return term;
	      }
	  mConfig.setCurrentTerm(Math.max(term, leaderTerm), 0);
	  if (entries == null)  //is heartbeat, no append, just update term and lastApplied
	      {
		  mLastApplied = Math.max(mLastApplied, mCommitIndex);
		  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
		  return term;
	      }
	  else
	      {
		  //client send request to me, should forward to leader
		  if (leaderID == mID)
		      {
			  //try each server
			  int num = mConfig.getNumServers();
			  for (int i = 1; i<=num;i++)
			      {
				  if (mID != i)
				  {
				      remoteAppendEntries(0,i,0, 0, 0, entries, 0);  //forward to leader
				  }
			      }
		  }
		  else  //true append
		      {
		      mConfig.setCurrentTerm(Math.max(term, prevLogTerm), 0);
		      if (prevLogIndex == -1)  //should append from start
			  {
			      mLog.insert(entries, -1, prevLogTerm);
			      result =0;
			      if (leaderCommit>mCommitIndex)  //only effecive when we append something
				  {
				      mCommitIndex = Math.min(leaderCommit, mLog.getLastIndex());
				      mLastApplied = Math.max(mLastApplied, mCommitIndex);
				  }
			  }
		      else
			  {
			      Entry testEntry = mLog.getEntry(prevLogIndex);
			      if (testEntry != null && testEntry.term == prevLogTerm) //same index, same term
				  {
				      //append, say yes and setCurrentTerm
				      mLog.insert(entries, prevLogIndex, prevLogTerm);
				      result = 0;
				      if (leaderCommit>mCommitIndex)
					  {
					      mCommitIndex = Math.min(leaderCommit, mLog.getLastIndex());
					      mLastApplied = Math.max(mLastApplied, mCommitIndex);
					  }
				  }  
			  }
		      }
	  }  
	  mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID); 
	  return result; 
      }
  }  
    
    // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
	mTimer.cancel();
        System.out.println("server "+mID+" in follower handletimeout and send remote request");
	int term = mConfig.getCurrentTerm()+1;
	mConfig.setCurrentTerm(term, mID); //prepare to vote ifself
	RaftResponses.setTerm(term);  //guarantee all later action can be accepted by RaftResponses
	RaftResponses.clearAppendResponses(term);
	RaftResponses.clearVotes(term);
	//System.out.println(mID+" before mTimer");
	
	//send request to each follower before switch to candidate mode
	int num = mConfig.getNumServers();
	for (int i = 1;i<=num;i++)
	    {
		if (i == mID)
		    {
			RaftResponses.setVote(mID, 0, term);  //vote for itself
			continue;
		    }
		remoteRequestVote (i, term,mID,mLog.getLastIndex(),mLog.getLastTerm());
	    }
	
    	RaftMode mode = new CandidateMode();
    	RaftServerImpl.setMode (mode);
    }
  }
}
