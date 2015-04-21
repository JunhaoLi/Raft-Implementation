package edu.duke.raft;

import java.util.Timer;

public class CandidateMode extends RaftMode {
	
    //generate a randomize timeout in the range 
    private int ELECTION_TIMEOUT;
    private Timer mTimer;
	
    public void go () {
    synchronized (mLock) {
      int term = mConfig.getCurrentTerm();
      System.out.println ("S" + 
			  mID + 
			  "." + 
			  term + 
			  ": switched to candidate mode.");
      ELECTION_TIMEOUT = (int)(((double)ELECTION_TIMEOUT_MAX-(double)ELECTION_TIMEOUT_MIN)*Math.random())+ELECTION_TIMEOUT_MIN; 
      //already sent remote request
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
	System.out.println("server "+mID+" in candiate "+candidateID+ "request vote");
    	int term = mConfig.getCurrentTerm();
    	if (candidateTerm > term)  //quit my election, update my term
    	{
    		mConfig.setCurrentTerm(candidateTerm, 0);  //vote for higher term
		System.out.println("server "+mID+" quit election in term "+term);
    		RaftMode mode = new FollowerMode();
    		RaftServerImpl.setMode(mode);
    	}
	mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
    	return term;
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
	System.out.println("server "+mID+"in candiate appendEntries");
	int term = mConfig.getCurrentTerm ();
	int result = term;
	//receive higher heartbeat, back to follower mode
	if (leaderTerm>=term)
	    {
		System.out.println("server "+mID+" in term "+term+" back to follower mode");
		RaftMode mode = new FollowerMode();
		RaftServerImpl.setMode(mode);
	    }
      /*//client send request to me, say no or append?
      if (leaderID == mID)
      {
    	  //how???
      }*/
	mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
	return result;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
    	mTimer.cancel();
    	System.out.println("server "+mID+"in candiate handletimeout");
	
		
	//if candidate has voted, it quit election
	if (mConfig.getVotedFor() != 0){
	    System.out.println("server "+mID+" quit to follower");
	    RaftMode mode = new FollowerMode();
	    RaftServerImpl.setMode(mode);
	}
    	//check vote first
        int count = 0;
        int term = mConfig.getCurrentTerm();
        int num = mConfig.getNumServers();
        int [] votes = RaftResponses.getVotes(term);
        for (int i = 1; i<=num;i++)
        {
      	 if (votes[i]>=term)  //have higher term, back to follower
      	 {
      		RaftMode mode = new FollowerMode();
      		RaftServerImpl.setMode(mode);
      	 }
      	 count += (votes[i] == 0?1:0);
        }
        System.out.println("server "+mID+" get "+count+" vote");
        //get majority, now send heartbeat
        if(count>=num/2+1) {
        	for (int i = 1; i<=num;i++)
	        {
			    if (mID == i)
			    {
			    	continue;
			    }
			    remoteAppendEntries(i, term, mID, 0, 0, null, 0);
	        }
        }
        RaftMode mode = (count>=num/2+1?new LeaderMode():new CandidateMode());
        RaftServerImpl.setMode(mode);
    }
  }
}
