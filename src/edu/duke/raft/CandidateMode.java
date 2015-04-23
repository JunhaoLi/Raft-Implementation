package edu.duke.raft;

import java.util.Timer;

public class CandidateMode extends RaftMode {

    private int ELECTION_TIMEOUT;
    private Timer mTimer;
    private Timer cTimer;  //counting timer
	
    public void go () {
    synchronized (mLock) {
    	int term = mConfig.getCurrentTerm()+1;
    	mConfig.setCurrentTerm(term, mID);
    	System.out.println ("S" + 
			  mID + 
			  "." + 
			  term + 
			  ": switched to candidate mode.");

		RaftResponses.setTerm(term);
		RaftResponses.clearVotes(term);
		RaftResponses.clearAppendResponses(term);
		//vote process
        int num = mConfig.getNumServers();
		 for (int i = 1;i<=num;i++)
	        {
	        	if (i == mID)
	        	{
	        		RaftResponses.setVote(mID, 0, term);  //vote for itself
	        		continue;
	        	}
	        	remoteRequestVote (i, term,mID,mLog.getLastIndex(),mLog.getLastTerm());  // in current ready queue
	        }
		 
    	ELECTION_TIMEOUT = (int)(((double)ELECTION_TIMEOUT_MAX-(double)ELECTION_TIMEOUT_MIN)*Math.random())+ELECTION_TIMEOUT_MIN;
    	//ELECTION_TIMEOUT = 10;
    	mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
    	cTimer = this.scheduleTimer(10, mID+1);
    }
    
    //otherwise, until timeout and election again
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
    	cTimer.cancel();
    	
    	System.out.println("server "+mID+" in candiate"+candidateID+ "request vote");
    	int term = mConfig.getCurrentTerm();
    	int result = term;
    	if (candidateTerm>term)  //higher term,  quit my election, give higher candidate time
    	{
            RaftMode mode =new FollowerMode();
            RaftServerImpl.setMode(mode);
            return result;  
    	}
    	mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
    	cTimer = this.scheduleTimer(10,mID+1);
    	return result;  //say no if in lower/same term
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
	cTimer.cancel();
	
	System.out.println("server "+mID+"in candiate appendEntries");
	int term = mConfig.getCurrentTerm ();
	int result = term;
	
	//receive higher heartbeat, back to follower mode
	if (leaderTerm>=term)
	{
		System.out.println("server "+mID+" in candidate term "+term+" back to follower mode");
		RaftMode mode = new FollowerMode();
		RaftServerImpl.setMode(mode);
		return result;
	}
      /*client send request to me, say no or append?
      if (leaderID == mID)
      {
    	  //how???
      }*/
	
	//from stale leader
	mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
	cTimer = this.scheduleTimer(10, mID+1);
	return result;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
    	
    	if (timerID == mID+1) //cTimer timeout
    	{
    		cTimer.cancel();
    		//check vote again, should be completed in go
            int count = 0;
            int term = mConfig.getCurrentTerm();
            int num = mConfig.getNumServers();
            int [] votes = RaftResponses.getVotes(term);
            for (int i = 1; i<=num;i++)
            {
          	 if (votes[i]>term)  //have higher term, back to follower
          	 {
          		 mTimer.cancel();
          		RaftMode mode = new FollowerMode();
          		RaftServerImpl.setMode(mode);
          		return;
          	 }
          	 count += (votes[i] == 0?1:0);
            }
            //System.out.println("server "+mID+" get "+count+" vote in timeout");
            if (count>=num/2+1)
            {
            	mTimer.cancel();
            	RaftMode mode = new LeaderMode();
                RaftServerImpl.setMode(mode);
                return;
            }
            else
            {
            	cTimer = this.scheduleTimer(10, mID+1);
            }
    	}
    	else //mTimer
    	{
    		mTimer.cancel();
    		RaftMode mode = new CandidateMode();
            RaftServerImpl.setMode(mode);
            return;
    	}	
    }
  }
}
