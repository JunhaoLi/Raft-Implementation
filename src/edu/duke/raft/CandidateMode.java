package edu.duke.raft;

import java.util.Timer;

public class CandidateMode extends RaftMode {
	
	//generate a randomize timeout in the range 
	private int 	ELECTION_TIMEOUT;
	private Timer mTimer;
	
    public void go () {
    synchronized (mLock) {
      //int term = 0;   
      int term = mConfig.getCurrentTerm()+1;
      System.out.println ("S" + 
			  mID + 
			  "." + 
			  term + 
			  ": switched to candidate mode.");
      ELECTION_TIMEOUT = (int)(((double)ELECTION_TIMEOUT_MAX-(double)ELECTION_TIMEOUT_MIN)*Math.random())+ELECTION_TIMEOUT_MIN; 
      //each server has a RaftResponses class
      mConfig.setCurrentTerm(term, mID); //prepare to vote ifself
      RaftResponses.setTerm(term);  //guarantee all later action can be accepted by RaftResponses
      RaftResponses.clearAppendResponses(term);
      RaftResponses.clearVotes(term);
      //!!part 2应该改成commitIndex和对应的term
      mTimer = this.scheduleTimer(ELECTION_TIMEOUT,mID);
      //send request to each follower
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
      //check vote
      int count = 0;
      int [] votes = RaftResponses.getVotes(term);
      for (int i = 0; i<votes.length;i++)
      {
    	 if (votes[i]>=term)  //have higher term, back to follower
    	 {
    		 RaftMode mode = new FollowerMode();
    		 mode.go();
    	 }
    	 count += (votes[i] == 0?1:0);
      }
      if (count>=num/2+1)  //get majority
      {
    	 RaftMode mode = new LeaderMode();
 		 mode.go();
      }
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
    	int term = mConfig.getCurrentTerm();
    	if (candidateTerm>=term)  //quit my election, update my term
    	{
    		mConfig.setCurrentTerm(candidateTerm, candidateID);  //vote for higher term
    		term = 0;//say yes
    	}
    	//default say no
    	// 理论上先成为的leader的server会在这个server的这一次选举完成前发出heartbeat
    	//就算这个server也成为了leader，也会收到更高的term而变回follower
    	return term;
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
      int term = mConfig.getCurrentTerm ();
      int result = term;
      //receive higher heartbeat, back to follower mode
      if (entries == null && leaderTerm>=term)
      {
    	  RaftMode mode = new FollowerMode();
    	  mode.go();
      }
      //client send request to me, say no or append?
      if (leaderID == mID)
      {
    	  //how???
      }
      return result;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
    	//start new election
    	this.go();
    }
  }
}
