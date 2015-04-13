package edu.duke.raft;

import java.util.Timer;

public class CandidateMode extends RaftMode {
	
	//generate a randomize timeout in the range 
	private final static int 	ELECTION_ROUND =(int)(((double)ELECTION_TIMEOUT_MAX-(double)ELECTION_TIMEOUT_MIN)*Math.random())+ELECTION_TIMEOUT_MIN; 
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
      mConfig.setCurrentTerm(term, mID);
      RaftResponses.setTerm(term);
      RaftResponses.clearAppendResponses(term);
      RaftResponses.clearVotes(term);
      //part 2应该改成commitIndex和对应的term
      this.requestVote(term, mID, mLog.getLastIndex(), mLog.getLastTerm());
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
      int num = mConfig.getNumServers();
      int lastIndex = mLog.getLastIndex();
      int lastTerm = mLog.getLastTerm();
      for (int i = 1; i<=num;i++)
      {
    	  if (i == mID)
    	  {
    		  continue;
    	  }
    	  this.remoteRequestVote(i, term, mID, lastIndex, lastTerm);
      }
      //start timer
      mTimer = this.scheduleTimer(ELECTION_ROUND, mID);
      int result = term;
      return result;
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
      //do not consider the case when client send requestion to candidate
      //always say no
      return result;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
    	mTimer.cancel();
    	//check response
    	int[] vResponses = RaftResponses.getVotes(mConfig.getCurrentTerm());
    	int yes = 0;
    	for(int i = 0; i<vResponses.length;i++)
    	{
    		yes += (vResponses[i]==0?1:0);
    	}
    	int majority = (mConfig.getNumServers()+1)/2;
    	if (yes>=majority)
    	{
    		//switch to leader
    	 	RaftMode mode = new LeaderMode();
        	mode.go();
    	}
    	else
    	{
        	//switch back to follower mode
        	int term = mConfig.getCurrentTerm()-1;
        	mConfig.setCurrentTerm(term, 0);  //0 means no vote history
        	RaftResponses.clearAppendResponses(term);
        	RaftResponses.clearVotes(term);
        	RaftMode mode = new FollowerMode();
        	mode.go();
    	}
    }
  }
}
