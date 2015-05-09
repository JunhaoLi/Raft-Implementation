package edu.duke.raft;

import java.rmi.Naming;
import java.util.Arrays;
import java.util.Timer;

public class LeaderMode extends RaftMode {
	
    private Timer heartbeatTimer; //heartbeat and appendrequest
    private int[] nextIndex;
    private int[] matchIndex;
    private int[] storeIndex;
    private int [] firstSend;
    //private int[] prevLast;      //store previous send index to each server
    private Timer selfappend;

    public void go () 
    {
      synchronized (mLock) 
      {
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
      
        //check heartbeat response immediately
      int[] currentResponse = RaftResponses.getAppendResponses(term);
      for (int i = 1; i<=num;i++)
      {
    	   if (currentResponse[i]>term)// change mode to follower
    	   {
          RaftMode mode =new FollowerMode();
          RaftServerImpl.setMode(mode);
          return;
        }
      }
      
      //reset follower status
      nextIndex = new int[num+1];
      matchIndex = new int[num+1];
      firstSend = new int[num+1];
      storeIndex = new int[num+1];///////////////////////////////////
      int lastIndex = mLog.getLastIndex();
      for (int i =1; i<=num;i++)
      {
       nextIndex[i] = lastIndex;//////////////////////////////////
       matchIndex[i] = -1;///////////////////////////////////////
       //prevLast[i] = 0;
       storeIndex[i]=lastIndex;///////////////////////////////////
          firstSend[i] = 1;
      }
      matchIndex[mID] = lastIndex;
      //start heartbeat period
      //selfappend= scheduleTimer(1000,mID+400);
      heartbeatTimer =  scheduleTimer (HEARTBEAT_INTERVAL, mID);
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
   int lastLogTerm) 
  {
    synchronized (mLock) 
    {
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
       heartbeatTimer.cancel();
       RaftMode  mode = new FollowerMode();
       RaftServerImpl.setMode(mode);
       return vote;
      } 
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
  int leaderCommit) 
  {
    synchronized (mLock) 
    {
      int term = mConfig.getCurrentTerm();

      if (leaderID == mID)  //client(follower) requests
      {
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
          heartbeatTimer.cancel();
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
  public void handleTimeout (int timerID) 
  {
    synchronized (mLock) 
    {
      if(timerID==mID+400)
      {   //once be leader, append entries to followers(the action is its own ID);
        selfappend.cancel();
        Entry[]content= new Entry[1];
        content[0]=new Entry(0,0);
        content[0].action=mID;
        content[0].term=mConfig.getCurrentTerm();
        mLog.append(content);
        selfappend= scheduleTimer(1000,mID+400);
      }
      else if(timerID==mID)
      {
        heartbeatTimer.cancel();
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
          else if(matchIndex[i]==currentLast)//identical log, send heartbeat 
          {
              if (firstSend[i] == 1)
              {
                  //send last entry
                  Entry[] currentEntry = new Entry[1];
                  currentEntry[0] = mLog.getEntry(mLog.getLastIndex());
                  int prevIndex = mLog.getLastIndex();
                  int prevTerm = prevIndex == -1?0: mLog.getEntry(prevIndex).term;
                  storeIndex[i] = mLog.getLastIndex();
                  remoteAppendEntries (i,term,mID,prevIndex,prevTerm,currentEntry,mCommitIndex);
                  //check append respond (may be delay)
                  int iResponse = RaftResponses.getAppendResponses(term)[i];
                  if (iResponse == -1)  //append entry fails because of server is down
                  {
                      continue;
                  }
                  else if (iResponse == 0)  //success
                  {
                      RaftResponses.setAppendResponse(i,-1,mConfig.getCurrentTerm());///////////
                      //matchIndex[i] = currentLast;
                      //nextIndex[i] = currentLast+1;
                      matchIndex[i] = storeIndex[i];
                      nextIndex[i] = mLog.getLastIndex();
                      storeIndex[i]=nextIndex[i];
                  }
                  else  //append entry fails because of log inconsistency
                  {
                      RaftResponses.setAppendResponse(i, -1, term);
                      if(nextIndex[i]>0){
                          nextIndex[i]--;
                      }
                  }
                  firstSend[i] = 0;
              }
              else
              {
                remoteAppendEntries(i, term, mID, 0, 0, null, 0);
              }

          }
          else 
          {    
		        //append entries
            int start = nextIndex[i]+1;
            int end = storeIndex[i];
            Entry[] currentEntry = new Entry[end-start+1];
            for (int j= 0; j<currentEntry.length;j++)
            {
              currentEntry[j] = mLog.getEntry(start+j);
            }
            int prevIndex = nextIndex[i];
            int prevTerm = prevIndex == -1?0: mLog.getEntry(prevIndex).term;
            storeIndex[i]=end;
            remoteAppendEntries (i,term,mID,prevIndex,prevTerm,currentEntry,mCommitIndex);

      		  //check append respond (may be delay)
            int iResponse = RaftResponses.getAppendResponses(term)[i];
      		  if (iResponse == -1)  //append entry fails because of server is down
      		  {
              continue;
            }
      		  else if (iResponse == 0)  //success
      		  {
                  RaftResponses.setAppendResponse(i,-1,mConfig.getCurrentTerm());///////////
              //matchIndex[i] = currentLast;
              //nextIndex[i] = currentLast+1;
              matchIndex[i] = storeIndex[i];
              nextIndex[i] = mLog.getLastIndex();
                  storeIndex[i]=nextIndex[i];
            }
      		  else  //append entry fails because of log inconsistency
      		  {
              RaftResponses.setAppendResponse(i, -1, term);
                  if(nextIndex[i]>0)
              nextIndex[i]--;
            }	 
          }
        }//end for

        int[] tempMatch = new int[num];//now update commitIndex, median of match index
        for (int i = 0; i<tempMatch.length;i++)
        {
          tempMatch[i] = matchIndex[i+1];
        }
        Arrays.sort(tempMatch);
        mCommitIndex = tempMatch[num/2];  //careful for index
        heartbeatTimer =  scheduleTimer (HEARTBEAT_INTERVAL, mID);
      }
    }//end sync
  }//end handle
  
}//end class
