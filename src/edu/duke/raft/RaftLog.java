package edu.duke.raft;

import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.LinkedList;

public class RaftLog {
  private LinkedList<Entry> mEntries;
  private Path mLogPath;
  
  public RaftLog (String file) {  //将file中的内容读入变成linkedlist
    mEntries = new LinkedList<Entry> ();
    try {
      mLogPath = FileSystems.getDefault ().getPath (file);
      String delims = " ";
      List<String> lines = Files.readAllLines (mLogPath, 
					       StandardCharsets.US_ASCII);
      Entry e = null;
      
      for (String line : lines) {  //log以空格分开, term_action
	String[] tokens = line.split (delims);
	if ((tokens != null) && (tokens.length > 0)) {
	  e = new Entry (Integer.parseInt (tokens[1]),  //action
			 Integer.parseInt (tokens[0]));  //term
	  mEntries.add (e);  //加入linkedlist
	} else {
	  System.out.println ("Error parsing log line: " + line);
	}	
      }      
    } catch (IOException e) {
      System.out.println (e.getMessage ());
      e.printStackTrace();
    }    
  }

  // @param entries to append (in order of 0 to append.length-1)
  // @param index of log entry before entries to append
  // @param term of log entry before entries to append
  // @return highest index in log after entries have been appended, or
  // 0 if the append failed.
  public int append (Entry[] entries) {
    try {
      if (entries != null) {
	OutputStream out = Files.newOutputStream (mLogPath, 
						  StandardOpenOption.CREATE,
						  StandardOpenOption.APPEND,
						  StandardOpenOption.SYNC);
	for (Entry entry : entries) {
	  if (entry != null) {
	    out.write (entry.toString ().getBytes ());
	    out.write ('\n');
	    mEntries.add (entry);
	  } else {
//	    System.out.println ("Tried to append null entry to RaftLog.");
	    break;
	  }	
	}
	out.close ();
      }
    } catch (IOException e) {
      System.out.println (e.getMessage ());
      e.printStackTrace();
    } 
    return (mEntries.size () - 1);
  }

  // @param entries to append (in order of 0 to append.length-1)
  // @param index of log entry before entries to append
  // @param term of log entry before entries to append
  // @return highest index in log after entries have been appended, if
  // the entry at prevIndex is not from prevTerm or if the log does
  // not have an entry at prevIndex, the append request will fail, and
  // the method will return -1.
  public int insert (Entry[] entries, int prevIndex, int prevTerm) {
    try {
      // Just append to the existing log if we aren't inserting in the
      // middle
      if (prevIndex == (mEntries.size () - 1)) {
	return append (entries);
      } else if (entries == null) {
	// can only append null to the end of the log
	return -1;
      } else if ((prevIndex == -1) ||
		 ((mEntries.get (prevIndex) != null) &&
		  (mEntries.get (prevIndex).term == prevTerm))) {  //上一个term要一样才能insert
	// Because we are inserting in the middle of our log, we
	// will update our log by creating a temporary on-disk log
	// with the new entries and then replacing the old on-disk
	// log with the temporary one.

	// First, create an in-memory copy of the existing log up to
	// the point where the new entries will be added
	LinkedList<Entry> tmpEntries = new LinkedList<Entry> ();  //拷贝之前的
	for (int i=0; i<=prevIndex; i++) {
	  Entry entry = mEntries.get (i);
	  tmpEntries.add (entry);
	}
	  
	// Next, add the new entries to temporary in-memory and
	// on-disk logs
	Path tmpLogPath = 
	  FileSystems.getDefault ().
	  getPath (mLogPath.toAbsolutePath ().toString () + ".tmp");  // 写到另一个文件
	  
	OutputStream out = 
	  Files.newOutputStream (tmpLogPath, 
				 StandardOpenOption.CREATE,
				 StandardOpenOption.TRUNCATE_EXISTING,
				 StandardOpenOption.SYNC);

	// Write out the prefix
	for (Entry entry : tmpEntries) {
	  out.write (entry.toString ().getBytes ());
	  out.write ('\n');
	}

	// Add the new entries
	for (Entry entry : entries) {  //插入entry
	  if (entry != null) {
	    out.write (entry.toString ().getBytes ());
	    out.write ('\n');
	    tmpEntries.add (entry);
	  }
	}
	out.close ();

	// switch the in-memory and on-disk logs to the new versions
	Files.move (tmpLogPath,   //替换原有的log文件
		    mLogPath, 
		    StandardCopyOption.REPLACE_EXISTING,
		    StandardCopyOption.ATOMIC_MOVE);
	mEntries = tmpEntries;
      } else {
	System.out.println (
	  "RaftLog: " +
	  "index and term mismatch, could not insert new log entries.");
	return -1;
      }	
    } catch (IOException e) {
      System.out.println (e.getMessage ());
      e.printStackTrace();
    }
     
    return mEntries.size ();
  }

  // @return index of last entry in log
  public int getLastIndex () {
    return (mEntries.size () - 1);
  }

  // @return term of last entry in log
  public int getLastTerm () {
    Entry entry = mEntries.getLast ();
    if (entry != null) {
      return entry.term;
    }
    return -1;
  }

  // @return entry at passed-in index, null if none
  public Entry getEntry (int index) {  
    if ((index > -1) && (index < mEntries.size())) {
      return new Entry (mEntries.get (index));  //返回index上的副本
    }
    
    return null;
  }

  public String toString () {
    String toReturn = "{";
    for (Entry e: mEntries) {
      toReturn += " (" + e + ") ";   //e.toString()
    }
    toReturn += "}";
    return toReturn;
  }  

  private void init () {
  }

  public static void main (String[] args) {  //for test
    if (args.length != 1) {
      System.out.println("usage: java edu.duke.raft.RaftLog <filename>");
      System.exit(1);
    }
    String filename = args[0];
    RaftLog log = new RaftLog (filename);
    System.out.println ("Initial RaftLog: " + log);

    Entry[] entries = new Entry[1];
    entries[0] = new Entry (0, 0);
    System.out.println("Appending new entry " + entries[0] + ".");
    log.append (entries);
    System.out.println ("Resulting RaftLog: " + log);

    Entry firstEntry = log.getEntry (0);
    Entry newEntry = new Entry (1, 3);

    System.out.println("Inserting entry " + newEntry + " at index 1.");
    entries[0] = newEntry;
    log.insert (entries, 0, firstEntry.term);
    System.out.println ("Resulting RaftLog: " + log);

    newEntry.term = 5;
    newEntry.action = 5;    
    System.out.println("Inserting entry " + newEntry + " at index 0.");
    entries[0] = newEntry;
    log.insert (entries, -1, -1);
    System.out.println ("Resulting RaftLog: " + log);
  }

}
