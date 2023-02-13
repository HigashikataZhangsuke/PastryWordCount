package yinzhe.test.entry;

import java.net.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import rice.p2p.scribe.ScribeImpl;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.Topic;
import rice.pastry.*;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import testcsv.testofread;
import rice.pastry.dist.DistPastryNodeFactory;
import yinzhe.test.countalgorithm.*;
import java.io.*;

/**
 * 
 * @author zhangsuke
 * This is the Test Class for this miniproject. It contains 3 functions, including main() as the entry; bootnode for node generation & boot and the makescribe for scribe
 * 
 */

public class Testentry {

	 
	  public Testentry(int bindport, InetSocketAddress bootaddress,
	      int numNodes, Environment env) throws Exception {  
	  }
	  
	  //The function for generate and boot nodes.  
	    public void Bootnodes(int bindport, InetSocketAddress bootaddress,NodeIdFactory nidFactory,PastryNodeFactory factory,Vector nodevec) throws IOException, InterruptedException {
	      PastryNode node = factory.newNode();
		  node.boot(bootaddress);
	      // the node may require sending several messages to fully boot into the ring
	      synchronized(node) {
	        while(!node.isReady() && !node.joinFailed()) {
	          // delay so we don't busy-wait
	          node.wait(1000);
	          // abort if can't join
	          if (node.joinFailed()) {
	            throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason()); 
	          }
	        }       
	      }
	      //use this Vec to store nodes for further scribe clients generation
	      nodevec.add(node);
	      System.out.println("Finished creating a new node and Join the Ring: " + node);
	    }
	    
	    //The function for client generation
	    public WordCountApplication Createapplications(PastryNode node,Environment env,int tpnum) throws InterruptedException {
	    	WordCountApplication collectclient = new WordCountApplication(node,tpnum);
	    	env.getTimeSource().sleep(3000);
	    	System.out.println("Finished generate one scribe client for " + node.getId().toString());
	    	return collectclient;
	    }
	    
	public int getnum(int i) {
		return i;
	}
	
	public HashSet<Integer> getsetforread(int max){
		Random rand = new Random();
		HashSet<Integer> hashset = new HashSet<Integer>();
		while (hashset.size()<max) {
			hashset.add(rand.nextInt(max));
		}
		return hashset;
	}
	

	public static void main(String[] args) throws Exception {
		
	    // Loads pastry configurations & Set a logger for whole system.
		
	    Environment env = new Environment();
	    
	    // disable the UPnP setting (in case you are testing this on a NATted LAN or single computer)
	    env.getParameters().setString("nat_search_policy", "never");
	    
	    try {
	    	
	      // the port to use locally
	      int bindport = Integer.parseInt(args[0]);

	      // build the bootaddress from the command line args
	      InetAddress bootaddr = InetAddress.getByName(args[1]);
	      int bootport = Integer.parseInt(args[2]);
	      InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);
	      //int sleepdelay =900000;
	      //int sleepdelay =450000;
	      int sleepdelay =1800000;
	      // the port to use locally
	      int numNodes = Integer.parseInt(args[3]);
	      int windowsize = Integer.parseInt(args[4]);
	      int rate=Integer.parseInt(args[5]);
	      int expsize= Integer.parseInt(args[6]);
	      int topicnum = Integer.parseInt(args[7]);
	      // Instance of test
	      Testentry test = new Testentry(bindport, bootaddress, numNodes,
	          env);
	      
	      //Generate basic parts for node generation
	      NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
	      PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env);
	      
	      //Store Vector initialized here
	      Vector nodevec = new Vector();
	      
	      //First loop, used for generate nodes
	      for (int i=0;i<numNodes;i++) {
	    	  test.Bootnodes(bindport, bootaddress, nidFactory, factory,nodevec);
	    	  env.getTimeSource().sleep(1000);
	    	  
	      }

	      env.getTimeSource().sleep(sleepdelay);

	      
	      //Now loop for subscribe clients
	      Vector cltvec = new Vector();
	      for (int i=0;i<numNodes;i++) {
	    	  WordCountApplication clt = test.Createapplications((PastryNode)nodevec.elementAt(i), env,topicnum);
	    	  env.getTimeSource().sleep(1000);
	    	//env.getTimeSource().sleep(3000);
	    	  cltvec.add(clt);
	      }

	      env.getTimeSource().sleep(sleepdelay);
	      //Scribe all topics for all nodes in this VM
	      for (int j=0;j<topicnum;j++) {
	      for (int i=0;i<numNodes;i++) {

	    	  ((WordCountApplication) cltvec.elementAt(i)).mkscribe(((WordCountApplication) cltvec.elementAt(i)).tpvec.elementAt(j));
	    	  env.getTimeSource().sleep(1000);
	    	  }
	    	  env.getTimeSource().sleep(30000);
	    	 
	    	  System.out.println("Finished scribe one scribe client");
	      }

	     env.getTimeSource().sleep(sleepdelay);

	      Set<Integer> nums = test.getsetforread(numNodes);
	       
	      for (int i=0;i<numNodes;i++) {
		    	//Set parameters and read data for Wordcount
		    	 ((WordCountApplication) cltvec.elementAt(i)).windowsize = windowsize;
		    	 ((WordCountApplication) cltvec.elementAt(i)).readrate = rate;
		    	 ((WordCountApplication) cltvec.elementAt(i)).leafnodenum = expsize;
		    	 ((WordCountApplication) cltvec.elementAt(i)).setreadnum(nums.toArray(new Integer[nums.size()])[i]);
		    	 ((WordCountApplication) cltvec.elementAt(i)).readdata(); 
		    	 env.getTimeSource().sleep(5000);
		    	 //Get childlist for next step
		    	 for (int j=0;j<topicnum;j++) {
		    			 ((WordCountApplication) cltvec.elementAt(i)).getchildlist((((WordCountApplication) cltvec.elementAt(i)).tpvec.elementAt(j)));
		    			 env.getTimeSource().sleep(1000);
		   	     } 
		         env.getTimeSource().sleep(30000);
		    	 System.out.println("Finished Read one node's Data");
		   }
	     

	      env.getTimeSource().sleep(sleepdelay);
	      System.out.println("Construction Finished, Now Processing Data");
	      for (int i=0;i<numNodes;i++) {
	    	  //Check here. If you are the root for some topic, then just start publish aggregation message.
	    	  if (((WordCountApplication) cltvec.elementAt(i)).checkRoot()==true) {
	    		  ((WordCountApplication) cltvec.elementAt(i)).startPublishTask();
	    	  }
	      }
	      
	    } catch (Exception e) {
	      // remind user how to use
	      System.out.println("Usage:");
	      System.out
	          .println("java [-cp FreePastry-<version>.jar] rice.tutorial.scribe.ScribeTutorial localbindport bootIP bootPort numNodes");
	      System.out
	          .println("example java rice.tutorial.scribe.ScribeTutorial 9001 pokey.cs.almamater.edu 9001 10");
	      throw e;
	    }
	  }	
	}
