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

	  //Constructor. Do nothing here.
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
	    public WordCountApplication Createapplications(PastryNode node,Environment env) throws InterruptedException {
	    	WordCountApplication collectclient = new WordCountApplication(node);
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
		//PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream("/home/johnny/YinzheWordCountTest.txt")),true);
		//System.setOut(ps);
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
	      int sleepdelay =900000;
	      // the port to use locally
	      int numNodes = Integer.parseInt(args[3]);
	      int windowsize = Integer.parseInt(args[4]);
	      int rate=Integer.parseInt(args[5]);
	      int expsize= Integer.parseInt(args[6]);
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
	    	  //env.getTimeSource().sleep(3000);
	      }
	      
	    //  System.out.println("Successfully Generated "+ numNodes+" nodes");
	     // System.out.println("=======================================================");
	      //Wait enough time if use multiple terminals
	      //env.getTimeSource().sleep(20000);
	      env.getTimeSource().sleep(sleepdelay);
	      //Generate our topic 
	      PastryNode node0 = (PastryNode) nodevec.elementAt(0);
	      Topic tp = new Topic(new PastryIdFactory(node0.getEnvironment()), "SimpleAggr");
	      
	      //Now loop for subscribe clients
	      Vector cltvec = new Vector();
	      for (int i=0;i<numNodes;i++) {
	    	  WordCountApplication clt = test.Createapplications((PastryNode)nodevec.elementAt(i), env);
	    	  env.getTimeSource().sleep(1000);
	    	//env.getTimeSource().sleep(3000);
	    	  cltvec.add(clt);
	      }
	     // System.out.println("Successfully Generated "+ numNodes+" clients");
	     // System.out.println("=======================================================");
	      //Wait enough time if use multiple terminals
	      //env.getTimeSource().sleep(30000);
	      env.getTimeSource().sleep(sleepdelay);
	      //Scribe to SimpleAggr
	      for (int i=0;i<numNodes;i++) {
	    	  ((WordCountApplication) cltvec.elementAt(i)).scribe(tp);
	    	  ((WordCountApplication) cltvec.elementAt(i)).settopic(tp);
	    	  env.getTimeSource().sleep(1000);
	    	   System.out.println("Finished scribe one scribe client");
	      }
	    //  
	      //System.out.println("Successfully scribed "+ numNodes+" nodes");
	      //env.getTimeSource().sleep(30000);
	     env.getTimeSource().sleep(sleepdelay);
	     //Determine if the tree is good
	     /**
	     for (int i=0;i<numNodes;i++) {
	    	  ((WordCountApplication) cltvec.elementAt(i)).getchildlist();;
	    	 // ((WordCountApplication) cltvec.elementAt(i)).settopic(tp);
	    	 // env.getTimeSource().sleep(1000);
	    	 if (((WordCountApplication) cltvec.elementAt(i)).getParent()!=null || ((WordCountApplication) cltvec.elementAt(i)).isRoot()==true ) {
	    		 System.out.println("Scribe for this client is good");
	    	 } 
	      }
	     //Now find out when the system is ready.
	       env.getTimeSource().sleep(sleepdelay);
	     int cnt =10;
	     while (cnt>0) {
	     for (int i=0;i<numNodes;i++) {
	    	  //((WordCountApplication) cltvec.elementAt(i)).scribe(tp);
	    	 // ((WordCountApplication) cltvec.elementAt(i)).settopic(tp);
	    	 // env.getTimeSource().sleep(1000);
	    	 if (((WordCountApplication) cltvec.elementAt(i)).isRoot()==true ) {
	    		 //System.out.println("Scribe for this client is good");
	    		 ((WordCountApplication) cltvec.elementAt(i)).check();
	    	 } 
	    	 // System.out.println("Finished scribe one scribe client");
	      }
	     env.getTimeSource().sleep(10000);
	     cnt-=1;
	     }
	     //tp.manager;
	     //printAllParentsDataStructure
	     **/
	     
	     
	     // System.out.println("=======================================================");
	      //Wait enough time if use multiple terminals
	      //env.getTimeSource().sleep(5000);
	      //System.out.println("The Tree Structure is");
	     // NodeHandle root = printTree(cltvec);
	     // System.out.println("=======================================================");
	      //Find Root and then start Test
	     // int Rootatwhere = 0;
	     // for (int i=0;i<numNodes;i++) {
	    	  //If my node handle is equal to the root
	    	//  if (((WordCountApplication) cltvec.elementAt(i)).endpoint.getId().toStringFull().equals(root.getId().toStringFull())==true) {
	    	//	 Rootatwhere = i;
	    	// }
	   //   }
	    //  
	      
	      Set<Integer> nums = test.getsetforread(numNodes);
	     // int leafcnt=0;
	      //int rootatwhere=0;
	    
	     // int interval =1;
	     boolean isRoot=false;
	      int rootwhere=0;
	      for (int i=0;i<numNodes;i++) {
		    	 ((WordCountApplication) cltvec.elementAt(i)).getchildlist();
		    	 ((WordCountApplication) cltvec.elementAt(i)).windowsize = windowsize;
		    	 //((WordCountApplication) cltvec.elementAt(i)).interval = interval;
		    	 ((WordCountApplication) cltvec.elementAt(i)).readrate = rate;
		    	 if (((WordCountApplication) cltvec.elementAt(i)).isRoot()==true) {
			    		//System.out.println("Computation Starts");
			    		//rootatwhere=i;
		    		    ((WordCountApplication) cltvec.elementAt(i)).leafnodenum = expsize;
			    		//Need to set output log to some file here. Only for root.
			    		rootwhere=i;
					    isRoot = true;
			    		}
		    	 
		    	 //if (((WordCountApplication) cltvec.elementAt(i)).childlist.isEmpty()) {
		    		 ((WordCountApplication) cltvec.elementAt(i)).setreadnum(nums.toArray(new Integer[nums.size()])[i]);
		    		 ((WordCountApplication) cltvec.elementAt(i)).readdata(); 
		    		 //leafcnt+=1;
		    	// }
		    		 env.getTimeSource().sleep(2000);
		   }
	      System.out.println("Finished Read Data");
	      //NodeHandle root = printTree(cltvec);
	     
	      //env.getTimeSource().sleep(30000);
	      //if (isRoot==true) {
	    //	  ((WordCountApplication) cltvec.elementAt(rootwhere)).setoutlogforroot();
	    	  //Ok the root is at this VM
	    	  //NodeHandle root = printTree(cltvec);
	   //   }
	      
	     // for (int i=0;i<numNodes;i++) {
	    	 // if (((WordCountApplication) cltvec.elementAt(i)).getParentParam(tp)!=null && ((WordCountApplication) cltvec.elementAt(i)).getChildrenParam(tp).size()!=0) {
	    //	  if (i!=rootwhere) {
	    //		  ((WordCountApplication) cltvec.elementAt(i)).setoutlogforparent();}
	    //	//  }
	    //  }

	      env.getTimeSource().sleep(sleepdelay);
	      System.out.println("Construction Finished, Now Processing Data");
	      for (int i=0;i<numNodes;i++) {
	    	  if (((WordCountApplication) cltvec.elementAt(i)).isRoot()==true) {
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
	
	public static NodeHandle printTree(Vector<WordCountApplication> apps) {
	    // build a hashtable of the apps, keyed by nodehandle
	    Hashtable<NodeHandle, WordCountApplication> appTable = new Hashtable<NodeHandle, WordCountApplication>();
	    Iterator<WordCountApplication> i = apps.iterator();
	    while (i.hasNext()) {
	    	WordCountApplication app = (WordCountApplication) i.next();
	      appTable.put(app.endpoint.getLocalNodeHandle(), app);
	    }
	    NodeHandle seed = ((WordCountApplication) apps.get(0)).endpoint
	        .getLocalNodeHandle();

	    // get the root
	    NodeHandle root = getRoot(seed, appTable);
	    
	    // print the tree from the root down
	    recursivelyPrintChildren(root, 0, appTable);
	    
	    //Print the Root
	    System.out.println("=======================================================");
	    System.out.println("The Root is "+ root.getId().toStringFull());
	    return root;
	  }

	  /**
	   * Recursively crawl up the tree to find the root.
	   */
	  public static NodeHandle getRoot(NodeHandle seed, Hashtable<NodeHandle, WordCountApplication> appTable) {
		  WordCountApplication app = (WordCountApplication) appTable.get(seed);
	    if (app.isRoot())
	      return seed;
	    NodeHandle nextSeed = app.getParent();
	    return getRoot(nextSeed, appTable);
	  }

	  /**
	   * Print's self, then children.
	   */
	  public static void recursivelyPrintChildren(NodeHandle curNode,
	      int recursionDepth, Hashtable<NodeHandle, WordCountApplication> appTable) {
	    // print self at appropriate tab level
	    String s = "";
	    for (int numTabs = 0; numTabs < recursionDepth; numTabs++) {
	      s += "  ";
	    }
	    s += curNode.getId().toString();
	    System.out.println(s);

	    // recursively print all children
	    WordCountApplication app = (WordCountApplication) appTable.get(curNode);
	    NodeHandle[] children = app.getChildren();
	    for (int curChild = 0; curChild < children.length; curChild++) {
	      recursivelyPrintChildren(children[curChild], recursionDepth + 1, appTable);
	    }
	  }
	
	}
