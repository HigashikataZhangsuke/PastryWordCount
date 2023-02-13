package yinzhe.test.countalgorithm;

import rice.environment.Environment;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.scribe.Scribe;
import rice.p2p.scribe.ScribeClient;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.ScribeImpl.TopicManager;
import rice.p2p.scribe.Topic;
import yinzhe.test.msg.*;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.messaging.JavaSerializedDeserializer;
/**
 * 
 * @author zhangsuke
 * This is the Class for Scribe Application/Client. Contains functions related to update
 * 
 */
import rice.tutorial.scribe.MyScribeClient.PublishContent;
public class WordCountApplication implements ScribeClient, Application{
	 public int windowsize;
	 public CancellableTask publishTaskroot;
	 public Endpoint endpoint;
	 public Scribe scribe;
	 Environment env;
	 public HashMap<Topic,LinkedHashMap<String,Integer>> selfDict = new HashMap<Topic,LinkedHashMap<String,Integer>>();
	 public HashMap<Topic,LinkedHashMap<Id,Integer>> childlist = new HashMap<Topic,LinkedHashMap<Id,Integer>>();
	 public int whichcsvread;
	 public int linecnt;
	 public int roundcnt;
	 public int readrate;
	 public int readnum;
	 public long endtimeforeachround;
	 public HashMap<Topic,Integer> startplace = new HashMap<Topic,Integer>();
	 public long starttimeforeachwindow;
	 public String[] keyarr;
	 public int leafnodenum;
	 public HashMap<Topic,Integer>  whichwindow = new HashMap<Topic,Integer>();
	 public int rootcnter=0;
	 public int readrndcnt;
	 public int readleft;
	 public int good;
	 public int topicnum;
	 public Vector<Topic> tpvec = new Vector<Topic>();
	 public Vector<Topic> myrttp =new Vector<Topic>();
	 int rttplocation =-1;
	 public HashMap<Topic,CancellableTask> leaftasks = new HashMap<Topic,CancellableTask>();
	 public HashMap<Topic,CancellableTask> roottasks = new HashMap<Topic,CancellableTask>();
	 public String[] tplist = {"VsfjRcpXEWlUhke","RcWZsNYDbSEOkeT","QYydgkMZRCDUGtL","kUYoSLtbNFvWnOw","YhvxzOVsegqIuNA","yQCFNakzldghmqZ","lEBYoHTnikSKeZb","lOwosWirSLfxdkb","yLdQPDXzvjUKmJk","gxcLzHvBSihPpZl","HQRIfzuKLqYjSor","tbvVGsSAPgHuCqh","ICOWVUmLgzJNBYS","OHXGUPysgdeuMzf","GZTWpzVjqfAKQhC","WqmAFSZdoVnHDGN","nfHmivxSURBhAPk","uAKyZUzpVkXLRTm","bXKTmWGwlxVOsgH","pOtJVSnKhIMmTwU","bEGzcgTOukArosN","XjmdohQVgyqtYpD","aipUODuXvSCQskm","gyoWVqeitTlJUwR","ScFYPNLWATMfKpi","KVgcGYvaTjSIDZr","KrtRYXeHEnUQkbT","rfovEBsXZzMYDQP","HzLsGUwcPjoDkTh","ZCdOteSscmnEUJu","ngGNBwMpQcqUOtH","TdavGkgLoutHXNE","nqDaLsRveAbXxZP","XivjMYgBsxDWNtJ","kwWHNmLJjgYbfoE","GhbqlAeTKWtcYfZ","RIqeliDsPUNTnLV","gReYWUFGAfVbphX","JXCOHyKMdrEvNSu","KvMlZSpWrfOIxUq","vIyTeKwZuNcUlHq","rHzcRtuPdOaeoxw","QZLlJcBHfKzasbX","fCviNlWVUrBdZTS","ckmMBIQOhSVnLAd","ehMmEfqGFOpPKdU","zhPYucRUfJBxNoW","aibxQIjFZrgmGPL","LneluObjFJMGYwW","gkMuXxmiRpaHPjD"};
	 //Constructor
	 public WordCountApplication(Node node,int topicnumin) {
		  this.endpoint = node.buildEndpoint(this, "wordcountinstance");
		  this.scribe = new ScribeImpl(node,"Wordcount");
		  ((JavaSerializedDeserializer)this.endpoint.getDeserializer()).setAlwaysUseJavaSerialization(true);
		  this.endpoint.register();
		  this.topicnum = topicnumin;
		  for (int i=0;i<topicnum;i++) {
			  tpvec.add(new Topic(new PastryIdFactory(env), this.tplist[i]));//Use a Vector to store topic.
			  whichwindow.put(tpvec.elementAt(i), 0);
			  this.startplace.put(tpvec.elementAt(i), 0);
		  }
		 
	 }

	  public void mkscribe(Topic tp) throws InterruptedException {
              //Scribe a given topic
			  scribe.subscribe(tp, this);  
	  }

	  public void setreadnum(int readin) {
		  //Set param for read which data
		  this.readnum = readin;
	  }
	
	  
	  public void getchildlist(Topic tp) {
		  //This function is used for a node to get it potential childlist. Will be used further for determining if this node get all data from its child.
		  //It will store all topic's data with this list. When update it will access it through 2 layer.

			  LinkedHashMap<Id,Integer> currtplist = new LinkedHashMap<Id,Integer>();
			  if (this.getChildrenParam(tp).size() !=0) {
				  Iterator iter = this.getChildrenParam(tp).iterator();
				  NodeHandle thisnh = (NodeHandle) iter.next();
				  currtplist.put(thisnh.getId(), 0);
				  while(iter.hasNext()) {
					 thisnh = (NodeHandle) iter.next();
					 currtplist.put(thisnh.getId(), 0);
				  }
				  childlist.put(tp,currtplist);
				  }
	  }
	  
	  
	  public void readdata() {

		  for (int i=0;i<topicnum;i++) {
		  
		  LinkedHashMap<String,Integer> currreaddata = new LinkedHashMap<String,Integer>();
			 this.roundcnt=0;
			 String csvFile1 = "/home/johnny/WCDatasetbig/TestRead"+readnum+".csv";
			 String line = "";
			  String csvspliter = ",";
			  try (BufferedReader br = new BufferedReader(new FileReader(csvFile1))){
			  while (roundcnt<readrate*windowsize) {
				     if ((line = br.readLine()) != null) {
						  String[] dataread = line.split(csvspliter);
						  if (currreaddata.get(dataread[0])!=null) {
							  currreaddata.put(dataread[0], currreaddata.get(dataread[0])+ Integer.parseInt(dataread[1])); }
						  else
						  {
							  currreaddata.put(dataread[0], Integer.parseInt(dataread[1])); 
						  }
						  roundcnt+=1;
					 }
			  }
			  currreaddata.put("marker", 1);
			  this.keyarr= currreaddata.keySet().toArray(new String[currreaddata.keySet().size()]);
			  } catch (FileNotFoundException e) {
		
				e.printStackTrace();
			} catch (IOException e) {
				
				e.printStackTrace();
			} 
			  this.selfDict.put(tpvec.elementAt(i),currreaddata);
		  }
	  }   
	  
	  public void run50(Topic dealwhichtp) {
		  //This is the function for update 50 lines of data stored in this node. Since we update data every second
		  LinkedHashMap<String,Integer> datasend = new LinkedHashMap<String,Integer>();
			if (this.startplace.get(dealwhichtp)!=readrate*windowsize-readrate) {
			for (int i=this.startplace.get(dealwhichtp);i<this.startplace.get(dealwhichtp)+this.readrate;i++) {
				
				datasend.put(keyarr[i],this.selfDict.get(dealwhichtp).get(keyarr[i]));
			}
			
			this.startplace.put(dealwhichtp,this.startplace.get(dealwhichtp)+ this.readrate);
		}
			else {
				//We Readched last line of data, now just udpate it with marker
				for (int i=this.startplace.get(dealwhichtp);i<this.startplace.get(dealwhichtp)+this.readrate+1;i++) {
					datasend.put(keyarr[i],this.selfDict.get(dealwhichtp).get(keyarr[i]));
				}
				//Reset place marker and also stop send data periodically
				this.leaftasks.get(dealwhichtp).cancel();
				this.startplace.put(dealwhichtp, 0);
			}
          //Send data to parent of this topic
		  NodeHandle dst = this.getParentParam(dealwhichtp);
  		  BottomNodesMsg msgsend = new BottomNodesMsg(datasend,this.endpoint.getId(),dealwhichtp);
  		  endpoint.route(dst.getId(), msgsend, null);
	  }
	  
	  
	 public void startPublishTask() throws InterruptedException {
		 for (int i=0;i<myrttp.size();i++) {
			//If you are the root for these topic, then just start a timer let you start aggregation for this window every 150sec.
		    roottasks.put(myrttp.elementAt(i),endpoint.scheduleMessage(new Startwindow(myrttp.elementAt(i)), 1000, windowsize*1000+150000));
		 }
     }
	 
	 public void startPublishTaskforleaf(Topic tp) {
		 //As a leaf node you just start your task of udpate data every 1 sec
		    leaftasks.put(tp,endpoint.scheduleMessage(new selfschedule50msg(tp), 0, 1000));   
     }
	 
	  public void sendupdaterequest(Topic tp) {
	      //As a root publish this content for everyone to start work.
		  this.selfDict.get(tp).put("marker", 1);
	  	  starttimeforeachwindow = System.nanoTime();
	  	  this.logger("Start time of Topic" +tp+" window "+(this.whichwindow.get(tp))+" is "+starttimeforeachwindow);
	  	  MsgOfRequestUpdate updatemsg = new MsgOfRequestUpdate(tp);
	  	  scribe.publish(tp, updatemsg);
	  }
	  
	  
	  //The deliver function for topic
      public void deliver(Topic topic, ScribeContent content) {
    	  if (content instanceof MsgOfRequestUpdate){
    		  Topic whichtp = (Topic)((MsgOfRequestUpdate)content).gettppublished();
    		  //Record which window is being processed. If not the first window, just replce record of childlist.
    		  this.whichwindow.put(whichtp,this.whichwindow.get(whichtp)+1);
    		  if (this.whichwindow.get(whichtp)!=1) {
    			  if (this.childlist.get(whichtp)!=null) {
    				  //Replace value.
    				  for (LinkedHashMap<Id, Integer> tpmap: this.childlist.values()) {
    					  tpmap.replaceAll((key,value)->0);
    				  }
    			  }	  
    		  if (this.getChildrenParam(whichtp).size() == 0) {
    			  startPublishTaskforleaf(whichtp);
    			  }
    		  }
    		  else
    		  {
    			  //No child
    			  if (this.getChildrenParam(whichtp).size() == 0) {
    			  startPublishTaskforleaf(whichtp);
    			  }
    		  }
    	  }
       }
       
      //The deliver function for App
  	  public void deliver(Id id, Message message) {
  		  if(message instanceof BottomNodesMsg) {  		    	
  		   //You received the message, then just need to merge it with your data and upload it
  		    	BottomNodesMsg msgrecv = (BottomNodesMsg)message;
  		    	LinkedHashMap<String,Integer> CountRecv = ((BottomNodesMsg) message).getdictdata();
  		    	Id from = ((BottomNodesMsg) message).getIddata();
  		    	
  		    	Topic thistp = ((BottomNodesMsg) message).gettp();
  		    	CountRecv.forEach((key, value) -> this.selfDict.get(thistp).merge(key, value, (v1,v2) -> v1+v2));
  		    	//this.childlist.remove(from);
  		    	//Maybe could modify here, if this child does not showed up, then assign 1. Else just add 1.
  		    	//In other words, do not pre check whom is who's child. Do it at deliver.
  		  	
  		    	this.childlist.get(thistp).put(from, this.childlist.get(thistp).get(from)+1);
  		    	if (this.getParentParam(thistp) != null) {
  		    		//You are the second-order parent, still need to upload data. But here you need to check your childlist to see if you've revceived all data
  		    		if (check(childlist.get(thistp))==true &&this.selfDict.get(thistp).get("marker")!=1) {
  		    			
  		    		NodeHandle dst = this.getParentParam(thistp);
    	    		BottomNodesMsg msgsend = new BottomNodesMsg(this.selfDict.get(thistp),this.endpoint.getId(),thistp);
    	    		endpoint.route(dst.getId(), msgsend, null);
    	    		this.selfDict.get(thistp).put("marker", 1);
    	    	}
  		    	}
  		    	else {
		    		
  		    		if (checkmarker(selfDict.get(thistp))==true) {
  		    	    //Or multicast here to cancel the former task before start next window?
  		    		endtimeforeachround = System.nanoTime();
  		    		this.logger("End time of Topic" +thistp+" window "+(this.whichwindow.get(thistp)-1)+" is "+endtimeforeachround);
  		    		}
  		    	
  		    	}
  		  }
  		  else if (message instanceof selfschedule50msg) {   	    	
  			    run50(((selfschedule50msg)message).gettp());
  		  }
  		  else if (message instanceof Startwindow) {
  			  
    		  sendupdaterequest(((Startwindow)message).gettp());
    		  //((Startwindow)message).gettp()
    	  }
  	  }  
      
	  public boolean forward(RouteMessage message) {
	    return true;
	  }

	  public void update(NodeHandle handle, boolean joined) {
	    
	  }
	  
	  public boolean anycast(Topic topic, ScribeContent content) {
		    boolean returnValue = scribe.getEnvironment().getRandomSource().nextInt(3) == 0;
		    return returnValue;
	  }
	  
	  public boolean isRoot(Topic tp) {
	    return scribe.isRoot(tp);
	  }
	    
	  public NodeHandle getRoot(Topic tp) {
		  return scribe.getRoot(tp);
	  }
	  
	  public NodeHandle getParentParam(Topic tpinstance) {
	    return ((ScribeImpl)scribe).getParent(tpinstance); 
	  }
	  
	  public Collection<NodeHandle> getChildrenParam(Topic tpinstance) {
	    return scribe.getChildrenOfTopic(tpinstance); 
	  }
	
	  public NodeHandle getParent(Topic tp) {
	    return scribe.getParent(tp); 
	  }	  
	
	  public NodeHandle[] getChildren(Topic tp) {
	    return scribe.getChildren(tp); 
	  }

	  public void childAdded(Topic topic, NodeHandle child) {	
	  }

	  public void childRemoved(Topic topic, NodeHandle child) {
	  }

	  public void subscribeFailed(Topic topic) {
	  }
	
		 public void logger(String str) {
			 // logger function
			 FileWriter fw = null;
			 try {
				 File f = new File("/home/johnny/Testlog/YinzheWordCountTestRoot"+this.endpoint.getId()+".txt");
			     fw = new FileWriter(f,true);
			 // }
			 }catch(IOException e) {
				 e.printStackTrace();
			 }
			 PrintWriter pw = new PrintWriter(fw);
			 pw.print(str+"\n");
			 pw.flush();
			 try {
				 fw.flush();
				 pw.close();
				 fw.close();
			 }catch(IOException e)
			 {
				 e.printStackTrace();
			 }
			 
			 
		 }

		 
		 public boolean check(LinkedHashMap<Id,Integer> childlistinput) {
			 //Check as a parent node if you already received all data from your child
			 boolean isUnique = true;
			// boolean isUnique = true;
			 for (Map.Entry<Id, Integer> en : childlistinput.entrySet()) {
			     //if (en.getValue()!=windowsize && en.getValue()!=1 ) {
				 if (en.getValue()!=windowsize && en.getValue()!=1 ) {
			    	 //if () {
			    		 //Not leaf nor intermediate
			    		 isUnique = false;
			    		 break;
			    	 //}
			     }
			     }         
			// }
			 return isUnique;
			 //return (new HashSet(childlistinput.values()).size()==1);
		 }
		 
		 public  boolean checkmarker(LinkedHashMap<String,Integer> selfDictinput) {
			 //check if you(root) get enough marker so that you could end this round of calculation 
			 int nummar = selfDictinput.get("marker");
			if (nummar>=(int)(this.leafnodenum*0.9))
			
				 {
					 return true;
				 }
				 else
				 {
					 return false;
				 }
		 }
	  
	  public boolean checkRoot() {
		  
		  boolean flag = false;
		  for (int i=0;i<topicnum;i++) {
			  ///Currently just think we are root for one topic. Need to think what if you are multiple topics' roots? 
			  //I think it's OK for you to use another Vector to store this.If use vector, then it should be 
			  if (this.isRoot(tpvec.elementAt(i))==true) {
				  //Ok I'm the root of this tp
				  this.myrttp.add(tpvec.elementAt(i));
				  this.rttplocation = i;
			  }
		  }
		  if (this.rttplocation!= -1) {
			  flag = true;
		  }
		 // System.out.println("One time");
		  return flag;
	  }
}
