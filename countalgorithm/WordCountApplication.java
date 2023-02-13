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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
	 public CancellableTask publishTaskleaf;
	 public Endpoint endpoint;
	 public Scribe scribe;
	 public Topic tp;
	 Environment env;
	 public LinkedHashMap<String,Integer> selfDict = new LinkedHashMap<String,Integer>();
	 public LinkedHashMap<Id,Integer> childlist = new LinkedHashMap<Id,Integer>();
	 public int whichcsvread;
	 public int linecnt;
	 public int roundcnt;
	 //public double interval;
	 public int readrate;
	 //public double rate;
	 public int readnum;
	 public long endtimeforeachround;
	 public int startplace;
	 public long starttimeforeachwindow;
	 public String[] keyarr;
	 public int leafnodenum;
	 public int whichwindow;
	 public int rootcnter=0;
	 public int readrndcnt;
	 public int readleft;
	 public int good;
	 //public ScheduledExecutorService executor; 
	 //public int cntfiles =0;
	 //Constructor
	 public WordCountApplication(Node node) {
		  this.endpoint = node.buildEndpoint(this, "wordcountinstance");
		  this.scribe = new ScribeImpl(node,"Wordcount");
		  ((JavaSerializedDeserializer)this.endpoint.getDeserializer()).setAlwaysUseJavaSerialization(true);
		  this.endpoint.register();
		  this.startplace =0;
		  this.whichwindow = 0;
		  
		  //this.executor = new ScheduledThreadPoolExecutor(2);
		  //Random readgen = new Random();
		  //readnum =readgen.nextInt(100);
	 }
	  
	  public void scribe(Topic tp) {
		  scribe.subscribe(tp, this); 
	  }
	  
	  public void setreadnum(int readin) {
		  this.readnum = readin;
	  }
	  
	  public void check() {
		  checkmsg msg = new checkmsg(0);
		  scribe.publish(tp, msg);
	  }
	  
	  public void getchildlist() {
		  //This function is used for a node to get it potential childlist. Will be used further for determining if this node get all data from its child.
		  if (this.getChildrenParam(this.tp).size() !=0) {
		  Iterator iter = this.getChildrenParam(this.tp).iterator();
		  NodeHandle thisnh = (NodeHandle) iter.next();
		  childlist.put(thisnh.getId(), 0);
		  //childlist.add(thisnh.getId());
		  while(iter.hasNext()) {
			 thisnh = (NodeHandle) iter.next();
			// childlist.add(thisnh.getId());
			 childlist.put(thisnh.getId(), 0);
		  }
		  }
	  }
	  
	  public void readdata() {
			// TODO Auto-generated method stub
		  //Read all the data you need
			 //int passline=0;
			 this.roundcnt=0;
			 
			 //String csvFile1 = "/home/johnny/WCDataset2/TAXI_sample_data_senml_time"+readnum+".csv";
			 String csvFile1 = "/home/johnny/WCDatasetbig/TestRead"+readnum+".csv";
			 String line = "";
			  String csvspliter = ",";
			  //PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream("/home/johnny/YinzheWordCountTest"+ this.endpoint.getId() +".txt")),true);
		      //System.setOut(ps);
			  try (BufferedReader br = new BufferedReader(new FileReader(csvFile1))){
			  while (roundcnt<readrate*windowsize) {
					  // long stime =  System.nanoTime();
				     //while (passline<linecnt)
				     //{br.readLine();
				     //passline+=1;}
				     if ((line = br.readLine()) != null) {
						  //long stime =  System.nanoTime();
				    	  //line = br.readLine();
						  String[] dataread = line.split(csvspliter);
						 // System.out.println(dataread[0]+" "+dataread[1]);
						  if (selfDict.get(dataread[0])!=null) {
						  selfDict.put(dataread[0], selfDict.get(dataread[0])+ Integer.parseInt(dataread[1])); }
						  else
						  {
							  selfDict.put(dataread[0], Integer.parseInt(dataread[1])); 
						  }
						  //selfDict.entrySet().forEach(entry-> {System.out.println(entry.getKey()+" "+ entry.getValue());});
						  roundcnt+=1;
					 }
			  }
			  //linecnt+= roundcnt;
			  this.selfDict.put("marker", 1);
			  this.keyarr= this.selfDict.keySet().toArray(new String[this.selfDict.keySet().size()]);
			  //this.readrndcnt = (readrate*windowsize)/800;
			  //this.readleft = (readrate*windowsize)-800*readrndcnt;
			  } catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	  }   
	  
	  public void run50() {
		  
		  LinkedHashMap<String,Integer> datasend = new LinkedHashMap<String,Integer>();
			if (this.startplace!=readrate*windowsize-readrate) {
			for (int i=startplace;i<this.startplace+this.readrate;i++) {
				
				datasend.put(keyarr[i],this.selfDict.get(keyarr[i]));
			}
			
			this.startplace+= this.readrate;
			//System.out.println(start);
			//datasend.entrySet().forEach(entry-> {System.out.println(entry.getKey()+" "+ entry.getValue());});
			//System.out.println("=============");
		}
			else {
				//Readched last line the marker
				//LinkedHashMap<String,Integer> datasend = new LinkedHashMap<String,Integer>();
				for (int i=startplace;i<this.startplace+this.readrate+1;i++) {
					
					datasend.put(keyarr[i],this.selfDict.get(keyarr[i]) );
				}
				
				//
				//start+=1;
				//this.startplace+= this.readrate+1;
				//datasend.put(keyarr[this.startplace],this.selfDict.get(keyarr[this.startplace]));
				//Just reset since we need to redo it again
				this.publishTaskleaf.cancel();
				this.startplace=0;
				
				//datasend.entrySet().forEach(entry-> {System.out.println(entry.getKey()+" "+ entry.getValue());});
				//System.out.println("=============");
				
				
			}

		  NodeHandle dst = this.getParentParam(this.tp);
  		  BottomNodesMsg msgsend = new BottomNodesMsg(datasend,this.endpoint.getId());
  		  endpoint.route(dst.getId(), msgsend, dst);
	  }
	  
		  /**
		  
		  LinkedHashMap<String,Integer> datasend = new LinkedHashMap<String,Integer>();
		  if (this.readrndcnt>0) {
			  if (this.startplace!=800-readrate) {
					for (int i=startplace;i<this.startplace+this.readrate;i++) {
						
						datasend.put(keyarr[i],this.selfDict.get(keyarr[i]));
					}
					
					this.startplace+= this.readrate;
			  }
			  else {
					for (int i=startplace;i<this.startplace+this.readrate;i++) {
						
						datasend.put(keyarr[i],this.selfDict.get(keyarr[i]) );
						
					}
					this.startplace =0;
					this.readrndcnt-=1;
			  }
			  
		  }
		  else {
			  if (this.startplace!=this.readleft-readrate) {
				  for (int i=startplace;i<this.startplace+this.readrate;i++) {
						
						datasend.put(keyarr[i],this.selfDict.get(keyarr[i]));
					}
					
					this.startplace+= this.readrate;
				  
			  }else {
					for (int i=startplace;i<this.startplace+this.readrate;i++) {
						
						datasend.put(keyarr[i],this.selfDict.get(keyarr[i]) );
						
					}
					//Also put the marker
					datasend.put("marker",1);
					this.publishTaskleaf.cancel();
					this.startplace =0;
			  }
			  
			  
		  }
		  
		  NodeHandle dst = this.getParentParam(this.tp);
  		  BottomNodesMsg msgsend = new BottomNodesMsg(datasend,this.endpoint.getId());
  		  endpoint.route(dst.getId(), msgsend, dst);
		  
		  
		  
	  }
		 **/ 
		  //Do the data send here. And just note you are a bottom leaf node
		  //Modify here. Since your data only have 822 non-repeated datakeys
		  //Or you could consider get a bigger dataset.
		  /**
		  LinkedHashMap<String,Integer> datasend = new LinkedHashMap<String,Integer>();
			if (this.startplace!=readrate*windowsize-readrate) {
			for (int i=startplace;i<this.startplace+this.readrate;i++) {
				
				datasend.put(keyarr[i],this.selfDict.get(keyarr[i]));
			}
			
			this.startplace+= this.readrate;
			//System.out.println(start);
			//datasend.entrySet().forEach(entry-> {System.out.println(entry.getKey()+" "+ entry.getValue());});
			//System.out.println("=============");
		}
			else {
				//Readched last line the marker
				//LinkedHashMap<String,Integer> datasend = new LinkedHashMap<String,Integer>();
				for (int i=startplace;i<this.startplace+this.readrate+1;i++) {
					
					datasend.put(keyarr[i],this.selfDict.get(keyarr[i]) );
				}
				
				//
				//start+=1;
				//this.startplace+= this.readrate+1;
				//datasend.put(keyarr[this.startplace],this.selfDict.get(keyarr[this.startplace]));
				//Just reset since we need to redo it again
				this.publishTaskleaf.cancel();
				this.startplace=0;
				
				//datasend.entrySet().forEach(entry-> {System.out.println(entry.getKey()+" "+ entry.getValue());});
				//System.out.println("=============");
				
				
			}

		  NodeHandle dst = this.getParentParam(this.tp);
  		  BottomNodesMsg msgsend = new BottomNodesMsg(datasend,this.endpoint.getId());
  		  endpoint.route(dst.getId(), msgsend, dst);
	  }
	  **/
	  
	  
	 public boolean check(LinkedHashMap<Id,Integer> childlistinput) {
		 //Redo. Since you are gonna to cehck the marker value
		//Set<Integer> values = new HashSet<Integer>(childlistinput.values());
		//boolean isUnique = values.size()==1;
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
	 
	 public boolean check2(LinkedHashMap<Id,Integer> childlistinput) {
		 //Redo. Since you are gonna to cehck the marker value
		Set<Integer> values = new HashSet<Integer>(childlistinput.values());
		boolean isUnique = values.size()==1;
		 //boolean isUnique = true;
		// boolean isUnique = true;
		// for (Map.Entry<Id, Integer> en : childlistinput.entrySet()) {
		     //if (en.getValue()!=windowsize && en.getValue()!=1 ) {
			// if (en.getValue()!=windowsize && en.getValue()!=1 ) {
		    	 //if () {
		    		 //Not leaf nor intermediate
		    //		 isUnique = false;
		    		 //break;
		    //	 //}
		   //  }
		   //  }         
		//// }
		return isUnique;
		 //return (new HashSet(childlistinput.values()).size()==1);
	 }
	 
	 
	 
	 public  boolean checkmarker(LinkedHashMap<String,Integer> selfDictinput) {
		 //boolean result = false;
		 //You need to check if you read the marker now. Since it always read at last time.
		 //if (selfDictinput.get("marker")!=null) {
		 int nummar = selfDictinput.get("marker");
			 if (nummar>=(int)(this.leafnodenum*0.9))
			 {
				 return true;
			 }
			 else
			 {
				 return false;
			 }
		 //}
		 //else {
		//	 return false;
		// }
		 //return result;
	 }
	 
	 public void startPublishTask() {
		    publishTaskroot = endpoint.scheduleMessage(new Startwindow(), 1000, windowsize*1000+90000);//Give 5 more sec every window.
		    //System.out.println("Start Publishtask");
     }
	 
	 public void startPublishTaskforleaf() {
		    publishTaskleaf = endpoint.scheduleMessage(new selfschedule50msg(), 0, 1000);
		    
		    //System.out.println("Start Publishtask");
     }
	 

	 public void logger(String str) {
		 FileWriter fw = null;
		 try {
		  //if (this.isRoot()!=true) 
		 // {
			//  File f = new File("/home/johnny/Testlog/YinzheWordCountTestPar" +this.endpoint.getId()+ ".txt");
		 // }
		  //fw = new FileWriter(f,true);
		 // }else 
		 // {
			 File f = new File("/home/johnny/Testlog/YinzheWordCountTestRoot.txt");
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
	 
	 
	  public void sendupdaterequest() {
	      //Just publish a random msg for asking update.
		  //System.out.println("Root sent the count and update request");
	  	  int datamu = 0;
	  	  //this.whichwindow +=1;
	  	  //this.whichwindow = windowcntinput;
	  	  //Only Root will receive this message
	  	  this.selfDict.replaceAll((key,value)->1);
	  	  starttimeforeachwindow = System.nanoTime();
	  	  this.logger("Start time of window "+(whichwindow)+" is "+starttimeforeachwindow);
	  	  MsgOfRequestUpdate updatemsg = new MsgOfRequestUpdate(datamu);
	  	  scribe.publish(this.tp, updatemsg);
	  }
	  
	  //The deliver function for topic
      public void deliver(Topic topic, ScribeContent content) {
    	  if (content instanceof MsgOfRequestUpdate){
    		  this.whichwindow +=1;
    		  if (this.whichwindow!=1) {
    		  //In case someone failed. Update your child list every window
    		  //if (this.isRoot()==true) {
    		//	  this.logger("Start time of getchild "+System.nanoTime());	
    	    //  }
    		 this.childlist.clear();
    		 this.getchildlist();
    		// if (this.isRoot()==true) {
   		//	 this.logger("end time of getchild "+System.nanoTime());	
   	         //}
    		 // publishTaskleaf.cancel();
    			  //Set for parents
    		// if (this.isRoot()==true) {
    			
    		// }
    		  //if (this.getParentParam(this.tp) != null) {
    		//	  this.selfDict.put("marker", 1);
    		  //}
    		  //Sleep here?
    		  if (this.getChildrenParam(this.tp).size() == 0) {
    			  startPublishTaskforleaf();
    			  }
    		  }
    		  else
    		  {
    			  //No child
    			  if (this.getChildrenParam(this.tp).size() == 0) {
    			  startPublishTaskforleaf();
    			  }
    		  }//But if the second time window come you need to stop the former task!!
    	  }
    	  else if (content instanceof checkmsg) {
    		  if (this.isRoot()!=true) {
    		  good = 1;
    		  goodmsg msg = new goodmsg(good,this.endpoint.getId());
    		  NodeHandle dst = this.getParentParam(this.tp);
	    	  endpoint.route(dst.getId(), msg, dst);
    		  }
    	  }
       }
       
      //The deliver function for App
  	  public void deliver(Id id, Message message) {
  		  if(message instanceof BottomNodesMsg) {  		    	
  		    //	System.out.println("*********Below is a Parent node's Update Process*********");
  		    //	System.out.println("I am Node "+endpoint.getId()+" received an BottomNodesMsg");
  		    	BottomNodesMsg msgrecv = (BottomNodesMsg)message;
  		    	LinkedHashMap<String,Integer> CountRecv = ((BottomNodesMsg) message).getdictdata();
  		    	Id from = ((BottomNodesMsg) message).getIddata();
  		    	CountRecv.forEach((key, value) -> this.selfDict.merge(key, value, (v1,v2) -> v1+v2));
  		    	//this.childlist.remove(from);
  		    	this.childlist.put(from, childlist.get(from)+1);
  		    	//if (this.isRoot()==true && CountRecv.get("marker")!=null) {
  		    	//	this.logger("Root Get Marker with value "+CountRecv.get("marker")+ " from "+from );
  		    	//}
  		    	/**
  		    	 * Remove is not a good idea. Prefer "Add"
  		    	 */
  		    	if (this.getParentParam(this.tp) != null) {
  		    		//You are the second-order parent, still need to upload data. But here you need to check your childlist to see if you've revceived all data
  		    		//if (CountRecv.get("marker")!=null) {
  		    			//this.logger("The Parent Get Marker with value" +CountRecv.get("marker")+ " from "+from );
  	  		    		//this.logger(this.childlist.toString());
  		    		//}
  		    		
  		    		if (check(childlist)==true &&this.selfDict.get("marker")!=1) {
  		    			//Make sure here, your marker data also received. Cuz you are only get it from the last part of bottom data
    	    		
  		    		NodeHandle dst = this.getParentParam(this.tp);
    	    		BottomNodesMsg msgsend = new BottomNodesMsg(this.selfDict,this.endpoint.getId());
    	    		endpoint.route(dst.getId(), msgsend, dst);
    	    		this.selfDict.replaceAll((key,value)->1);
    	    		
    	    	//	System.out.println("Node "+endpoint.getId()+" finished Route Msgs");
    	    	//	System.out.println("The dict I have is "+ this.selfDict);
    	    	//	System.out.println("                                         ");
    	    	}
  		    	}
  		    	else {
		    		//you are the root, print final result in the last round. 
  		    		//System.out.println("Received this round");
  		    		//LinkedHashMap<String,Integer> Dictsorted = sortByValue(this.selfDict);
  					//for (Map.Entry<String, Integer> en : Dictsorted.entrySet()) {
  				         //System.out.println("Root Marker "+this.selfDict.get("marker"));
  				   //                      ", Value = " + en.getValue());
  				   // }
  		    		//if (check(childlist)==true) {
  		    			//System.out.println("Root Marker "+this.selfDict.get("marker"));
  		    			//Ok received all data, then just check marker
  				         //xiuzheng
  				    //this.selfDict.put("marker",this.selfDict.get("marker")-(windowsize-1)*this.childlist.size());
  				 // System.out.println("Root Marker "+this.selfDict.get("marker"));
  		    		//this.logger("Root Marker "+this.selfDict.get("marker"));
  		    		//System.out.println(checkmarker(selfDict));
  		    		if (checkmarker(selfDict)==true) {
  		    	    //Or multicast here to cancel the former task before start next window?
  		    		endtimeforeachround = System.nanoTime();
  		    		this.logger("End time of window "+(whichwindow-1)+" is "+endtimeforeachround);
  		    		//if (check(childlist)==true) {
  		    			
  		    		//}
  		    		}
  		    		
  		    		//ruhe quebao zuihouyicigengxin gaoding ?
		    		//System.out.println("Final result is");
  		    		//LinkedHashMap<String,Integer> Dictsorted = sortByValue(this.selfDict);
  					//for (Map.Entry<String, Integer> en : Dictsorted.entrySet()) {
  				    //     System.out.println("Key = " + en.getKey() +
  				     //                    ", Value = " + en.getValue());
  				    //}
  					
		    	   // }
  		    		//selfDict.clear();
  		    	
  		    	}
  		  }
  		  else if (message instanceof selfschedule50msg) {   	    	
  	    	//if (this.isRoot()!=true) {
	    		//Do nothing here
	      //  }
	    	//else{
	    		//I'm a bottom node,send data to my parent
	    	//if (this.getChildrenParam(this.tp).size() == 0) {
	    	//	System.out.println("Bottom Child Node "+endpoint.getId()+ " is Ready to update");
	    		run50();
	    	//}
	    	//}
  		 // }
  		  }
  		  else if (message instanceof Startwindow) {
  			 // if (this.rootcnter!=0) {
  			//	  //For 2nd, 3rd.... you will need to let every one sleep 6000 sec
  			//  }
  			//  else {//This is the first time. Ok you could just start working.
    		  sendupdaterequest();
    		//  this.rootcnter+=1;
    		 // }
    	  }
  		  else if (message instanceof goodmsg) {
  			    goodmsg msgrecv = (goodmsg)message;
		    	int good = msgrecv.getgd();
		    	Id from = msgrecv.getid();
		    	this.good +=good;
		    	//this.childlist.remove(from);
		    	this.childlist.put(from, childlist.get(from)+1);
		    	if (this.getParentParam(this.tp) != null) {
		    		if (check2(childlist)==true ) {
		    			//Make sure here, your marker data also received. Cuz you are only get it from the last part of bottom data
		    		  goodmsg msg = new goodmsg(this.good,this.endpoint.getId());
		      		  NodeHandle dst = this.getParentParam(this.tp);
		  	    	  endpoint.route(dst.getId(), msg, dst);
		  	    	  this.childlist.clear();
		  	    	  this.getchildlist();
	    	}
		    	}
		    	else {
		    		if (check2(childlist)==true &&this.good==299) {
		    			System.out.println("System is stable now");
		    			this.good =1;
		    		//this.logger("End time of window "+(whichwindow-1)+" is "+endtimeforeachround);
		    		}
  		  }
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
	  
	  public boolean isRoot() {
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
	
	  public NodeHandle getParent() {
	    return scribe.getParent(tp); 
	  }	  
	
	  public NodeHandle[] getChildren() {
	    return scribe.getChildren(tp); 
	  }

	  public void childAdded(Topic topic, NodeHandle child) {	
	  }

	  public void childRemoved(Topic topic, NodeHandle child) {
	  }

	  public void subscribeFailed(Topic topic) {
	  }
	
	  public void settopic(Topic tpinstance) {
		this.tp = tpinstance;
	  }
	  
	  public static LinkedHashMap<String, Integer> sortByValue(LinkedHashMap<String, Integer> hm)
	    {
	        // Create a list from elements of LinkedHashMap
	        List<Map.Entry<String, Integer> > list =
	                new LinkedList<Map.Entry<String, Integer> >(hm.entrySet());
	 
	        // Sort the list
	        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
	            public int compare(Map.Entry<String, Integer> o1,
	                               Map.Entry<String, Integer> o2)
	            {
	                return (o2.getValue()).compareTo(o1.getValue());
	            }
	        });
	         
	        // put data from sorted list to LinkedHashMap
	        LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
	        for (Map.Entry<String, Integer> aa : list) {
	            temp.put(aa.getKey(), aa.getValue());
	        }
	        return temp;
	    }
	  
}
