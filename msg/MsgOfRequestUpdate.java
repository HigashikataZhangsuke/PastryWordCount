package yinzhe.test.msg;

import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.Topic;

/**
 * 
 * @author zhangsuke
 * This is the Class for Constructing a RequestUpdateWordCountData Message. Data could be any type.
 * 
 */

public class MsgOfRequestUpdate implements ScribeContent {
	 
	  Topic mytp;
	  public MsgOfRequestUpdate(Topic tp) {
		
		  this.mytp = tp;
	  }
	  
	  public Topic gettppublished() {
		  return this.mytp;
	  }

}
