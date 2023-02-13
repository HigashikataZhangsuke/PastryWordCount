package yinzhe.test.msg;

import java.io.IOException;
import java.util.LinkedHashMap;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.scribe.Topic;
import rice.p2p.util.rawserialization.JavaSerializer;

public class selfschedule50msg implements Message {
	
	  Topic tptrans;
	  
	  public selfschedule50msg(Topic tpin) {
		    this.tptrans = tpin;
		    
		  }
		  
		  public String toString() {
		    return "Dict";
		  }

		  public int getPriority() {
		    return Message.LOW_PRIORITY;
		  }

		  
		  public Topic gettp() {
			  return this.tptrans;
		  }
		  
	
		  
		  public void serialize(OutputBuffer buf) throws IOException {
			//Use Java default Serializer
			JavaSerializer.serialize(this, buf);
		  }

}
