package yinzhe.test.msg;

import java.io.IOException;

import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.scribe.Topic;
import rice.p2p.util.rawserialization.JavaSerializer;

public class Startwindow implements Message{
	 Topic tptrans;
	  public Startwindow(Topic tpin) {
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
