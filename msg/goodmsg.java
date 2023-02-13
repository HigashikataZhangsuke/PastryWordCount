package yinzhe.test.msg;

import java.io.IOException;
import java.util.LinkedHashMap;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.JavaSerializer;

public class goodmsg implements RawMessage {
	  public static final short TYPE = 1;
	  Id from;
	  int good;
	  
	  public goodmsg(int gd,Id fin) {
	    this.good =gd;
	    this.from = fin;
	  }
	  
	  public String toString() {
	    return "Dict";
	  }

	  public int getPriority() {
	    return Message.LOW_PRIORITY;
	  }

	  public short getType() {
	    return TYPE;
	  }

	  public int getgd() {
		  return this.good;
	  }
	  
	  public Id getid() {
		  return this.from;
	  }
	  
	  public void serialize(OutputBuffer buf) throws IOException {
		//Use Java default Serializer
		JavaSerializer.serialize(this, buf);
	  }
}
