package yinzhe.test.msg;

import rice.p2p.scribe.ScribeContent;

public class checkmsg implements ScribeContent{
	int data;
	public checkmsg(int dt) {
		this.data = dt;
	}
}
