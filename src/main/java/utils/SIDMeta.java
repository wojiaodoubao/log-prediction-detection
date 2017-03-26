package utils;

import motif.Meta;

public class SIDMeta extends Meta{
	protected long sid;
	public SIDMeta(long sid){
		this.sid = sid;
	}	
	@Override
	public boolean equals(Object m) {
		if(this==m)return true;
		if(m instanceof SIDMeta){
			SIDMeta sm = (SIDMeta)m;
			if(this.sid==sm.sid)return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return sid+"";
	}
	public long getSID(){
		return this.sid;
	}
}
