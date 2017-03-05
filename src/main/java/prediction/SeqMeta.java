package prediction;

import motif.Meta;

public class SeqMeta extends Meta{
	protected long sid;
	public SeqMeta(long sid){
		this.sid = sid;
	}
	@Override
	public boolean equals(Meta m) {
		if(this==m)return true;
		if(m instanceof SeqMeta){
			SeqMeta sm = (SeqMeta)m;
			if(this.sid==sm.sid)return true;
		}
		return false;
	}
	@Override
	public int hashCode(){
		return (int)sid;
	}
	@Override
	public String toString() {
		return sid+"";
	}

}
