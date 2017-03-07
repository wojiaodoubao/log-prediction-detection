package prediction;

import motif.Meta;

public class TimeMeta extends Meta{
	protected long sid;
	protected long time;
	public TimeMeta(long sid,long time){
		this.sid = sid;
		this.time = time;
	}
	@Override
	/**
	 * sid相等就相等。
	 * */
	public boolean equals(Meta m) {
		if(this==m)return true;
		if(m instanceof TimeMeta){
			TimeMeta sm = (TimeMeta)m;
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
		return sid+":"+time;
	}

}
