package prediction;

import java.util.HashMap;
import java.util.Map;

import motif.Meta;

public class TimeMeta extends SIDMeta{
	protected long time;
	public TimeMeta(long sid,long time){
		super(sid);
		this.time = time;
	}
	public TimeMeta(SIDMeta m,long time){
		super(m.sid);
		this.time = time;
	}
	@Override
	public int hashCode(){
		return (int)sid;
	}
	@Override
	public String toString() {
		return sid+":"+time;
	}
	public long getTime(){
		return this.time;
	}	
}
