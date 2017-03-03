package motif;

public class DimensionLogMeta extends LogMeta{
	protected int dimension;
	public DimensionLogMeta(long sid,int dimension){
		super(sid);
		this.dimension = dimension;
	}
	@Override
	public boolean equals(Meta m) {
		if(this==m)return true;
		if(m instanceof DimensionLogMeta){
			DimensionLogMeta dlm = (DimensionLogMeta)m;
			if(dlm.sid==this.sid&&dlm.dimension==this.dimension)return true;
		}
		return false;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "["+sid+"]";
	}	
}