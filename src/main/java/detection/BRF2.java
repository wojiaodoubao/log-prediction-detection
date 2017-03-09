package detection;

import java.util.List;

public class BRF2 extends BruteForceFired{

	public BRF2(List<MotifNode> mTree, long gap, double alpha) {
		super(mTree, gap, alpha);
	}

	@Override
	public double marking(List<Double> probList) {
		double alpha = this.alpha;
		if(probList==null)return 0;
		if(alpha>1)alpha=0.5;
		double left = 1;
		double score = 0;
		for(int i=probList.size()-1;i>=0;i--){
			score = left*alpha*probList.get(i)+score;
			left = (1-alpha)*left;
		}
		return score;
	}

}
