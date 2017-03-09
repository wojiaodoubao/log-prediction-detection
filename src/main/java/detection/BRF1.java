package detection;

import java.util.List;

public class BRF1 extends BruteForceFired{

	public BRF1(List<MotifNode> mTree, long gap, double alpha) {
		super(mTree, gap, alpha);
	}

	@Override
	/**
	 * 当alpha为1时总是可以满足打分的两个基本要求，当alpha不为1时，打分要求中xy>x条不成立。 
	 * */	
	public double marking(List<Double> probList) {
		double alpha = 1;
		if(probList==null)return 0;
		double score = 0;
		for(int i=probList.size()-1;i>=0;i--){
			score = (1-score*alpha)*probList.get(i)+score*alpha;
		}
		return score;
	}

}
