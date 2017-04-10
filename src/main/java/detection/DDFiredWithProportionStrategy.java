package detection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import utils.TimeMeta;

public class DDFiredWithProportionStrategy extends DPDicTreeFired{
	public static void main(String args[]) throws IOException{
		/**
		 * mTree内容：
			0,1,3:0,0.5,0
			0,1,4:1,0.3,1
			0,2:2,0.2,2 
		 * */
		List<DDNode> ddTree = DPDicTreeFired.createDDTreeFromLog("/home/belan/Desktop/mTree");
		//firstSearch(mTree,0);
		List<TimeMeta> logSeq = new ArrayList<TimeMeta>();
		logSeq.add(new TimeMeta(0,0));
		logSeq.add(new TimeMeta(1,1));
		logSeq.add(new TimeMeta(3,2));
		logSeq.add(new TimeMeta(2,3));
		logSeq.add(new TimeMeta(4,4));
		System.out.println(new DDFiredWithProportionStrategy(ddTree,10,0.5).scoreForLogSeq(logSeq));		
	}
	
	public DDFiredWithProportionStrategy(List<DDNode> ddTree, long gap, double alpha) {
		super(ddTree, gap, alpha);
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
