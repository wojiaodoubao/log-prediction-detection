package motif;

import java.util.List;
import java.util.Map;

/**
 * 跨维度的motif
 * */
public class MultiDimensionMotif {
	List<DimensionLogMeta> seq;
	float POD;// POD=TP/(TP+FN) 此motif预测对正例占所有正例的比例 越高越好最高100%
	float FAR;// FAR=FP/(TP+FP) 此motif预测为正的有多少预测错     越低越好最低0%
	float CSI;// CSI=TP/(TP+FP+FN) TP+FN+FP=T+FP 因为永远预测为P，所以TN=0，故CSI=(TP+TN)/(TP+FP+FN+TN)预测正确率
	Map<Integer,List<Integer>> indexMap;//序列出现标记位置索引-<example_id,位置索引>

}
