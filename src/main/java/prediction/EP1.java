package prediction;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class EP1 extends ErrorPrediction{

	public EP1(List<RuleNode> rTree, Map<String, Long> logToSidDict, Map<Long, String> sidToLogDict, long ruleGap,
			long gap) {
		super(rTree, logToSidDict, sidToLogDict, ruleGap, gap);
	}

	@Override
	public void ruleFiredInfo(List<SeqMeta> prefix, Set<List<SeqMeta>> suffixSet, TimeMeta curM, long ruleGap) {
		System.out.println("-----------------------------");
		System.out.println(prefix);
		System.out.println(suffixSet);
	}

}
