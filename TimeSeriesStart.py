#encoding:utf-8
import os
import sys

def main():
	if len(sys.argv)<9:
		print('Wrong arguments!')
		return
	jarPath = sys.argv[1]		
	inputPath = sys.argv[2]
	outputPath = sys.argv[3]
	gap_frequentSequence = sys.argv[4]#频繁模式挖掘时，连续序列约束gap；一般来说gap_frequentSequence==gap_ruleGenerate
	gap_ruleGenerate = sys.argv[5]#规则产生时，连续序列约束gap
	frequency = sys.argv[6]
	ruleGap = sys.argv[7]
	cardinality = sys.argv[8]
	firedThreshold = sys.argv[9]

	command = 'export HADOOP_OPTS= \"-Xmx4096m\"'#set java heap
	execute(command)

	#DataPreprocessing
	DataPreprocessing_Input = inputPath
	DataPreprocessing_Output = outputPath+'/DataPreprocessing'
	command = 'time hadoop jar '+jarPath+' preprocessing.DataPreprocessing '+DataPreprocessing_Input+' '+DataPreprocessing_Output
	execute(command)
	#LogToMeta
	LogToMeta_Input = DataPreprocessing_Output
	Dict = outputPath+'/Dict'
	command = 'time hadoop jar '+jarPath+' preprocessing.LogToMeta '+LogToMeta_Input+' '+Dict	
	execute(command)
	#MetaFileSplit
	MetaFileSplit_Input = DataPreprocessing_Output
	MetaFileSplit_Output = outputPath+'/MetaFileSplit'
	command = 'time hadoop jar '+jarPath+' preprocessing.MetaFileSplit '+MetaFileSplit_Input+' '+MetaFileSplit_Output+' '+Dict	
	execute(command)
	#FrequentSequenceDiscoveryMR
	FrequentSequenceDiscoveryMR_Input = MetaFileSplit_Output
	FrequentSequenceDiscoveryMR_Output = outputPath+'/DuplicatedSequences'
	command = 'time hadoop jar '+jarPath+' prediction.FrequentSequenceDiscoveryMR '+FrequentSequenceDiscoveryMR_Input+' '+FrequentSequenceDiscoveryMR_Output+' '+Dict+' '+gap_frequentSequence+' '+frequency
	execute(command)
	#RemoveDuplicatedSequenceMR
	RemoveDuplicatedSequenceMR_Input = FrequentSequenceDiscoveryMR_Output
	RemoveDuplicatedSequenceMR_Output = outputPath+'/Sequences'
	command = 'time hadoop jar '+jarPath+' prediction.RemoveDuplicatedSequenceMR '+RemoveDuplicatedSequenceMR_Input+' '+RemoveDuplicatedSequenceMR_Output
	execute(command)
	#RulesDiscoveryMR
	RulesDiscoveryMR_Input = RemoveDuplicatedSequenceMR_Output
	RulesDiscoveryMR_Output = outputPath+'/RuleScore'
	command = 'time hadoop jar '+jarPath+' prediction.RulesDiscoveryMR '+RulesDiscoveryMR_Input+' '+RulesDiscoveryMR_Output+' '+inputPath+' '+gap_ruleGenerate+' '+ruleGap+' '+cardinality+' '+firedThreshold+' '+Dict
	execute(command)
	#RulesScoresToRulesMR
	RulesScoresToRulesMR_Input = RulesDiscoveryMR_Output
	RulesScoresToRulesMR_Output = outputPath+'/Rules'
	command = 'time hadoop jar '+jarPath+' prediction.RulesScoresToRulesMR '+RulesScoresToRulesMR_Input+' '+RulesScoresToRulesMR_Output
	execute(command)

def execute(command):
	print(command)
	os.system(command)	

if __name__=='__main__':
	main()
