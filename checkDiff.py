#encoding:utf-8
import sys
import os

Usage = '''
	Usage:
	checkDiff.py path1 path2	
'''
FilePath = ["Sequences/part-r-00004",
	"Sequences/part-r-00003",
	"Sequences/part-r-00002",
	"Sequences/part-r-00001",
	"Sequences/part-r-00000",
	"MetaFileSplit/part-r-00000",
	"Dict/part-r-00000",
	"DataPreprocessing/part-r-00000"]
def main():
	if len(sys.argv)<3:
		print(Usage)
		return
	path1 = sys.argv[1]
	path2 = sys.argv[2]
	for fileName in FilePath:
		print('*****'+fileName+'*****')
		p1 = open(os.path.join(path1,fileName))
		p2 = open(os.path.join(path2,fileName))
		line1 = p1.readlines()
		line2 = p2.readlines()	
		minLines = min(len(line1),len(line2))	
		i=0
		while i<minLines:
			if line1[i]!=line2[i]:
				print(i,line1[i],line2[i])
			i+=1
		while i<len(line1):
			print(i,line1[i],None)
			i+=1
		while i<len(line2):
			print(i,None,line2[i])
			i+=1
		p1.close()
		p2.close()
	return

if __name__=='__main__':
	main()
