import threading, time, os, json
os.chdir('/Users/Timothe/Downloads/SF_light/scripts') # Cause VScode is weird
NUM_THREADS = 2

class processingThread (threading.Thread):
	global legs, out
	def __init__(self, threadID, name, runFunction, **kwargs):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.func = runFunction
		self.kwargs = kwargs
	def run(self):
		print(self.name + ": Starting process")
		outDict = self.func(**self.kwargs)
		threadLock.acquire()
		out.update(outDict)		
		threadLock.release()

def testFunc(**kwargs):
	#st, end
	stC = kwargs.get("stC")
	endC = kwargs.get("endC")
	outDict = {}

	for i in range(stC, endC):
		l = legs[i].split(",")
		outDict[l[1]] = l[2]
	return outDict

threadLock = threading.Lock()
threads = []
with open("../input_files/legs.csv", "r") as legFile:
	legs = legFile.readlines()

for i in range(NUM_THREADS):
	st = (i * len(legs)) // NUM_THREADS
	end = ((i+1) * len(legs)) // NUM_THREADS
	threads.append(processingThread(i, "test_thread_" + str(i), testFunc, stC = st, endC = end))
out = {}
for thread in threads:
	thread.start()
for t in threads:
	t.join()
	print(t.name + ": Process finished")

with open("../output_files/multi_thread_out.json", "w") as outFile:
	outFile.write(json.dumps(out))
print("Main process finished")