# Generic class to call a function on multiple threads
import threading
class processingThread (threading.Thread):
    def __init__(self, threadID, name, runFunction, **kwargs):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.func = runFunction
        self.kwargs = kwargs
    def run(self):
        # print(self.name + ": Starting process")
        self.out = self.func(**self.kwargs)
        # print(self.name + ": Process finished")
