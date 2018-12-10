import os
import random
import time
i=1
name_dict={<your name>:<your symphony id>}

while i<100:
    a = str(random.random()*1000000000000)
    filename = "/tmp/symphony-out.%s"%a
    tmpfilename = "/tmp/_symphony-out.%s"%a
    f = open(tmpfilename, 'w')
    x=time.strftime('%m/%d/%Y %H:%M:%S')
    f.write("%s\tHello %s for %sth time. How is this %s"%(name_dict['<your name>'],"<your name>",i,x))
    f.close()
    os.rename(tmpfilename, filename)
    print (x)
    # Sleeping here for 1 ms cause symphony can't handle it. Too many messages.
    time.sleep(0.001) 
    i=i+1
    
