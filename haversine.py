import math 

import os, pp

def haversine(key,lat1, lon1, lat2, lon2):
    R = 6372.8 # Earth radius in kilometers
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    a = math.sin(dLat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dLon/2)**2
    c = 2* math.asin(math.sqrt(a))
    #calculating KM
    a = R * c
    message = "the Great Circle Distance calculated by pid %d was %d KM"%(os.getpid(), a)
    return (key, message)


def aggregate_results(result):
    print "Computing results with PID [%d]" % os.getpid()
    result_dict[result[0]] = result[1]

users={'california_to_newJersey' : (37.0000,120.0000,40.0000,74.0000),
'oklahoma_to_texas' : (35.5000, 98.0000,31.0000, 100.0000),
'arizona_to_kansas' : (34.0000, 112.0000,38.5000, 98.0000),
'mississippi_to_boston' : (33.0000, 90.0000,42.3581, 71.0636)} 

#This dict stores Great distance values for each key in users
result_dict = {}


job_server = pp.Server(ncpus=4)

for key in users.keys():
    job_server.submit(haversine, (key,users[key][0],users[key][1],users[key][2],users[key][3]), modules=('os','math',),callback=aggregate_results)

"""
Above line creates multiple processes and assign executing haversine function with arguments as key,expanded tuple.Processes starts executing using all cores of your processor. Wait for all processes complete execution then retrieve result
"""
job_server.wait()

#Next main process starts executing
print "Main process PID [%d]" % os.getpid()
for key, value in result_dict.items():
    print "For input %s, %s" % (key, value)
