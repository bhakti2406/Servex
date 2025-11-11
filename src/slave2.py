# Pyro enables code to call methods on objects even if that object is running on a remote machine
import Pyro4
import psutil

def convertTime(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return "%d:%02d:%02d" % (hours, minutes, seconds)

# Exposing classes, methods and properties is done using the @Pyro4.expose decorator
@Pyro4.expose
class Operations(object):
    # This is a normal Python object but it is registered with Pyro so that you can access it remotely.
    # Pyro objects are written just as any other object but the fact that Pyro knows something about them makes them special,
    # in the way that you can call methods on them from other programs. A class can also be a Pyro object,
    # but then you will also have to tell Pyro about how it should create actual objects from that class when handling remote calls.

    # Get status of slaves

    def getStatus(self):
        msg = psutil.sensors_battery()
        msg1 = convertTime(msg.secsleft)
        msg2 = msg.percent
        msg3 = msg.power_plugged
        msg4 = str(str(msg1) + " " + str(msg2) + " " + str(msg3))
        return msg4

    # Getting words from master and counting frequency of every words

    def getMap(self, words):
        dic = {}
        print(words)
        for i in words:
            if i in dic:
                dic[i.lower()] += 1
            else:
                dic[i.lower()] = 1
        st = ""
        for key, val in dic.items():
            st = st + str(str(key) + ":" + str(val)) + " "
        return st

    # Doing Matrix mmultiplication of small portions given by master

    def matmul(self, a, b):
        result = [0] * len(b[0])
        print(a)
        print(b)
        for i in range(len(a)):
            for k in range(len(b[0])):
                result[k] += a[i] * b[i][k]
        str1 = ""
        for i in range(len(result)):
            str1 += str(result[i])
            if i != len(result) - 1:
                str1 += " "
        return str1

# Running daemon on localhost
daemon = Pyro4.Daemon(host="10.200.19.168")
obj = Operations()
uri = daemon.register(obj)
print(uri)
# Locating nameserver running on different PC
ns = Pyro4.locateNS('10.200.19.168')
# Registering the object on nameserver 
ns.register('slave2', uri)
print("Ready!")
daemon.requestLoop()
