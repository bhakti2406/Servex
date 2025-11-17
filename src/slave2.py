import Pyro4
import psutil


def convertTime(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return "%d:%02d:%02d" % (hours, minutes, seconds)


@Pyro4.expose
class Operations(object):

    def getStatus(self):
        msg = psutil.sensors_battery()
        if msg is None:
            return "Battery info unavailable"

        msg1 = convertTime(msg.secsleft) if msg.secsleft != psutil.POWER_TIME_UNLIMITED else "Charging"
        msg2 = msg.percent
        msg3 = msg.power_plugged

        return f"{msg1} {msg2}% plugged={msg3}"

    def getMap(self, words):
        dic = {}
        print("Received words:", words)

        for i in words:
            key = i.lower()
            dic[key] = dic.get(key, 0) + 1

        return " ".join([f"{k}:{v}" for k, v in dic.items()])

    def matmul(self, a, b):
        print("Row:", a)
        print("Matrix:", b)

        result = [0] * len(b[0])
        for i in range(len(a)):
            for k in range(len(b[0])):
                result[k] += a[i] * b[i][k]

        return " ".join(map(str, result))


if __name__ == "__main__":
    print("Starting Slave2 service...")

    daemon = Pyro4.Daemon(host="127.0.0.1")
    obj = Operations()
    uri = daemon.register(obj)

    print(f"Slave2 URI: {uri}")

    try:
        ns = Pyro4.locateNS("127.0.0.1")
        ns.register("slave2", uri)
        print("Slave2 registered successfully with NameServer.")
    except Exception as e:
        print("❌ Failed to connect to NameServer:", e)
        exit(1)

    print("Slave2 ready and waiting for tasks.")
    daemon.requestLoop()
