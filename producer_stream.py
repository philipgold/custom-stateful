# This code simulates simple stream of events of two types. Event A rate is  0.1 sec , event B rate is 1 sec.
# Each event contains its serial number embedded in the event value
# By Alexander Hazan May 13 2018

from kafka import KafkaProducer
#from kafka.errors import KafkaError
import numpy as np
import time,datetime
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def BuildMessages(noMessages,TimeInterval,Str):
    val = ''.join(["Value" , Str])
    data = [{"TimeStamp":0, val:1}]
    for i in xrange(noMessages):
        data.append({"TimeStamp":(i+1)*TimeInterval, val:i+2})
    return data

if __name__ == "__main__":
    # event latency
    EventDelay = 0.1
    # late events latency
    EventLateTime = 0
    # number of messages
    noMessages=100
    # event rate
    TimeInterval = 0.1

    DataA = BuildMessages(noMessages, TimeInterval, "A")
    DataB = BuildMessages(noMessages/10,TimeInterval*10,"B")

    SamplesA = map(lambda d: d.get('TimeStamp'), DataA)
    SamplesB = map(lambda d: d.get('TimeStamp'), DataB)
    AllSamples = SamplesA+SamplesB
    AllSamples.sort()
    times = np.unique(AllSamples)

    for curtime in times:

        if curtime == 0:
            Delta = 0
        else:
            Delta = curtime - times[np.where(np.array(times)==curtime)[0][0]-1]

        time.sleep(Delta)

        # current time wall clock
        TimeCurrent = datetime.datetime.fromtimestamp(time.time())

        # Add latency
        TimeEvent = TimeCurrent - datetime.timedelta(0, EventDelay)

        idxA = np.where(np.array(SamplesA)==curtime)[0]
        idxB = np.where(np.array(SamplesB)==curtime)[0]

        if idxA.size>0 and idxB.size>0:
            # Send message A & B
            messages = (DataA[idxA], DataB[idxB])
        elif idxA.size>0:
            # Send message A
            messages = (DataA[idxA],())
        elif idxB.size>0:
            # Send message B
            messages = (DataB[idxB],())
        else:
            messages = ()


        for message in messages:
            if message:
                if message.keys()[1].endswith("A"):
                    eventname = "A"
                    k = "A"
                    v = message['ValueA']
                else:
                    eventname = "B"
                    k = "B"
                    v = message['ValueB']

                # Late data
                if (v % 10 == 4):
                    TimeEvent = TimeEvent - datetime.timedelta(0, EventLateTime)

                eventtime = TimeEvent.strftime('%Y-%m-%d %H:%M:%S.%f')
                event = {"EventName":eventname, "EventTime":eventtime, "Value":v}
                # event = "EventName" + eventname, "EventTime" + eventtime, "Value"+ str(v)
                print event
                producer.send("Cads", key= k, value = json.dumps(event))  #self.producer.send(self.topic,key=key, value=str(rawMsg))
