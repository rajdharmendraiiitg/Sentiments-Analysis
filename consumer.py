from collections import OrderedDict
from pymongo import MongoClient
import datetime
import boto.sqs
from boto.sqs.message import Message
import pickle
from datetime import date
import json
import re
import ast
import sys
import random
import topia.termextract
from topia.termextract import extract


ACCESS_KEY="Enter Your"
SECRET_KEY="Enter Your"

REGION="Enter Your"

####-----------------------------------------------------------------########

parseddic = {}
aggregatedic = {}
TERMS = {}
global conn,q,client,db,keyword

#######-------Consumer function---------------------------#########
def setupConsumer():
    global conn,q,client,db,keyword
    keyword = 'india'

    #load sentimens
    sent_file =  open('AFINN.txt')
    sent_lines = sent_file.readlines()
    for line in sent_lines:
        s = line.split(' ')
        l = len(s)
       # print float(s[l-1])
        TERMS[s[0]] = s[l-1]
    sent_file.close()

    #####----Connect to sqs---#####
conn = boto.sqs.connect_to_region(REGION,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
# q = conn.get_all_queues(prefix='arsh-queue')
q = conn.lookup('arsh-queue')
#print "q==:"
#print q
# queuecount=len(q)
# print("Queue count = "+ str(queuecount))

    ###--------Connect to mongoDB----#####
client = MongoClient()
client = MongoClient('localhost',27017)
db = client['na']

#####------------Sentiment finder----------#########
def findsentiment(tweet):
    splitTweet = tweet.split()
    sentiment = 0.0
    for word in splitTweet:
        # print word
        if TERMS.has_key(word):
            # print TERMS[word]
            sentiment = sentiment + float(TERMS[word])
    return sentiment

#####---------ParseTweet Functin-------#####
def parseTweet(tweet):
    global retweettext
    if(tweet.has_key('created_at')):
        createdat = tweet['created_at']
        hourint = int(createdat[11:13])
        parseddic['hour'] = str(hourint)
    # print "i m here"
    ######---retweets
    parseddic['toptweets'] = {}
    if tweet.has_key('retweeted_status'):
        retweetcount = tweet['retweeted_status']['retweet_count']
        retweetscreenname = tweet['retweeted_status']['user']['screen_name'].encode('utf-8',errors = 'ignore')
        retweetname = tweet['retweeted_status']['user']['name'].encode('utf-8',errors = 'ignore')
        # print tweet['retweeted_status']['user']
        if tweet['retweeted_status']['user']['description']:
            retweettext = tweet['retweeted_status']['user']['description'].encode('utf-8',errors = 'ignore')
        retweetdic = {}
        retweetdic['retweetcount'] = retweetcount
        retweetdic['retweetscreenname'] = retweetscreenname
        retweetdic['retweetname'] = retweetname
        retweetdic['retweettext'] = retweetname
        retweetdic['retweetsentiment'] = findsentiment(retweettext)
        parseddic['toptweets'] = retweetdic

    #######------text sentiment
    if tweet.has_key('text'):
        text = tweet['text'].encode('utf-8',errors = 'ignore')
        parseddic['text'] = text
        sentiment = findsentiment(text)
        parseddic['sentimentscore'] = sentiment
        parseddic['positivesentiment'] = 0
        parseddic['negativesentiment'] = 0
        parseddic['neutralsentiment'] = 0

        if sentiment > 1.5:
            parseddic['positivesentiment'] = 1
        elif sentiment < -0.5:
            parseddic['negativesentiment'] = 1
        elif (sentiment > -0.5 ) and (sentiment <= 0.5 ):
            parseddic['neutralsentiment']  = 1

    ######----Hashtags----
    if tweet.has_key('entities'):
        res1 = tweet['entities']
        taglist = res1["hashtags"]
        hashtaglist = []
        for tagitem in taglist:
            hashtaglist.append(tagitem['text'])
        parseddic['hashtags'] = hashtaglist


############------------aNALYSE TWEET FUNCTION-------
def analyzeTweet(tweetdic):
    text = tweetdic['text']
    text = text.lower()
    if not aggregatedic.has_key(keyword):
        valuedic = {'totaltweets': 0, 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0,'hashtags':{},'toptweets':{},'totalretweets':0,'hourlyaggregate':{
            '0': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '1': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '2': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '3': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '4': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '5': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '6': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '7': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '8': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '9': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '10': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '11': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '12': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '13': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '14': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '15': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '16': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '17': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '18': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '19': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '20': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '21': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '22': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},
            '23': {'totaltweets': 0 , 'positivesentiment':0, 'negativesentiment':0,'neutralsentiment':0},}}
        aggregatedic[keyword] = valuedic

    ######--------Counts---
    valuedic = aggregatedic[keyword]
    valuedic['totaltweets']+=  1
    valuedic['positivesentiment']+= tweetdic['positivesentiment'];
    valuedic['negativesentiment']+= tweetdic['negativesentiment'];
    valuedic['neutralsentiment']+= tweetdic['neutralsentiment'];
    ####-----Hourly aggregate
    hour = tweetdic['hour']
    valuedic['hourlyaggregate'][hour]['positivesentiment']+= tweetdic['positivesentiment']
    valuedic['hourlyaggregate'][hour]['negativesentiment']+= tweetdic['negativesentiment']
    valuedic['hourlyaggregate'][hour]['neutralsentiment']+= tweetdic['neutralsentiment']
    valuedic['hourlyaggregate'][hour]['totaltweets']+= 1

    ####------Top hashtags
    tagsdic =  valuedic['hashtags']
    for tag in tweetdic['hashtags']:
        if tagsdic.has_key(tag):
            tagsdic[tag]+=1
        else:
            tagsdic[tag] =1

    ####-------Top tweets----
    if tweetdic.has_key('toptweets'):
        if tweetdic['toptweets'].has_key('retweetscreenname'):
            toptweetsdic = valuedic['toptweets']
            retweetkey = tweetdic['toptweets']['retweetscreenname']

            if toptweetsdic.has_key(retweetkey):
                toptweetsdic[retweetkey]['retweetcount'] = tweetdic['toptweets']['retweetcount']
            else:
                toptweetsdic['retweetkey'] = tweetdic['toptweets']
    ######-----Aggregate-----
    aggregatedic[keyword] = valuedic

#####---------Post processing work----
def postProcessing():
    print(aggregatedic)
    valuedic = aggregatedic[keyword]

    ######_----------Top 10 hashtags-------

    keysdic = valuedic['hashtags']
    sortedkeysdic = OrderedDict(sorted(keysdic.items(),key= lambda x:x[1],reverse= True))
    tophashtagsdic ={}
    i=0
    for items in sortedkeysdic:
        if i>9:
            break
        i+=1
        tophashtagsdic[items] = keysdic[items]
    valuedic['hastags'] = tophashtagsdic

    #####------------Total retweets & Top 10 tweets------------
    toptweetsdic = valuedic['toptweets']

    for key in toptweetsdic:
        valuedic['totalretweets'] += toptweetsdic[key]['retweetcount']

    sortednames = sorted(toptweetsdic, key = lambda x:toptweetsdic[x]['retweetcount'],reverse= True)
    sortedtoptweetsdic = OrderedDict()
    i=0
    for k in sortednames:
        if i>99:
            break
        i+=1
        sortedtoptweetsdic[k] = toptweetsdic[k]
    valuedic['toptweets'] = sortedtoptweetsdic

    ######--------Key  for MongoDb--------
    valuedic['_id'] = str(str(date.today())+"/"+keyword)
    valuedic['metadata'] = {'date':str(date.today()),'key':keyword}

    #######-------------Inserting in MongoDb-----------
    print(valuedic)
    print("inserting data to MongoDb")
    postid = db.myapp_micollection.insert(valuedic)

#############3------------Main()----------------###########
def main():
    print("Setting up consumer")
    setupConsumer()
    print("Completed consumer setup")

    ####------no of tweets to consume------
    consumerCount =100

    print("Consuming "+str(consumerCount)+ " feeds")

    consumerCount = consumerCount/10
    for i in range(consumerCount):
        rs = conn.receive_message(q, 10, attributes='All')
        if len(rs) >0:
            for m in rs:
                post = m.get_body()
                deserializedpost = pickle.loads(post)
                postdic = json.loads(deserializedpost)
                parseTweet(postdic)
                analyzeTweet(parseddic)

        conn.delete_message_batch(q,rs)

    queuecount = q.count()
    print("Remaining queue count = "+ str(queuecount))
    print("Completed consuming....")
    print("Starting post processing...")
    postProcessing()
    print("Completed post processing....")
    print("Done")

#######--------Entry Point------#####
if __name__ == '__main__':
    main()
