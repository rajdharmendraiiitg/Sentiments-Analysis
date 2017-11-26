import boto
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from boto.sqs.message import Message
import pickle




ckey="Enter Your"
csecret="Enter Your"
atoken="Enter Your"
asecret="Enter Your"

ACCESS_KEY="Enter Your"
SECRET_KEY="Enter Your"

REGION="Enter Your"

conn = boto.sqs.connect_to_region(REGION,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
q= conn.get_all_queues(prefix = 'arsh-queue')

class StdOutListener(StreamListener):
    def on_data(self, data):
        msg = pickle.dumps(data)
        m = Message()
        m.set_body(msg)
        status = q[0].write(m)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    keywords = ['india']
    I = StdOutListener()
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    stream = Stream(auth,I)
    stream.filter(track=keywords)

