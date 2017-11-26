import boto
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from boto.sqs.message import Message
import pickle




ckey="gVFA2Dt05ZOUnE7BeNXHdpyIY"
csecret="AP4L4B4ivICBZKo8Uj0wuctX29jo3wvMoC3Uvbk0HoSYGe1kM8"
atoken="787712324748443648-dE134IpGAyHtoRJTruX3bXT6lgBlmRX"
asecret="ETQWCHxIl5qEiyBt4vLna5QuRxJxhlxqH2ccHEQIYW3Dr"

ACCESS_KEY="AKIAIGK5O2MQKLS5SZBA"
SECRET_KEY="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"

REGION="us-west-2"

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

