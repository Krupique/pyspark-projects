import requests
import configparser
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
import socket
import json
import re
import time

# read configs
config = configparser.ConfigParser()
config.read('conf/config.ini')

# Set up your credentials
api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']
bearer_token = ###bt

# Gainaing access and connecting to Twitter API using Credentials
client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
api = tweepy.API(auth)

search_terms = ["cat has:videos", 'dog has:videos', 'horse has:videos']

lst_text = []
# Bot searches for tweets containing certain keywords
class MyStream(tweepy.StreamingClient):

    def set_socket(self, csocket):
        self.client_socket = csocket

    def on_data(self, tweet):
        try:
            tweet = json.loads(tweet.decode('utf-8'))

            text = tweet['data']['text']

        
            #res = re.search(r'(?<=https).*',text)
            #if res != None:
            #    res = 'http' + res.group()
            #    print(res)
            print(text)

            self.client_socket.send( text.encode('utf-8') )

            return True

        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True


def sendData(c_socket):
    stream = MyStream(bearer_token=bearer_token)

    stream.set_socket(c_socket)

    for term in search_terms:
        stream.add_rules(tweepy.StreamRule(term))

    # Starting stream
    stream.filter(tweet_fields=["text"])


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print('Get rules')
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print('Delete all rules')
    print(json.dumps(response.json()))
    return response.json()


if __name__ == "__main__":
    s = socket.socket()         # Create a socket object
    host = "127.0.0.1"          # Get local machine name
    port = 5554                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.


    rules = get_rules()
    delete = delete_all_rules(rules)

    print( "Received request from: " + str( addr ) )

    sendData( c )