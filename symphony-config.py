import datetime
now = datetime.datetime.now()
ymd=now.strftime("%y%m%d")

#Use the fully qualified directory for this. 
#For example use "/home/symphony/log/"+ymd+"/symphony.debug.log" instead of 
#debuglogfile ="symphony.debug.log"
debuglogfile ="symphony.debug.log"

errorlogfile ="symphony.error.log"
infologfile ="symphony.info.log"
my_cert= "bot.user1-cert.pem"
my_key= "bot.user1-key.pem"
my_plain_key = "bot.user1-plainkey.pem"
path_for_bot_file = "/tmp"
#This one is bot.user1
bot_symphony_id = <your bot's symphony id here>
#USING DB 1 for redis
redis_db=1

#NEW STUFF
redis_port=<your redis-port>
redis_host='localhost'

#URLS
#Replace acme with your url. 

session_auth_url="https://acme-api.symphony.com:8444/sessionauth/v1/authenticate"
key_manager_auth_url="https://acme-api.symphony.com:8444/keyauth/v1/authenticate"
datafeed_create_url="https://acme.symphony.com/agent/v4/datafeed/create"
logout_url="https://acme-api.symphony.com/sessionauth/v1/logout"
accept_conn_url="https://acme.symphony.com/pod/v1/connection/accept"
im_create_url="https://acme.symphony.com/pod/v1/im/create"
health_check_url="https://acme.symphony.com/agent/v2/HealthCheck"
find_users_url="https://acme.symphony.com/pod/v1/user"

get_connections_url="https://acme.symphony.com/pod/v1/connection/list?status=all"
stream_list_url="https://acme.symphony.com/pod/v1/streams/list?limit=5000"
register_presence_url="https://acme.symphony.com/pod/v1/user/presence/register"

get_presence_slug="https://acme.symphony.com/pod/v3/user/"
read_feed_slug="https://acme.symphony.com/agent/v4/datafeed/"
send_message_slug="https://acme.symphony.com/agent/v4/stream/"
