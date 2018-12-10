"""
    Symphony Class Module to listen and send messages
    By - Harmeet Goindi
    @ Trade Alert
    """

import os
import time
import csv
import json
import asyncio
import random
import re
import requests
import aiohttp
from bs4 import BeautifulSoup

class SymphonyBot():
    """
    Symphony Bot to listen and send messages
    By - Harmeet Goindi
    @ Trade Alert
    """
    # pylint: disable=too-many-instance-attributes
    # Three is reasonable in this case.
    # pylint: disable=line-too-long
    # pylint: disable=broad-except
    # pylint: disable=missing-docstring
    def __init__(self, config_dict, logger, redis_instance):
        self.cfg = config_dict
        self.bot_symphony_id = self.cfg.bot_symphony_id
        self.symphony_certificate_tuple = (self.cfg.my_cert, self.cfg.my_plain_key)
        self.path_for_message_files = self.cfg.path_for_bot_file
        self.redis_instance = redis_instance
        self.logger = logger
        self.user_list = list()
        self.session_token = self.redis_instance.get('symphony_session_token') if self.redis_instance.get('symphony_session_token') else self.get_session_token()
        self.km_token = self.redis_instance.get('symphony_km_token') if self.redis_instance.get('symphony_km_token') else self.get_key_manager_token()
        self.feed_id = self.redis_instance.get('symphony_main_feed_id') if self.redis_instance.get('symphony_main_feed_id') else self.create_symphony_stream()

    def get_session_token(self):
        """
        Gets session token from symphony.
        """
        try:
            my_response = requests.post(self.cfg.session_auth_url, cert=self.symphony_certificate_tuple, headers={"cache-control":"no-cache"})
            if my_response.ok:#ok mean 200
                json_response = json.loads(my_response.content)
                self.session_token = json_response["token"]
                self.redis_instance.set('symphony_session_token', self.session_token)
                return True
            #In case of error
            self.logger.error("Error in get_session_token - %s"%(my_response))
            return False
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False

    def get_key_manager_token(self):
        """
        Gets key manager token from symphony.
        """
        try:
            my_response = requests.post(self.cfg.key_manager_auth_url, cert=self.symphony_certificate_tuple, headers={"cache-control":"no-cache"})
            if my_response.ok:#ok mean 200
                json_response = json.loads(my_response.content)
                self.km_token = json_response["token"]
                self.redis_instance.set('symphony_km_token', self.km_token)
                return True
            #else error
            self.logger.error("Error in get_key_manager_token - %s"%(my_response))
            return False
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            print(request_exception)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False

    def session_logout(self):
        """
        Logs out from symphony.
        """
        try:
            my_response = requests.post(self.cfg.logout_url, cert=self.symphony_certificate_tuple, headers={"cache-control":"no-cache", "sessiontoken":self.session_token})
            if my_response.ok:#ok mean 200
                return True
            #else error
            self.logger.debug("Error in session_logout - %s"%(my_response))
            return False
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            print(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False

    def create_symphony_stream(self):
        """
        Creates symphony data feed
        """
        try:
            my_response = requests.post(self.cfg.datafeed_create_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token, "keyManagerToken":self.km_token})
            print("sessiontolen", my_response.text)
            if my_response.ok:#ok mean 200
                json_response = json.loads(my_response.content)
                self.feed_id = json_response["id"]
                self.redis_instance.set('symphony_main_feed_id', str(self.feed_id))
                self.logger.debug("feed id created %s"%json_response)
                return True
            #else error
            self.logger.error("Error in create_stream - %s"%(my_response))
            return False
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            print(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False

    def process_connection_request(self, userid):
        """
        Processes connection request to the bot from users. Takes symphony user id as an argument
        Here you can include logic for access - accept or deny based on user
        """
        params = '{"userId":%s}'%userid
        try:
            my_response = requests.post(self.cfg.accept_conn_url, cert=self.symphony_certificate_tuple, data=params, headers={"sessiontoken":self.session_token, "Content-Type": "application/json"})
            if my_response.ok:  #Creating stream
                if self.create_user_stream(userid):
                    #Send Welcome Message
                    message = 'Welcome to Option Alert.<br/>Current subscribers please visit <a href="https://whatstrading.trade-alert.com/accounts/change_im_service/">our site to update your account with IM Service Symphony </a>and your ScreenName ID <b>%s.</b> <hr/> New users can now try out our service in Demo mode. <br/> Type any stock ticker, sector code, or option to get a recap menu. <br/>Examples include AAPL, @FIN, or SPY Jun 230 put. <br/>Demo mode recaps are limited to one page of prior day data. '%userid
                    welcome_thing = ("%s\t%s")%(userid, message)
                    filename = '%s/symphony-out__117'%(self.path_for_message_files)
                    if not os.path.exists(self.path_for_message_files):
                        os.makedirs(self.path_for_message_files)
                    file_opened = open(filename, 'w')
                    file_opened.write(welcome_thing)
                    file_opened.close()
            #else error
            self.logger.error("Error in processing connection request - %s"%(my_response.content))
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            print(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)

    def create_user_stream(self, userid):
        """
        Creates a stream to send message to user. Takes symphony userid
        """
        userid_data = '[%s]'%userid
        try:
            im_create_response = requests.post(self.cfg.im_create_url, cert=self.symphony_certificate_tuple, data=userid_data, headers={"sessiontoken":self.session_token, "Content-Type": "application/json"})
            if im_create_response.ok:
                json_response = json.loads(im_create_response.content)
                my_key = 's_user:%s'%userid
                my_user = dict(fname="Not Known Yet", lname="Not Known Yet", stream=json_response["id"])
                self.redis_instance.hmset(my_key, my_user)
                self.redis_instance.hset("symphony_user_map", userid, my_user)
                details = self.find_user_by_id(userid)
                try:
                    my_user.fname = details["firstname"]
                    my_user.lname = details["lastname"]
                    self.redis_instance.hmset(my_key, my_user)
                    self.redis_instance.hset("symphony_user_map", userid, my_user)
                except KeyError:
                    return True
                return True
            #else error
            self.logger.error("Error in creating user stream - %s"%im_create_response.content)
            return False
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            print(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False

    def check_health(self):
        """
        Check health of the bot. Do this periodically or when you see errors
        """
        try:
            my_response = requests.get(self.cfg.health_check_url, cert=self.symphony_certificate_tuple, headers={"cache-control":"no-cache", "sessiontoken":self.session_token, "keyManagerToken":self.km_token})
            if my_response.status_code == 200:
                json_response = json.loads(my_response.text)
                print(json_response)
                if (json_response["podConnectivity"] and json_response["keyManagerConnectivity"] and json_response["encryptDecryptSuccess"]):
                    return True
                #else error
                return self.re_auth_key_sess()
            elif (my_response.status_code == 401) or (my_response.status_code == 403) or (my_response.status_code == 400):
                return self.re_auth_key_sess()
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            print(request_exception)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False
        return False

    def re_auth_key_sess(self):
        """
        Re authorization in case of error(403 etc).
        Just calls sessin token and key manager token functions.
        """
        if (not self.get_session_token() or not self.get_key_manager_token()):
            self.logger.error("Health Check failure - Keys and Token issue")
            return False
        # No error so continue
        try:
            my_response_2 = requests.get(self.cfg.health_check_url, cert=self.symphony_certificate_tuple, headers={"cache-control":"no-cache", "sessiontoken":self.session_token, "keyManagerToken":self.km_token})
            if (my_response_2.status_code == 401) or (my_response_2.status_code == 403):
                self.logger.error("Error in check health. Keys Token are fine - %s"%(my_response_2.content))
                return False
            return True
        except requests.exceptions.RequestException as request_exception:
    # catastrophic error.
            self.logger.error(request_exception)
            print(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False

    def find_user_by_id(self, uid):
        """
        A snippet to find user by symphony id
        """
        find_url = "%s?uid=%s&local=false"%(self.cfg.find_users_url, str(uid))
        my_response = requests.get(find_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token})
        print(my_response.text)
        return json.loads(my_response.text)

    def find_user_by_email(self, email):
        """
        A snippet to find user by email
        """
        find_url = "%s?email=%s&local=false"%(self.cfg.find_users_url, str(email))
        my_response = requests.get(find_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token})
        print(my_response.text)
        return json.loads(my_response.text)


    def find_user_by_uname(self, uname):
        """
        A snippet to find user by username
        """
        find_url = "%s?username=%s&local=false"%(self.cfg.find_users_url, str(uname))
        my_response = requests.get(find_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token})
        print(my_response.text)
        return json.loads(my_response.text)

    def find_users(self, qry):
        """
        A snippet to find user by whatever you have on hand
        """
        query_dict = dict(query=str(qry))
        find_url = "%s/search?local=false&limit=200"%self.cfg.find_users_url
        try:
            my_response = requests.post(find_url, cert=self.symphony_certificate_tuple, data=json.dumps(query_dict), headers={"sessiontoken":self.session_token, "Content-Type": "application/json"})
            if my_response.ok:
                json_response = json.loads(my_response.text)
                for usr in json_response["users"]:
                    ret_str = "%s"%str(usr["id"])
                    try:
                        usr["firstName"]
                        ret_str = "%s: %s"%(ret_str, usr["firstName"])
                    except KeyError:
                        pass
                    try:
                        usr["lastName"]
                        ret_str = "%s: %s"%(ret_str, usr["lastName"])
                    except KeyError:
                        pass
                    try:
                        usr["displayName"]
                        ret_str = "%s: %s"%(ret_str, usr["displayName"])
                    except KeyError:
                        pass
                    try:
                        usr["company"]
                        ret_str = "%s: %s"%(ret_str, usr["company"])
                    except KeyError:
                        pass
                    print(ret_str)
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            print(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)

    def list_users_in_memory(self):
        """
        A snippet to find user in redis db. Internal
        """
        for i in self.redis_instance.keys("s_user*"):
            print("%s - %s"%(i, self.redis_instance.hgetall(i)))

    def read_stream(self):
        """
        This function reads the main symphony stream for incoming messages.
        The code is very blocky here and needs refactoring
        The incoming messages are parsed and written to files for internal ssystem to pick up and respond.
        Could be done better via internal sockets.
        """
        incoming_cmd_stream = ""
        feed_read_url = self.cfg.read_feed_slug+str(self.feed_id)+"/read"
        try:
            my_response = requests.get(feed_read_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token, "keyManagerToken":self.km_token})
            #print(my_response.text)
            if my_response.status_code == 200:#ok mean 200
                json_response = json.loads(my_response.content)
                for i in json_response:
                    if not i:
                        continue
                    try:
                        if i["type"] == "MESSAGESENT":
                            try:
                                usr = i["initiator"]["user"]
                                recipient_usr = "Don't Know"
                                if i["payload"]["messageSent"]["message"]["user"]["userId"]:
                                    usr_sent = i["payload"]["messageSent"]["message"]["user"]
                                    recipient_usr = "To-%s: %s"%(str(usr_sent["userId"]), str(usr_sent["displayName"]))
                                my_str = "From-%s: %s: %s"%(str(i["type"]), str(usr["userId"]), str(usr["displayName"]))
                                self.logger.debug("%s, %s, %s"%(my_str, recipient_usr, i["payload"]["messageSent"]["message"]["message"]))
                            except KeyError as err:
                                self.logger.error("Key Error %s in one of the following - type, initiator, displayname, userId- %s"%(err, i))
                            if i["initiator"]["user"]["userId"] != self.bot_symphony_id:
                                try:
                                    incoming_cmd = self.process_message(i)
                                    u_info = "%s, %s, %s, %s"%(str(usr["firstName"]), str(usr["lastName"]), str(usr["email"]), str(usr["displayName"]))
                                except KeyError as err:
                                    self.logger.error("Key Error %s in one of the following - firstName, lastName, email, displayname- %s"%(err, i))
                                if incoming_cmd:
                                    incoming_cmd_stream = ("%s\t%s\n%s")%(incoming_cmd, u_info, incoming_cmd_stream)
                        elif i["type"] == "CONNECTIONREQUESTED":
                            print(i)
                            self.process_connection_request(i["initiator"]["user"]["userId"])
                        else:
                            self.logger.debug("Other message types - %s"%i)
                    except KeyError as err:
                        self.logger.error("Key Error %s i['type']- %s"%(err, i))
                        print("Key Error %s i['type']- %s"%(err, i))
                        
                if incoming_cmd_stream != "":
                    print(incoming_cmd_stream)
                    rnd_ext = random.random()*1000000000000
                    tmpfilename = '%s/_symphony.incoming.%f'%(self.path_for_message_files, rnd_ext)
                    filename = '%s/symphony.incoming.%f'%(self.path_for_message_files, rnd_ext)
                    #os.remove(filename)
                    if not os.path.exists(self.path_for_message_files):
                        os.makedirs(self.path_for_message_files)
                    file_opened = open(tmpfilename, 'w')
                    incoming_cmd_stream = incoming_cmd_stream.replace(u'\xa0', u' ')
                    file_opened.write(incoming_cmd_stream)
                    file_opened.close()
                    os.rename(tmpfilename, filename)
                return True
            elif (my_response.status_code == 401) or (my_response.status_code == 301) or (my_response.status_code == 403):
                self.logger.error(my_response.text)
                self.redis_instance.delete("symphony_session_token")
                self.redis_instance.delete("symphony_km_token")
                self.re_auth_key_sess()
                return False

            elif my_response.status_code != 204:
                self.logger.info(my_response.content.decode())
                self.logger.error("Error in read_stream - %s"%(my_response.content.decode()))
                if self.check_health():
                    self.create_symphony_stream()
                return False
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            print(request_exception)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False
        return False

    def process_message(self, json_msg):
        """
        This function is called by read_stream() to parse the message and make an entry in redis.
        """
        try:
            if json_msg["type"] == "MESSAGESENT":
                sender = json_msg["initiator"]["user"]["userId"]
                my_key = 's_user:%s'%sender
                my_user = dict(fname=json_msg["initiator"]["user"]["firstName"], lname=json_msg["initiator"]["user"]["lastName"], stream=json_msg["payload"]["messageSent"]["message"]["stream"]["streamId"])
                self.redis_instance.hmset(my_key, my_user)
                self.redis_instance.hset("symphony_user_map", sender, my_user)
                soup = BeautifulSoup(json_msg["payload"]["messageSent"]["message"]["message"], 'html.parser')
                text_val = soup.get_text()
                if text_val == "":
                    text_val = "+"
                my_str = "%s: %s: CMD typed: %s"%(sender, str(json_msg["initiator"]["user"]["displayName"]), text_val)
                self.logger.info(my_str)
                return "%s\t%s"%(sender, text_val)
            return None
        except KeyError as err:
            self.logger.error("Key error %s %s"%(err, json_msg))
            print("Key error %s %s"%(err, json_msg))
            return None
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return None

    def get_user_buddy_request_status(self):
        """
        This function looks at incoming requests from user and accepts or denies (in my case no denial :) )
        """
        try:
            my_response = requests.get(self.cfg.get_connections_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token})
            if my_response.ok:#ok mean 200
                json_response = json.loads(my_response.content)
                for i in json_response:
                    try:
                        if i["status"] == "PENDING_INCOMING":
                            self.process_connection_request(i["userId"])
                        elif i["status"] == "ACCEPTED":
                            self.logger.debug(str(i["userId"])+" has been accepted")
                    except KeyError as err:
                        self.logger.error("Key error %s in i['status'] pending buddy request - %s"%(err, i))
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            print(request_exception)
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)

    def get_stream_list(self):
        """
        Gets all the streams that the bot is talking to and stores in local redis db.
        """
        params = '{"streamTypes":[{"type": "IM"}]}'
        try:
            my_response = requests.post(self.cfg.stream_list_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token, "Content-Type": "application/json"}, data=params)
            
            if my_response.ok:#ok mean 200
                del self.user_list[:]
                #self.redis_instance.delete("stream")
                json_response = json.loads(my_response.content)
                self.logger.debug(json_response)
                for stream_details in json_response:
                    try:
                        for i in stream_details['streamAttributes']['members']:
                            if i != self.bot_symphony_id:
                                self.user_list.append(i)
                                my_key = 's_user:%s'%i
                                stream_to_send = self.redis_instance.hget(my_key, 'stream')
                                if not stream_to_send:
                                    my_user = dict(fname="Not Known Yet", lname="Not Known Yet", stream=stream_details["id"])
                                    self.redis_instance.hmset(my_key, my_user)
                                    self.redis_instance.hset("symphony_user_map", i, my_user)
                                    #Now fill in data
                                    details = self.find_user_by_id(i)
                                    try:
                                        my_user.fname = details["firstname"]
                                        my_user.lname = details["lastname"]
                                        self.redis_instance.hmset(my_key, my_user)
                                        self.redis_instance.hset("symphony_user_map", i, my_user)
                                    except KeyError as err:
                                        self.logger.error("Key Error %s in finding user by id %s"%(err, i))
                    except KeyError as err:
                        self.logger.error("Key Error %s in getting all stream ids %s"%(err, stream_details))
            elif (my_response.status_code == 401) or (my_response.status_code == 403) or (my_response.status_code == 301):
                self.logger.error(my_response.text)
                self.redis_instance.delete("symphony_session_token")
                self.redis_instance.delete("symphony_km_token")
                self.re_auth_key_sess()
            else:
                self.logger.error("List of user in my stream error %s"%my_response.text)
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            print(request_exception)
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)

    def register_presence_interest(self):
        """
        Registers presence for all users. If user is not connected anymore ignore her.
        """
        try:
            my_response = requests.post(self.cfg.register_presence_url, cert=self.symphony_certificate_tuple, data=json.dumps(self.user_list), headers={"sessiontoken":self.session_token, "Content-Type": "application/json", "cache-control":"no-cache"})
            if not my_response.ok:
                self.logger.error("Registering presence interest returned %s. \n Deleteing user from presence file"%my_response.text)
                #self.user_list.remove(userid)
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            print(request_exception)
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)

    def get_presence(self, userid):
        """
        Get presence of user. 
        """
        presence_url = self.cfg.get_presence_slug+"/%s/presence"%str(userid)
        try:
            my_response = requests.get(presence_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token, "cache-control":"no-cache"})
            json_response = json.loads(my_response.text)
            self.logger.debug(json_response)
            if my_response.ok:
                return {"error":"none", "code":json_response['category']}
            #error else:
            self.logger.error("Error in getting presence%s"%json_response)
            return {"error":"connection_error", "code":json_response['code']}
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            print(request_exception)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False

    def write_presence_file(self):
        """
        Write presend to a file for my process to read. You can send this via socket
        """
        column_of_user = ""
        for userid in self.user_list:
            presence_status = self.get_presence(userid)
            if (presence_status["error"] != "connection_error" and presence_status["code"] != "OFFLINE"):
                column_of_user += str(userid)+"\n"
            elif presence_status["code"] == 403:
                self.logger.debug("Got a 403 error which means the user is not connected to us. Deleting %s user from the list and adding to bad user id list in redis"%userid)
                self.user_list.remove(userid)
        tmpfilename = '%s/_symphony.buddylist'%self.path_for_message_files
        filename = '%s/symphony.buddylist'%self.path_for_message_files
        if not os.path.exists(self.path_for_message_files):
            os.makedirs(self.path_for_message_files)
        file_opened = open(tmpfilename, 'w')
        file_opened.write(column_of_user)
        file_opened.close()
        os.rename(tmpfilename, filename)

    async def send_message_asynchronously(self, stream_id, formatted_msg, file_name=None):
        """
        This is the crux of sending fast messages. Async function with await on post to symphony. Post response takes 300ms so in that time other messages can be send. Not possible without async
        """
        try:
            send_message_url = self.cfg.send_message_slug+"%s/message/create"%stream_id.decode()
        except AttributeError as stream_error:
            self.logger.error("Error %s in stream id %s. Content are %s "%(stream_error, stream_id, formatted_msg))
        try:
            formdata = aiohttp.FormData()
            formdata.add_field(name='message', value=formatted_msg, content_type='multipart/form-data')
            headers = {"sessiontoken":self.session_token.decode(), "keyManagerToken":self.km_token.decode()}
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.post(send_message_url, data=formdata) as my_response:
                    try:
                        json_response = await my_response.json()
                        if my_response.status == 200:
                            self.logger.debug("File data message sent %s"%file_name)
                        elif (my_response.status == 401)  or (my_response.status == 403) or (my_response.status == 301):
                            self.logger.error("Error in sending message  - %s. Original message was -%s"%(json_response, formatted_msg))
                            self.redis_instance.delete("symphony_session_token")
                            self.redis_instance.delete("symphony_km_token")
                            self.re_auth_key_sess()
                        else:
                            ##Some of it is for debug and should be removed in prd
                            self.logger.debug("Error in sending message  - %s. Original message was -%s"%(json_response, formatted_msg))
                            err_sg_debug = '%s'%json_response["message"]
                            err_str_debug = re.sub('\W+', ' ', err_sg_debug)
                            err_message = "<messageML>%s</messageML>"%err_str_debug
                            data = {'message':err_message}
                            my_response = requests.post(send_message_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token, "keyManagerToken":self.km_token}, files=data)
                    except aiohttp.client_exceptions.ContentTypeError as content_exception:
                        self.logger.error(content_exception)
                        print(content_exception)
                    except aiohttp.client_exceptions.ClientConnectorError as connection_error:
                        self.logger.error(connection_error)
                        print(connection_error)
                        time.sleep(0.001)
        except requests.exceptions.Timeout as timeout_exception:
            self.logger.error(timeout_exception)
            print(timeout_exception)
        # Maybe set up for a retry,  or continue in a retry loop
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            print(request_exception)
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)

    def send_message_slow(self, stream_id, formatted_msg, file_name=None):
        """
        This is the old implementation and is slow. Post response takes 300ms so in that time we wait and do nothing causing lag. 
        """
        try:
            send_message_url = self.cfg.send_message_slug+"%s/message/create"%stream_id.decode()
        except AttributeError as stream_error:
            self.logger.error("Error %s in stream id %s. Content are %s "%(stream_error, stream_id, formatted_msg))
        data = {'message':formatted_msg}

        try:
            my_response=requests.post(send_message_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token, "keyManagerToken":self.km_token}, files=data)
            json_response = json.loads(my_response.text)
            if my_response.ok:
                self.logger.debug("File data message sent %s"%file_name)
                return True
            elif (my_response.status_code == 401)  or (my_response.status_code == 403) or (my_response.status_code == 301):
                self.logger.error("Error in sending message  - %s. Original message was -%s"%(json_response, formatted_msg))
                self.redis_instance.delete("symphony_session_token")
                self.redis_instance.delete("symphony_km_token")
                self.re_auth_key_sess()
                return False
            else:
                ##Some of it is for debug and should be removed in prd
                self.logger.debug("Error in sending message  - %s. Original message was -%s"%(json_response, formatted_msg))
                err_sg_debug = '%s'%json_response["message"]
                err_str_debug = re.sub('\W+', ' ', err_sg_debug)
                err_message = "<messageML>%s</messageML>"%err_str_debug
                data = {'message':err_message}
                my_response = requests.post(send_message_url, cert=self.symphony_certificate_tuple, headers={"sessiontoken":self.session_token, "keyManagerToken":self.km_token}, files=data)
                return False
        except requests.exceptions.Timeout as timeout_exception:
            self.logger.error(timeout_exception)
            print(timeout_exception)
        # Maybe set up for a retry,  or continue in a retry loop
        except requests.exceptions.RequestException as request_exception:
        # catastrophic error.
            self.logger.error(request_exception)
            #Sleeping 1 second for max connection pool error
            time.sleep(1)
            print(request_exception)
            return False
        except Exception as catchall_exception:
            self.logger.error("Exception occured. Please debug :%s"%catchall_exception)
            print(catchall_exception)
            return False

    def read_files_and_send_reply(self):
        """
        This is where I read response from backend and send message to user.
        I use files (legacy reason) but sockets would be better.
        """
        loop = asyncio.get_event_loop()
        tasks = []
        fileslist = [("%s/%s")%(self.path_for_message_files, file_name) for file_name in os.listdir(self.path_for_message_files) if file_name.lower().startswith('symphony-out')]
        fileslist.sort(key=os.path.getmtime)
        if fileslist:
            for file_name in fileslist:
                #self.logger.debug("File %s last modified: %s" % (file_name, time.ctime(os.path.getmtime(file_name))))
                with open(file_name) as tsv_file:
                    try:
                        reader = csv.reader(tsv_file, delimiter='\t')
                        for u_id, message in reader:
                            my_key = 's_user:%s'%u_id
                            message = "<messageML>%s</messageML>"%message
                            stream_to_send = self.redis_instance.hget(my_key, 'stream') if self.redis_instance.hget(my_key, 'stream') else self.create_user_stream(u_id)
                            if stream_to_send is None:
                                self.logger.error("bad stream id for user %s"%u_id)
                                print("bad id %s"%u_id)
                                os.rename(file_name, file_name.replace("symphony", "errorsymph_badstream"))
                            else:
                                tasks.append(asyncio.ensure_future(self.send_message_asynchronously(stream_to_send, message, file_name)))
                                try:
                                    os.remove(file_name)
                                    self.logger.debug("At deleting file %s"%file_name)
                                except FileNotFoundError:
                                    self.logger.error("could not delete file %s"%file_name)
                    except FileNotFoundError as file_error:
                        self.logger.error("Should not happen. File just was deleted.  %s"%file_error)
                    except UnicodeDecodeError as reading_error:
                        self.logger.error("Unicode error %s in file %s. Content are %s "%(reading_error, file_name, tsv_file.read()))
                        newfilename = file_name.replace("symphony", "errorsymph_unicode")
                        os.rename(file_name, newfilename)
                    except ValueError as wrongbot_format:
                        self.logger.error("Value error %s in file %s. Content are %s  "%(wrongbot_format, file_name, tsv_file.read()))
                        newfilename = file_name.replace("symphony", "errorsymph_format")
                        os.rename(file_name, newfilename)
                    except Exception as catchall_exception:
                        self.logger.error("Some error %s in file %s. Content are %s  "%(catchall_exception, file_name, tsv_file.read()))
                        newfilename = file_name.replace("symphony", "errorsymph")
                        os.rename(file_name, newfilename)


            loop.run_until_complete(asyncio.gather(*tasks))
            #self.logger.debug("async loop done")
