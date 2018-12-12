"""
Symphony Bot to listen and send messages
By - Harmeet Goindi
@ Trade Alert
"""
import logging
from  logging.handlers import RotatingFileHandler
import symphony_config as cfg
import redis
import SymphonyClass 
import time

logger = logging.getLogger(__name__)
formatter=logging.Formatter('[%(funcName)s:%(lineno)d] [%(levelname)s %(asctime)s] %(message)s')
debughandler = RotatingFileHandler(cfg.debuglogfile,maxBytes=20971520,backupCount=10)
debughandler.setLevel(logging.DEBUG)
debughandler.setFormatter(formatter)
logger.addHandler(debughandler)

infohandler = RotatingFileHandler(cfg.infologfile,maxBytes=20971520,backupCount=10)
infohandler.setLevel(logging.INFO)
infohandler.setFormatter(formatter)
logger.addHandler(infohandler)

errorhandler = RotatingFileHandler(cfg.errorlogfile,maxBytes=5242880,backupCount=10)
errorhandler.setLevel(logging.ERROR)
errorhandler.setFormatter(formatter)
logger.addHandler(errorhandler)
logger.setLevel(logging.DEBUG)

r = redis.StrictRedis(host=cfg.redis_host, port=cfg.redis_port, db=cfg.redis_db)

symph_instance=SymphonyClass.SymphonyBot(cfg,logger,r)
symph_instance.get_stream_list()
symph_instance.register_presence_interest()
symph_instance.get_user_buddy_request_status()
symph_instance.write_presence_file()
old_time=time.time()
while True:
    symph_instance.read_stream()
    if time.time()-old_time>300:
        symph_instance.get_stream_list()
        symph_instance.register_presence_interest()
        symph_instance.get_user_buddy_request_status()
        symph_instance.write_presence_file()
        old_time=time.time()

