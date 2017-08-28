#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import json
import sys
import os
import time
import uuid
import paho.mqtt.client as mqtt
import uuid
import socket
from  multiprocessing  import Pool
import math
import random
from docopt import docopt

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


#发送消息的用户id，可以根据实际情况，构造出来
from_users=[123456,4567,565656,34433]
#接收消息的id，可以根据实际情况，构造出来
to_users=[123,3434,34343,3443434]

#将消息分成了两部分
#消息说明
real_from_msg="""{\"from\":{\"userId\":\"12743\",\"nickName\":\"张艳丽A\",\"orgId\":\"13967\",\"orgName\":\"\",\"portrait\":\"\",\"groupId\":\"13967001\",\"groupName\":\"......\",\"groupType\":\"1\"},\"to\":\"13530\",\"messageType\":\"1\",\"contentType\":\"1\",\"sendTime\":\"1503283563070\",\"messageId\":\"89ac3961-0ed7-4c19-8349-1dd3b59bf7b8\",\"contentBuffer\":\"\"}"""

#消息业务内容
real_contentBuffer_msg={
	"1":"""{\\\"contentType\\\":\\\"text/plain\\\",\\\"text\\\":\\\"111111111\\\"}\"}""",
	"2":"""{\\\"contentType\\\":\\\"text/plain\\\",\\\"text\\\":\\\"111111111\\\"}\"}""",
	"3":"""{\\\"contentType\\\":\\\"text/plain\\\",\\\"text\\\":\\\"111111111\\\"}\"}""",
	"4":"""{\\\"contentType\\\":\\\"text/plain\\\",\\\"text\\\":\\\"111111111\\\"}\"}""",
	}


#构造发送给server端的消息（mqtt的payload数据）
def get_publish_message(from_user_nums,from_user_id):
	
	publish_msg=json.loads(real_from_msg)
	publish_msg["from"]["userId"]=str(from_user_id)
	publish_msg["to"]=str(random.choice(to_users[0:from_user_nums]))
	msg_type=random.choice("1234")
	publish_msg["messageType"]=msg_type
	publish_msg["messageId"]=str(uuid.uuid5(uuid.NAMESPACE_DNS,"test"))
	publish_msg["sendTime"]=str(math.trunc(time.time()*1000))
	publish_msg["contentBuffer"]=real_contentBuffer_msg[msg_type]
	
	
	

	return json.dumps(publish_msg,ensure_ascii=False).encode(encoding="utf-8")



#mqtt client 的缺省设置
def_config = {
	'host':'127.0.0.1',
	'port':1883,
    'keepalive': 30,
    'qos': 1,
    'retain': False,
    'auto_reconnect': True,
    'reconnect_max_interval': 10,
    'reconnect_retries': 2,
}

#定制mqtt协议的callback函数
def on_connect(client, userdata, flags, rc):
	pass

def on_disconnect(client,userdata,rc):
	
	#client.reconnect()
	pass

def on_message_subscribe(client,userdata,message):
	print("收到消息===：%s" % message.payload.decode(encoding="utf-8"))

def on_publish(client, userdata, mid):
	pass



def on_subscribe(client, userdata, mid, granted_qos):
	pass

def on_unsubscribe(client, userdata, mid):
	pass

def on_log(client, userdata, level, buf):
	
	print(str(os.getpid())+"=======:"+buf)





def publish_Ext(from_user_nums,from_userid,topic,max_msgs=None,interval=0):
	
	client_id="pub_"+socket.gethostname()+str(from_userid)
	client=mqtt.Client(client_id=client_id, clean_session=False, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
	client.enable_logger(logger)
	#client.disable_logger()
	client.username_pw_set(str(from_userid), "123456")
	client.on_log = on_log
	client.on_disconnect = on_disconnect
	client.connect(def_config["host"], def_config["port"], keepalive=def_config["keepalive"])
	
	if max_msgs:
		
		while max_msgs>0:
			payload_message=get_publish_message(from_userid,from_user_nums)
			result = client.publish(topic,payload=payload_message,qos=1,retain=def_config["retain"])
			max_msgs=max_msgs-1
			time.sleep(interval)
	else:
		while True:
			try:
				payload_message=get_publish_message(from_userid)
				result = client.publish(topic,payload=payload_message,qos=1,retain=False)
				time.sleep(interval)
			except KeyboardInterrupt as ki:
				break
	client.disconnect()
	return  (client_id,result.mid)


def run_publish_Ext(from_user_nums,topic,publish_max_msgs=None,interval=0,duration=None):
	pool= Pool()
	results=[]
	published_message=0
	for i in from_users[0:from_user_nums]:
		results.append(pool.apply_async(publish_Ext,args=(from_user_nums,i,topic,publish_max_msgs,interval)))
	pool.close()
	
	if duration:
	
		pool.join(duration)
	else:
		pool.join()
	for res in results:
		print(res.get()[0]+u"发送消息数"+str(res.get()[1]))
		published_message = published_message + res.get()[1]
	print(u"publish总数 %d" % published_message)
		

		



def subscribe_Ext(to_userid,topic,qos=def_config["qos"]):
	
	client_id="sub_"+socket.gethostname()+str(to_userid)
	client=mqtt.Client(client_id=client_id, clean_session=False, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
	#client.enable_logger(logger)
	client.disable_logger()
	client.username_pw_set(str(to_userid), "123456")
	client.on_log = on_log
	client.on_message = on_message_subscribe
	client.on_disconnect = on_disconnect
	client.connect(def_config["host"], def_config["port"], keepalive=def_config["keepalive"])
	#若不是接收消息方，不是subsribe，可将这条注释掉
	client.subscribe(topic,qos)
	client.loop_forever()




def run_subscribe_Ext(to_user_nums,topic,qos,duration=None):
	pool= Pool()
	results=[]
	for i in to_users[0:to_user_nums]:
		results.append(pool.apply_async(subscribe_Ext,args=(i,topic,qos)))
	pool.close()
	if duration:
		print(duration)
		pool.join()
	else:
		pool.join()
	

if __name__=="__main__":
	
	
	#接收消息，并发用户数，topc，qos_level
	run_subscribe_Ext(5,"1/#",1)
	#发送消息,并发用户数，topic ，总的消息数，发送间隔（秒）
	#run_publish_Ext(5,"1/1",1000,0)
	
	
   
	
