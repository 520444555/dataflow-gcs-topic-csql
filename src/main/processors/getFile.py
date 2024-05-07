import json
import logging

def getfile(message):
	#Decoding messgae
	message = message.decode('utf-8')
	json_object = json.loads(message)
	#getting bucket name
	bucket=json_object['bucket']
	file_name=json_object['name']
	full_name='gs://'+bucket+'/'+file_name
	logging.info('Pubsub file name ------------------: %s ', full_name)
	#return full name
	return full_name
