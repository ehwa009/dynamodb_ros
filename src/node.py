#!/usr/bin/python
#-*- encoding: utf8 -*-

import rospy
import boto3
import yaml

from mind_msgs.msg import Reply, RaisingEvents, DBQuery
from mind_msgs.srv import ReloadWithResult, ReadData, WriteData, GoogleEntity

class DynamoDBNode:
    
    def __init__(self):
        try:
            config_file = rospy.get_param('~config_file')
        except:
            rospy.logerr('please set param ~config_file.')
            exit(-1)

        with open(config_file) as f:
            data = yaml.load(f)
            print(data)

        dynamodb = boto3.resource('dynamodb',
            region_name=data['region'],
            aws_access_key_id=data['aws_access_key_id'],
            aws_secret_access_key=data['aws_secret_access_key'])

        self.table = dynamodb.Table(data['table'])

        rospy.Subscriber('db_query_events', DBQuery, self.handle_api_call)
        rospy.loginfo('%s initialized' %rospy.get_name())
        rospy.spin()

    def handle_api_call(self, msg):
        api_call = msg.api_number
        # req_entities = msg.entities
        entities = msg.entities

        if api_call == 1:
            resp = self.table.get_item(
                Key = {
                    'Name': entities
                }
            )
            item = resp['Item']
            print(item)

if __name__ == '__main__':
    rospy.init_node('dynamodb_node', anonymous=False)
    db = DynamoDBNode()
    