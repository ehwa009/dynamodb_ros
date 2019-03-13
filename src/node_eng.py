#!/usr/bin/python
#-*- encoding: utf8 -*-

import rospy
import boto3
import yaml
import json
import random

from boto3.dynamodb.conditions import Key, Attr
from mind_msgs.srv import DBQuery

LOCATION_TEXT = [
    "The {location} is {desc}.",
]
PRESCRIPTION_TEXT_FAIL = [
    '''I'm sorry {name}, your doctor has not yet written your prescription, and so it is not ready for collection at the moment.
    However, I have sent a message to your doctor. Once the prescription has been written, someone will call you and let you know.
    '''
]
PRESCRIPTION_TEXT_SUCCESS = [
    "Great! Here it is.", "Ok, Here it is."
]
FORGOT_DR_NAME = [
    "No problem {name}, I can see that you have an appointment with {dr_name} today and have checked you in"
]
WAITING_TIME = [
    "you are next to see {dr_name}, he will be around {waiting_time} more minutes."
]


class DynamoDBNode:
    
    def __init__(self):
        try:
            config_file = rospy.get_param('~config_file')
        except:
            rospy.logerr('please set param ~config_file.')
            exit(-1)

        with open(config_file) as f:
            data = yaml.load(f)
        dynamodb = boto3.resource('dynamodb',
            region_name=data['region'],
            aws_access_key_id=data['aws_access_key_id'],
            aws_secret_access_key=data['aws_secret_access_key'])

        self.table = dynamodb.Table(data['table'])
        # print(self.table.creation_date_time)

        self.srv_query_db = rospy.Service('%s/query_data'%rospy.get_name(), DBQuery, self.handle_api_call)

        rospy.loginfo('%s initialized' %rospy.get_name())
        rospy.spin()

    
    def handle_api_call(self, data):
        data = data.api_call
        res = data.split(' ')
        if res[1] == 'appointment':
            name = res[2] + ' ' + res[3]
            address = res[4] + ' ' + res[5] + ' ' + res[6]
            resp = self.get_data_with_attr(name, address)
            output_string = random.choice(FORGOT_DR_NAME)
            response = output_string.format(name=resp['Name'], dr_name=resp['DrName'])
        elif res[1] == 'location':
            resp = self.get_data(res[2])
            output_string = random.choice(LOCATION_TEXT)
            response = output_string.format(location=resp['Name'], desc=resp['Desc'])
        elif res[1] == 'prescription':
            name = res[2] + ' ' + res[3]
            address = res[4] + ' ' + res[5] + ' ' + res[6]
            resp = self.get_data_with_attr(name, address)
            if resp['Prescription'] is False:
                output_string = random.choice(PRESCRIPTION_TEXT_FAIL)
                response = output_string.format(name=resp['Name'])
            else:
                response = random.choice(PRESCRIPTION_TEXT_SUCCESS)
        elif res[1] == 'waiting_time':
            name = res[2] + ' ' + res[3]
            address = res[4] + ' ' + res[5] + ' ' + res[6]
            resp = self.get_data_with_attr(name, address)
            dr_info = self.get_data(resp['DrName'])
            output_string = random.choice(WAITING_TIME)
            response = output_string.format(dr_name=dr_info['Desc'], waiting_time=dr_info['WaitingTime'])
        else:
            print('something wrong.')

        return response

    def get_data(self, key):
        resp = self.table.query(
            KeyConditionExpression=Key('Name').eq(key)
        )
        return resp['Items'][0]

    def get_data_with_attr(self, key, attr):
        resp = self.table.scan(
            FilterExpression=Attr('Name').eq(key) & Attr('Address').eq(attr)
        )
        return resp['Items'][0]


if __name__ == '__main__':
    rospy.init_node('dynamodb_node', anonymous=False)
    db = DynamoDBNode()
