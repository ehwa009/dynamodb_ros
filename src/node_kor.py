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
    u"네, {location}은 {desc}에 있어요.",
]
PRESCRIPTION_TEXT_FAIL = [
    u"죄송해요 {name}님, 아직 의사선생님께서 처방전을 작성하지 않아서 아직 준비가 안됬어요. 제가 의사선생님께 이를 알려드렸어요. 처방전이 준비 되었을때, 알려드리도록 할게요."
]
PRESCRIPTION_TEXT_SUCCESS = [
    u"잠시만 기다리세요. 여기 있어요.", "여기 있어요."
]
FORGOT_DR_NAME = [
    u"걱정 마세요 {name}님, 오늘 {dr_name}의사 선생님과 예약이 잡혀있어요. 접수 해 드렸어요."
]
WAITING_TIME = [
    u"네, {dr_name}의사 선생님은 {waiting_time} 분 뒤에 진료 가능하십니다."
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
        if res[1] == '예약':
            name = res[2]
            address = res[3]
            resp = self.get_data_with_attr(name, address)
            output_string = random.choice(FORGOT_DR_NAME)
            response = output_string.format(name=resp['Name'], dr_name=resp['DrName'])
        elif res[1] == '위치':
            resp = self.get_data(res[2])
            output_string = random.choice(LOCATION_TEXT)
            response = output_string.format(location=resp['Name'], desc=resp['Desc'])
        elif res[1] == '처방전':
            name = res[2]
            address = res[3]
            resp = self.get_data_with_attr(name, address)
            if resp['Prescription'] is False:
                output_string = random.choice(PRESCRIPTION_TEXT_FAIL)
                response = output_string.format(name=resp['Name'])
            else:
                response = random.choice(PRESCRIPTION_TEXT_SUCCESS)
        elif res[1] == '대기시간':
            name = res[2]
            address = res[3]
            resp = self.get_data_with_attr(name, address)
            dr_info = self.get_data(resp['DrName'])
            output_string = random.choice(WAITING_TIME)
            response = output_string.format(dr_name=dr_info['Name'], waiting_time=dr_info['WaitingTime'])
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
