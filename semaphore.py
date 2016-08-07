from flask import (
    Flask,
    jsonify,
)
import boto3
from botocore.exceptions import ClientError
import os
import time
import uuid

app = Flask(__name__)
table_name = os.environ.get('DYNAMO_TABLE', 'semaphore')
region = os.environ.get('DYNAMO_REGION', 'us-east-1')


def _get_semaphore_table():
    dynamodb = boto3.resource('dynamodb', region_name=region)
    return dynamodb.Table(table_name)


@app.route('/uuid/', methods=['GET'])
def create_uuid():
    return jsonify({'uuid': str(uuid.uuid4())})


@app.route('/<uuid>/lock/', methods=['POST'])
def lock(uuid):
    now = int(time.time())
    try:
        table = _get_semaphore_table()
        table.put_item(
            Item={
                'uuid': uuid,
                'created': now},
            ConditionExpression="attribute_not_exists(#uid)",
            ExpressionAttributeNames={'#uid': 'uuid'}
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return jsonify({
                    'status': 'failure',
                    'error': 'Semaphore is already locked'}), 409
        else:
            return jsonify({
                    'status': 'failure',
                    'error': 'A server error occurred'}), 500
    except Exception as e:
        return jsonify({
                'status': 'failure',
                'error': 'A server error occurred'}), 500
    else:
        return jsonify({'status': 'success'}), 200


@app.route('/<uuid>/unlock/', methods=['POST'])
def unlock(uuid):
    try:
        table = _get_semaphore_table()
        table.delete_item(
            Key={'uuid': uuid},
            ConditionExpression='attribute_exists(#uid)',
            ExpressionAttributeNames={'#uid': 'uuid'})
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return jsonify({
                    'status': 'failure',
                    'error': 'Semaphore is already unlocked'}), 404
        else:
            return jsonify({
                    'status': 'failure',
                    'error': 'A server error occurred'}), 500
    except Exception as e:
        return jsonify({
                'status': 'failure',
                'error': 'A server error occurred'}), 500
    else:
        return jsonify({'status': 'success'}), 200


if __name__ == '__main__':
    app.run(debug=True, port=8001)
