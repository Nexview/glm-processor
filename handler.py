import json
import boto3
from netCDF4 import Dataset
import os
import dateutil.parser as dp

s3 = boto3.client('s3')

def moveExisting():
    s3_resource = boto3.resource('s3')
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    while True:
        objs = s3.list_objects_v2(Bucket='data.nexview.io', Prefix='glm/')['Contents']
        objCount = len(objs)
        if objCount > 46:
            oldestItem = sorted(objs, key=get_last_modified, reverse=False)[1]
            s3_resource.Object('data.nexview.io', oldestItem['Key']).delete()
        else:
            return

def process(event, context):
    message = event['Records'][0]['Sns']['Message']
    eventTime = json.loads(message)['Records'][0]['eventTime']
    parsed_t = dp.parse(eventTime)
    t_seconds = str(int(parsed_t.timestamp() * 1000))
    body = json.loads(message)['Records'][0]['s3']['object']['key']
    if body.split('/')[0].lower() == 'glm-l2-lcfa':
        print(body)
        s3.download_file('noaa-goes16', body, '/tmp/' + t_seconds + '.nc')
        ncin = Dataset('/tmp/' + t_seconds + '.nc', 'r', format='NETCDF4')
        latitude = ncin.variables['flash_lat']
        longitude = ncin.variables['flash_lon']

        correctedLatitude = latitude[:]
        correctedLongitude = longitude[:]

        merged_list = list(set([i for i in tuple(zip(correctedLatitude, correctedLongitude))]))
        open(os.fsencode('/tmp/' + t_seconds + '.json'), 'w').close()
        with open(os.fsencode('/tmp/' + t_seconds + '.json'), 'a') as file:
            file.write('{\n\t"type": "FeatureCollection",\n\t"features": [')
            file.write('\n\t\t{\n\t\t\t"type": "Feature",\n\t\t\t"geometry": {\n')
            file.write('\t\t\t\t"type": "MultiPoint",\n\t\t\t\t"coordinates": [\n\t\t\t\t\t')
            for point in merged_list:
                file.write('[' + str(point[1]) + ', ' + str(point[0]) + '], ')
            file.seek(0, os.SEEK_END)
            file.seek(file.tell() - 2, os.SEEK_SET)
            file.truncate()
            file.write('\n\t\t\t\t]\n\t\t\t}\n\t\t}')
            file.write('\n\t]\n}')
            file.close()
        s3.upload_file('/tmp/' + t_seconds + '.json', 'data.nexview.io', 'glm/' + t_seconds + '.json', ExtraArgs={'ACL':'public-read'})
        moveExisting()

    response = {
        "statusCode": 200,
        "body": body
    }

    return response