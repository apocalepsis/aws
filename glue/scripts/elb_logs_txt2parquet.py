import sys
import fnmatch
import boto3
import time
import re

from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glue_context = GlueContext(sc)

s3 = boto3.resource('s3')
glue = boto3.client('glue',region_name='us-east-1')

job = Job(glue_context)
job_run_id = args['JOB_RUN_ID']

print 'job_run_id={0}'.format(job_run_id)

queue_bucket_name = 'aws.falej.logs.elb.queue'
input_bucket_name = 'aws.falej.logs.elb.input'
output_bucket_name = 'aws.falej.logs.elb.output'
staging_bucket_name = 'aws.falej.logs.elb.staging'
output_crawler_name = 'elb_logs_output'
batch_objects_size = 10
elb_regex = "^(\d*)-(\d*)-(\d*)T(\d*):(\d*):(\d*).(\d*)Z (.*) ([.\d]*):(\d*) ([.\d]*):(\d*) ([.\d]*) ([.\d]*) ([.\d]*) (\d*) (\d*) (\d*) (\d*) \"(.*) (.*) (- |.*)\" \"(.*)\" (.*) (.*)$"

do_move_queue_to_input = False
do_convert_txt_2_parquet = False
do_move_staging_to_output = False
do_clean_staging = False
do_run_crawler = False

def move_object(src_bucket_name,src_obj_key,tgt_bucket_name,tgt_obj_key):
    print 'Moving object: FROM <BucketName=\'{0}\',ObjectKey=\'{1}\'>, TO: <BucketName=\'{2}\',ObjectKey=\'{3}\'> ... ' \
        .format(src_bucket_name,src_obj_key,tgt_bucket_name,tgt_obj_key),
    src_obj = s3.Object(src_bucket_name,src_obj_key)
    s3.Object(tgt_bucket_name,tgt_obj_key).copy_from(CopySource = src_bucket_name+'/'+src_obj_key)
    s3.Object(src_bucket_name,src_obj_key).delete()
    print 'Done.'

def delete_object(bucket_name,obj_key):
    print 'Deleting object <BucketName=\'{0}\',ObjectKey=\'{1}\'> ... '.format(bucket_name,obj_key),
    s3.Object(bucket_name,obj_key).delete()
    print 'Done.'

def parse_elb_log_file(line):
    m = re.search(elb_regex,line)
    if m:
        return Row(
            year = m.group(1).encode('utf-8'),
            month = m.group(2).encode('utf-8'),
            day = m.group(3).encode('utf-8'),
            hours = m.group(4).encode('utf-8'),
            minutes = m.group(5).encode('utf-8'),
            seconds = m.group(6).encode('utf-8'),
            seconds_fraction = m.group(7).encode('utf-8'),
            elb_name = m.group(8).encode('utf-8'),
            request_ip = m.group(9).encode('utf-8'),
            request_port = m.group(10).encode('utf-8'),
            backend_ip = m.group(11).encode('utf-8'),
            backend_port = m.group(12).encode('utf-8'),
            request_processing_time = m.group(13).encode('utf-8'),
            backend_processing_time = m.group(14).encode('utf-8'),
            client_response_time = m.group(15).encode('utf-8'),
            elb_response_code = m.group(16).encode('utf-8'),
            backend_response_code = m.group(17).encode('utf-8'),
            received_bytes = m.group(18).encode('utf-8'),
            sent_bytes = m.group(19).encode('utf-8'),
            request_verb = m.group(20).encode('utf-8'),
            url = m.group(21).encode('utf-8'),
            protocol = m.group(22).encode('utf-8'),
            user_agent = m.group(23).encode('utf-8'),
            ssl_cipher = m.group(24).encode('utf-8'),
            ssl_protocol = m.group(25).encode('utf-8')
        )

job.init(args['JOB_NAME'], args)

queue_bucket = s3.Bucket(queue_bucket_name)
obj_list = list(queue_bucket.objects.limit(batch_objects_size))

do_move_queue_to_input = len(obj_list) > 0

if do_move_queue_to_input:

    print '>>> Moving objects from <BucketName=\'{0}\'> to <BucketName=\'{1}\'> ...' \
        .format(queue_bucket_name,input_bucket_name)

    for obj in obj_list:
        move_object(queue_bucket_name,obj.key,input_bucket_name,job_run_id+'/'+obj.key)

    do_convert_txt_2_parquet = True

    print '<<< Done.'

else:
    print 'No objects found'

if do_convert_txt_2_parquet:

    print '>>> Converting TXT files on <BucketName=\'{0}\',PrefixKey=\'{1}\'> to PARQUET files on <BucketName=\'{2}\',PrefixKey=\'{3}\'> ...' \
        .format(input_bucket_name,job_run_id,staging_bucket_name,job_run_id)

    sql_context = SQLContext(sc)

    logs = sc.textFile('s3://'+input_bucket_name+'/'+job_run_id+'/*')

    df = logs.map(parse_elb_log_file)
    sql_df = sql_context.createDataFrame(df)

    sql_df.write.mode('append').partitionBy('year','month').parquet('s3://'+staging_bucket_name+'/'+job_run_id+'/')

    do_move_staging_to_output = True

    print '<<< Done.'

if do_move_staging_to_output:

    print '>>> Moving PARQUET files from <BucketName=\'{0}\',PrefixKey=\'{1}\'> to <BucketName=\'{2}\',PrefixKey=\'{3}\'> ...' \
        .format(staging_bucket_name,job_run_id,output_bucket_name,'/')

    staging_bucket = s3.Bucket(staging_bucket_name)
    obj_list = list(staging_bucket.objects.filter(Prefix = job_run_id))

    for obj in obj_list:
        obj_key_name = re.search(job_run_id+'/(.*\.parquet)',obj.key)
        if obj_key_name:
            move_object(staging_bucket_name,obj.key,output_bucket_name,obj_key_name.group(1))

    do_clean_staging = True
    do_run_crawler = True

    print '<<< Done.'

if do_clean_staging:

    print '>>> Cleaning staging <BucketName=\'{0}\',PrefixKey=\'{1}\'> ...' \
        .format(staging_bucket_name,job_run_id)

    staging_bucket = s3.Bucket(staging_bucket_name)
    obj_list = list(staging_bucket.objects.filter(Prefix = job_run_id))

    for obj in obj_list:
        delete_object(staging_bucket_name,obj.key)

    print '<<< Done.'

if do_run_crawler:

    print '>>> Running crawler <{0}> ...' \
        .format(output_crawler_name)

    glue.start_crawler(Name=output_crawler_name)

    while True:
        time.sleep(1)
        state = glue.get_crawler(Name=output_crawler_name)['Crawler']['State']
        print "Crawler {0} is {1}".format(output_crawler_name,state)
        if state == 'READY':
            break

    print '<<< Done.'

job.commit()
