import boto.s3
import sys
from boto.s3.key import Key
import traceback



class S3FileUploader:

    def __init__(self, **kwargs):
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        self.conn = boto.connect_s3(self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY, is_secure=self.is_secure)
        self.bucket = self.conn.create_bucket(self.bucket_name, location=boto.s3.connection.Location.DEFAULT)


    def percent_cb(self, complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    def upload(self, filename):
        try:
            upload_from = './' + filename
            upload_to = filename
            k = Key(self.bucket)
            k.key = upload_to
            k.set_contents_from_filename(upload_from, cb=self.percent_cb,num_cb=10)
            print('completed upload of %s to %s ' % (upload_from, upload_to))
            return True
        except:
            traceback.print_exc()
            return False

# try:
#     AWS_ACCESS_KEY_ID = 'AKIAXXASKJMWFAUXSP5P'
#     AWS_SECRET_ACCESS_KEY = 'q0dmcZF/GSYX4Nmpm0dDemoQ8g6gvk3WzVIJXCqr'
#
#     bucket_name = 'hitparade.tennis'
#     conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, is_secure=False)
#
#
#     bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.DEFAULT)
#
#     testfile = "./testupload.txt"
#     print('Uploading %s to Amazon S3 bucket %s' %  (testfile, bucket_name))
#
#     def percent_cb(complete, total):
#         sys.stdout.write('.')
#         sys.stdout.flush()
#
#
#     k = Key(bucket)
#     k.key = 'new_test_file.txt'#'my test file'
#     k.set_contents_from_filename(testfile, cb=percent_cb, num_cb=10)
#     print('completed')
# except:
#     traceback.print_exc()