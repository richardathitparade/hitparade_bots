import boto.s3
import sys
from boto.s3.key import Key
import traceback



class S3FileUploader:

    def __init__(self, **kwargs):
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        self.AWS_ACCESS_KEY_ID = kwargs.get('AWS_ACCESS_KEY_ID',  kwargs.get('-AWS_ACCESS_KEY_ID', None))
        self.AWS_SECRET_ACCESS_KEY = kwargs.get('AWS_SECRET_ACCESS_KEY',  kwargs.get('-AWS_SECRET_ACCESS_KEY', None))
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
