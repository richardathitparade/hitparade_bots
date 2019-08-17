from events_hitparade_co.serializers.serializer import HitParadeSerializer
from events_hitparade_co.serializers.s3_serializer import S3FileUploader
import os
from datetime import datetime
import json



class HitParadeJsonFileSerializer(HitParadeSerializer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.s3_uploader = S3FileUploader(**kwargs)

    def store(self, **kwargs):
        filename = kwargs.get('filename', None)
        if '?' in filename:
            filename = filename.split('?')[0] + '.json'
        data = kwargs.get('data', None)
        print('writing out to file %s ....' % filename)
        file_written = False
        with open(filename, 'w') as outfile:
            json.dump(data,outfile, sort_keys=True, indent=4, default=self.myconverter)
            outfile.close()
            self.s3_uploader.upload(filename)
            print('uploaded to s3 - removing output file %s ' % outfile)
            os.remove('./'+filename)
            file_written = True
        return filename, file_written

    def myconverter(self, o):
        if isinstance(o, datetime):
            return o.__str__()

    class Factory:
        def create(self, **kwargs): return HitParadeJsonFileSerializer(**kwargs)

