from events_hitparade_co.serializers.serializer import HitParadeSerializer
import json
from events_hitparade_co.serializers.s3_serializer import S3FileUploader
import os
class HitParadeJsonFileSerializer(HitParadeSerializer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.s3_uploader = S3FileUploader(**kwargs)

    def store(self, **kwargs):
        filename = kwargs.get('filename', None)
        data = kwargs.get('data', None)
        print('writing out to file %s ....' % filename)
        with open(filename, 'w') as outfile:
            json.dump(data, outfile)
            outfile.close()
            self.s3_uploader.upload(filename)
            print('uploaded to s3 - removing output file %s ' % outfile)
            os.remove('./'+filename)



    class Factory:
        def create(self, **kwargs): return HitParadeJsonFileSerializer(**kwargs)

