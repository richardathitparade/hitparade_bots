import traceback
from .file_serializer_json import HitParadeJsonFileSerializer
from .aws_document_db import AwsDocumentDb
from events_hitparade_co.serializers.serializer import HitParadeSerializer

class AggregatedSerializer(HitParadeSerializer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.s3_uploader = HitParadeJsonFileSerializer(**kwargs)
        self.document_db = AwsDocumentDb(**kwargs)



    def store(self, **kwargs):
        try:
            id_document = None
            reformatter = None
            database_serializer = None
            reformatted_document = None
            id_document_rfm = None
            id_file_rfm = None
            stored_document_db_rfm = None
            stored_file_rfm = None
            if not kwargs.get('data', {}).get('reformatter', None) is None:
                try:
                    type_id_val = kwargs.get('type_id', None)
                    kwargs.update(self.__dict__)
                    del kwargs['type_id']
                    reformatter = self.cache_output_component_func(type_id=kwargs.get('data', {}).get('reformatter', None),  **kwargs)
                    kwargs['type_id'] = type_id_val
                except:
                    traceback.print_exc()
                    print('reformatter creation failed')

            if not kwargs.get('data', {}).get('database_serializer', None) is None:
                try:
                    kwargs_dict = dict()
                    kwargs_dict.update(self.__dict__)
                    if not kwargs_dict.get('type_id', None) is None:
                        del kwargs_dict['type_id']
                    database_serializer = self.cache_output_component_func(  type_id=kwargs.get('data', {}).get('database_serializer', None), **kwargs_dict)
                except:
                    traceback.print_exc()
                    print('database_serializer creation failed')

            try:
                if not reformatter is None:
                    reformatted_document = reformatter.reformat(data=kwargs.get('data', None))
                    kwargs['data']['flattened'] = reformatted_document
                # id_document, stored_document_db = self.document_db.store(**kwargs)
                # if stored_document_db:
                #     print('document db stored %s ' % id_document)
            except:
                print('document db sotrage failed %s ' )
                traceback.print_exc()

            try:
                id_file, stored_file = self.s3_uploader.store(**kwargs)
                if stored_file:
                    print('file stored %s ' % id_file)
                kwargs_dict = dict()
                filename = kwargs['filename']
                if filename is None:
                    print('filename is none for ' % json.dumps(kwargs))
                else:
                    if filename and  '?' in filename:
                        kwargs_dict['filename'] = filename.split('?')[0] + '.json'
                
                    kwargs_dict['filename'] =   '.'.join( kwargs['filename'].split('.')[0:-1] + ['flattened'] + [kwargs['filename'].split('.')[-1]] )
                    # reformatted_document['filename_flattened'] = kwargs_dict['filename']
                    kwargs_dict['data']     =   reformatted_document
                    id_file_rfm, stored_file_rfm = self.s3_uploader.store(**kwargs_dict)
            except:
                print('file storage 1 error')
                traceback.print_exc()

            try:
                if not database_serializer is None:
                    new_kwargs = dict()
                    new_kwargs['data'] = reformatted_document
                    if not reformatted_document is None:
                        database_serializer.store(**new_kwargs)
            except:
                traceback.print_exc()
                print('error saving to database')
        except:
            traceback.print_exc()

    class Factory:
        def create(self, **kwargs): return AggregatedSerializer(**kwargs)
