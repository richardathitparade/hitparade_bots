import sys
import traceback
from pymongo import MongoClient
from events_hitparade_co.serializers.serializer import HitParadeSerializer

class AwsDocumentDb(HitParadeSerializer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.hostname = kwargs.get('hostname', kwargs.get('-hostname', None))
        self.port = int(kwargs.get('port', kwargs.get('-port', 27012)))
        self.username =  kwargs.get('username', kwargs.get('-username', None))
        self.password= kwargs.get('password', kwargs.get('-password', None))
        self.connection_string = 'mongodb://{username}:{password}@{hostname}:{port}/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreference'.format(username=self.username, password=self.password,hostname=self.hostname,port=self.port)
        self.client = MongoClient(self.connection_string)
        self.default_database = kwargs.get('default_database', kwargs.get('-default_database', None))
        self.database_connections = dict()
        self.db = self.__db()


    def __db(self, database_name=None):
        try:
            db_name = database_name if database_name else self.default_database
            if self.database_connections.get(db_name, None) is None:
                self.database_connections[db_name] = self.client[db_name]
            return self.database_connections[db_name]
        except:
            traceback.print_exc()



    def reformat_data_unicode(self, obj=None):
        if obj and isinstance(obj, dict) and len(obj.keys()) > 0:
            new_obj = {k.replace('.', 'U+02FFE'):obj[k] for k in obj.keys()}
            for k in new_obj.keys():
                if isinstance(new_obj[k], list):
                    new_list = []
                    for no in new_obj[k]:
                        if not isinstance(no, str) or isinstance(no, int) or isinstance(no, float) or isinstance(no, bool):
                            new_list.append(self.reformat_data_unicode(obj=no))
                    if len(new_list) == len(new_obj[k]):
                        new_obj[k] = new_list
            return new_obj
        elif obj and isinstance(obj, list) and len(obj) > 0:
            new_list = []
            for l in obj:
                if not isinstance(l, str) or isinstance(l, int) or isinstance(l, float) or isinstance(l, bool):
                    new_list.append(self.reformat_data_unicode(obj=l))
            if len(new_list) == len(obj):
                return new_list
        return obj

    def store(self, **kwargs):
        collection = kwargs.get('collection', None)
        filename = kwargs.get('filename', None)
        if '?' in filename:
            filename = filename.split('?')[0] + '.json'
        data = kwargs.get('data', None)
        data['filename'] = filename
        print('writing out to file %s ....' % filename)
        resultset = self.__db(database_name=data['database'])[data['collection']].find_one({"hash" : data['hash'], "hash_wurl": data['hash_wurl']})
        if resultset is None:
            data_ = self.reformat_data_unicode(obj={ k.replace('.', 'U+022FE'):data[k] for k in data.keys() })
            self._id = self.__db(database_name=data['database'])[data['collection']].insert_one(data_)
            print('self_id is %s ' % self._id.inserted_id)
            return self._id.inserted_id, self._id.acknowledged
        else:
            print('id already found not being re-written ' % str(resultset.get('_id')) )
            return str(resultset.get('_id')), False


    class Factory:
        def create(self, **kwargs): return AwsDocumentDb(**kwargs)