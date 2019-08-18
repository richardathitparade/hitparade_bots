import psycopg2
import psycopg2.extras
import traceback
class HitParadeTransactionManager:

    def __init__(self, **kwargs):
        self.dbhostname = kwargs.get('dbhostname', kwargs.get('-dbhostname', None))
        self.dbport = kwargs.get('dbport', kwargs.get('-dbport', None))
        self.dbusername = kwargs.get('dbusername', kwargs.get('-dbusername', None))
        self.dbpassword = kwargs.get('dbpassword', kwargs.get('-dbpassword', None))
        self.db = kwargs.get('db', kwargs.get('-db', None))
        self.transaction_tables = kwargs.get('transaction_tables', [])
        self.store_state_static_prop = kwargs.get('store_state_static_prop', None)

    def db_connect(self, autocommit=False):
        try:
            connection = psycopg2.connect(user=self.dbusername,
                                          password=self.dbpassword,
                                          host=self.dbhostname,
                                          port=self.dbport,
                                          database=self.db )
            if autocommit:
                connection.autocommit = True
            else:
                connection.autocommit = False
            cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
            return connection, cursor
        except:
            traceback.print_exc()
            return None, None

    def db_close(self, cursor=None, connection=None):
        if (cursor):
            try:
                cursor.close()
                print("PostgreSQL cursor is closed")
            except:
                traceback.print_exc()
        if (connection):
            try:
                connection.close()
                print("PostgreSQL connection is closed")
            except:
                traceback.print_exc()


    def load_transactions(self, **kwargs):
        connection, cursor = self.db_connect()
        try:
            with connection:
                for table in self.transaction_tables:
                    cursor.execute( 'select hash, hash_wurl, scraper_url, sequence_number,current_url, is_deleted from  ' + table, {} )
                    for row in cursor:
                        if row:
                            self.store_state_static_prop(prop=row['hash'], val=row['scraper_url'], dict_sub=None)
                            self.store_state_static_prop(prop=row['hash_wurl'], val=row['scraper_url'], dict_sub=None)
                            self.store_state_static_prop(prop=row['hash']+'.fullvalue', val=row['current_url'], dict_sub=None)
                            self.store_state_static_prop(prop=row['hash']+'.fullvalue', val=row['current_url'], dict_sub=None)

                            self.store_state_static_prop(prop=row['scraper_url']+'.stripped' , val=row['hash'] , dict_sub=None)
                            self.store_state_static_prop(prop=row['scraper_url'], val=row['hash_wurl'] , dict_sub=None)
                            self.store_state_static_prop(prop=row['current_url'] + '.stripped', val=row['scraper_url'] , dict_sub=None)
                            self.store_state_static_prop(prop=row['current_url'], val=row['hash'] , dict_sub=None)
                            self.store_state_static_prop(prop=row['current_url'] + '.status', val=['scraped', 'stored'], dict_sub=None)
                            self.store_state_static_prop(prop=row['scraper_url'] + '.status', val=['scraped', 'stored'], dict_sub=None)

                            self.store_state_static_prop(prop=row['scraper_url'] +'.sqeuence' , val=str(row['sequence_number']), dict_sub=None)
                            self.store_state_static_prop(prop=row['current_url'] +'.sqeuence', val=str(row['sequence_number']), dict_sub=None)
                            self.store_state_static_prop(prop=row['hash'] +'.sqeuence', val=str(row['sequence_number']), dict_sub=None)
                            self.store_state_static_prop(prop=row['hash_wurl'] +'.sqeuence', val=str(row['sequence_number']), dict_sub=None)

                            self.store_state_static_prop(prop=row['scraper_url'] +'.is_deleted' , val=str(row['is_deleted']), dict_sub=None)
                            self.store_state_static_prop(prop=row['current_url'] +'.is_deleted', val=str(row['is_deleted']), dict_sub=None)
                            self.store_state_static_prop(prop=row['hash'] +'.is_deleted', val=str(row['is_deleted']), dict_sub=None)
                            self.store_state_static_prop(prop=row['hash_wurl'] +'.is_deleted', val=str(row['is_deleted']), dict_sub=None)
                            print('%s loaded...' % row['scraper_url'])
        except:
            traceback.print_exc()
        finally:
            self.db_close(cursor=cursor, connection=connection)
