from events_hitparade_co.serializers.serializer import HitParadeSerializer
import traceback
import json
import psycopg2
import psycopg2.extras
from collections import OrderedDict
import hashlib
class HitParadeMatchDetailsSofaDbSerializer(HitParadeSerializer):

    def __init__(self, **kwargs):
        self.dbhostname = kwargs.get('dbhostname', kwargs.get('-dbhostname', None))
        self.dbport = kwargs.get('dbport', kwargs.get('-dbport', None))
        self.dbusername = kwargs.get('dbusername', kwargs.get('-dbusername', None))
        self.dbpassword = kwargs.get('dbpassword', kwargs.get('-dbpassword', None))
        self.db = kwargs.get('db', kwargs.get('-db', None))
        self.store_state_static_prop = kwargs.get('store_state_static_prop', None)


    GAME_POINT_VALUES = ['0', '15','30','40', 'A', 'X']

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


    def user_exists(self, sofa_id=None):
        connection, cursor = self.db_connect()
        try:
            with connection:
                cursor.execute('select sofa_id, hpid from tennis_players where sofa_id=%(sofa_id)s' ,{'sofa_id' : sofa_id} )
                for row in cursor:
                    if row:
                        return row.get('sofa_id', None), row.get('hpid', None)
                return None, None
        except:
            traceback.print_exc()
        finally:
            self.db_close(cursor=cursor, connection=connection)

    def match_exists(self, hash=None, hash_wurl=None, scraper_url=None, remove_older=False):
        connection, cursor = self.db_connect()
        try:
            with connection:
                cursor.execute('select scraper_url, hpid, hash, hash_wurl, sequence_number from tennis_match_details where hash=%(hash)s and hash_wurl=%(hash_wurl)s and is_deleted=%(is_deleted)s' ,{'hash' : hash, 'is_deleted' : 'F', 'hash_wurl' : hash_wurl} )
                row = cursor.fetchone()
                if row:
                    return row.get('scraper_url', None), row.get('hpid', None), row.get('hash', None), row.get('hash_wurl', None), row.get('sequence_number', None), False
                else:
                    cursor.execute( 'select scraper_url, hpid, hash, hash_wurl, sequence_number from tennis_match_details where scraper_url=%(scraper_url)s and is_deleted=%(is_deleted)s', {'scraper_url' : scraper_url, 'is_deleted': 'F'} )
                    row = cursor.fetchone()
                    if row and remove_older:
                        scraper_url = row.get('scraper_url', None)
                        hpid = row.get('hpid', None)
                        hash = row.get('hash', None)
                        hash_wurl = row.get( 'hash_wurl', None)
                        sequence_number = row.get('sequence_number', None)
                        print('found an older version of stored url %s removing version %s ' % (scraper_url, str(sequence_number)))
                        cursor.execute('update tennis_match_details set is_deleted=%(is_deleted)s where hpid=%(hpid)s', {'is_deleted' : 'T', 'hpid' : hpid})
                        cursor.execute('update tennis_match_details_games set is_deleted=%(is_deleted)s where match_hpid=%(match_hpid)s', {'is_deleted' : 'T', 'match_hpid' : hpid})
                        cursor.execute('update tennis_match_details_points set is_deleted=%(is_deleted)s where match_hpid=%(match_hpid)s', {'is_deleted' : 'T', 'match_hpid' : hpid})
                        cursor.execute('update tennis_match_details_sets set is_deleted=%(is_deleted)s where match_hpid=%(match_hpid)s', {'is_deleted' : 'T', 'match_hpid' : hpid})
                        connection.commit()
                        return scraper_url, hpid, hash, hash_wurl, sequence_number, True
                return None, None, None, None, None, None
        except:
            traceback.print_exc()
            connection.rollback()
        finally:
            self.db_close(cursor=cursor, connection=connection)

    def __get_idx(self, list_value=None, indx_val=None):
        idx = -1
        if list_value and isinstance(list_value, list):
            try:
                idx = list_value.index(indx_val)
            except:
                pass
        return idx

    def __get_prev_point_value(self, point_value=None):
        ppv = '0'
        idx_value = self.__get_idx(HitParadeMatchDetailsSofaDbSerializer.GAME_POINT_VALUES, indx_val=str(point_value.strip()))
        if idx_value > 0:
            ppv = HitParadeMatchDetailsSofaDbSerializer.GAME_POINT_VALUES[(idx_value-1)]
        return ppv

    def __get_points_left(self,point=None, idx=None, previous_point=None):
        home_idx_value = self.__get_idx(HitParadeMatchDetailsSofaDbSerializer.GAME_POINT_VALUES,indx_val=str(point['start_home_score']))
        away_idx_value = self.__get_idx(HitParadeMatchDetailsSofaDbSerializer.GAME_POINT_VALUES, indx_val=str(point['start_away_score']))

        if previous_point is None or idx == 0:
            if point['tiebreaker_point']:
                return {
                    'away' : 7,
                    'home' : 7
                }
            else:
                return {
                    'away' : 4,
                    'home' : 4
                }
        elif not point['tiebreaker_point']:
            subtract_val = 4
            if point['start_home_score'] == 'A' or point['start_away_score']=='A':
                subtract_val = 5
            return {
                'home' : (subtract_val-home_idx_value),
                'away' : (subtract_val-away_idx_value)
            }
        else:
            home_int = int(previous_point.get('home_score', None))
            away_int = int(previous_point.get('away_score', None))
            if home_int < 8 and away_int < 8:
                return {
                    'home': 7 - home_int ,
                    'away':  7 - away_int,
                }
            else:
                if home_int == away_int:
                    return {
                        'home' : 2,
                        'away' : 2,
                    }
                elif home_int > away_int:
                    return {
                        'home': 1,
                        'away': 3,
                    }
                elif home_int < away_int:
                    return {
                        'home': 3,
                        'away': 1,
                    }
                else:
                    print('unable to satify tiebrake score...')
                    return {
                        'home': 10,
                        'away': 10,
                    }


    def __get_prev_point(self,point=None, idx=None, previous_point=None):
        if point['home_score'] == 'X' or point['away_score'] == 'X':
            return {
                    'home' : previous_point['home_score'],
                    'away' : previous_point['away_score']
                }
        if previous_point is None or idx == 0:
            return {
                'away' : 0,
                'home' : 0
            }
        elif not point['tiebreaker_point']:
            if point['winner'] == 'home':
                return {
                    'home' : self.__get_prev_point_value(point_value=point.get('home_score', None)),
                    'away' : point.get('away_score', None)
                }
            elif point['winner'] == 'away':
                return {
                    'home' : point.get('home_score', None),
                    'away' : self.__get_prev_point_value(point_value=point.get('away_score', None))
                }
            else:
                print('no winner found to decrement point value ---- no change')
                return {
                    'home': previous_point.get('home_score', None),
                    'away': previous_point.get('away_score', None),
                }
        else:
            return {
                'home': previous_point.get('home_score', None),
                'away': previous_point.get('away_score', None),
            }


    def insert_match_details(self, match=None):
        connection, cursor = self.db_connect()
        if match:
            if connection:
                try:
                    print(match)
                    away_player_hpid_1              = None
                    away_player_rank_1              = None
                    away_player_rank_text_1         = None
                    away_player_sofa_id_1           = None
                    away_player_hpid_2              = None
                    away_player_rank_2              = None
                    away_player_rank_text_2         = None
                    away_player_sofa_id_2           = None
                    home_player_hpid_1              = None
                    home_player_rank_1              = None
                    home_player_rank_text_1         = None
                    home_player_sofa_id_1           = None
                    home_player_hpid_2              = None
                    home_player_rank_2              = None
                    home_player_rank_text_2         = None
                    home_player_sofa_id_2           = None
                    if not match.get('away_players', None) is None:
                        away_player_hpid_1              = match['away_players'][0].get('hpid', None)
                        away_player_rank_1              = match['away_players'][0].get('rank', None)
                        away_player_rank_text_1         = match['away_players'][0].get('rank_text', None)
                        away_player_sofa_id_1           = match['away_players'][0].get('sofa_id', None)
                    if not match.get('away_players', None) is None and len(match['away_players']) > 1:
                        away_player_hpid_2          = match['away_players'][1].get('hpid', None)
                        away_player_rank_2          = match['away_players'][1].get('rank', None)
                        away_player_rank_text_2     = match['away_players'][1].get('rank_text', None)
                        away_player_sofa_id_2       = match['away_players'][1].get('sofa_id', None)
                    if not match.get('home_players', None) is None and not match.get('home_players', None) is None:
                        home_player_hpid_1              = match['home_players'][0].get('hpid', None)
                        home_player_rank_1              = match['home_players'][0].get('rank', None)
                        home_player_rank_text_1         = match['home_players'][0].get('rank_text', None)
                        home_player_sofa_id_1           = match['home_players'][0].get('sofa_id', None)
                    if  not match.get('home_players', None) is None and len(match['home_players']) > 1:
                        home_player_hpid_2          = match['home_players'][1].get('hpid', None)
                        home_player_rank_2          = match['away_players'][1].get('rank', None)
                        home_player_rank_text_2     = match['away_players'][1].get('rank_text', None)
                        home_player_sofa_id_2       = match['away_players'][1].get('sofa_id', None)
                    away_score_value = 0
                    home_score_value = 0
                    try:
                        away_score_value = None if match.get('score_result', {}).get('away_score', None) is None else int(match.get('score_result', {}).get('away_score', None))
                    except:
                        pass
                    try:
                        home_score_value = None if match.get('score_result', {}).get('home_score', None) is None else int(match.get('score_result', {}).get('home_score', None))
                    except:
                        pass
                    started = 'T' if match.get('score_result', {}).get('started', None) else 'F'
                    finished = 'T' if match.get('score_result', {}).get('finished', None) else 'F'
                    winner = 'home' if match.get('score_result', {}).get('home_winner', None) and not  match.get('score_result', {}).get('away_winner', None) else 'away' if not match.get('score_result', {}).get('home_winner', None) and match.get('score_result', {}).get('away_winner', None) else None
                    parameters = {
                        'away_string'                               :       match.get('away', None),                                                                            #away_string
                        'away_game_wins'                            :       match.get('away_game_wins', None),                                                                  #away_game_wins
                        'away_lower'                                :       match.get('away_lower', None),                                                                      #away_lower
                        'away_player_hpid_1'                        :       away_player_hpid_1,                                                                                 #away_player_hpid_1
                        'away_player_hpid_2'                        :       away_player_hpid_2,                                                                                 #away_player_hpid_2
                        'home_player_hpid_1'                        :       home_player_hpid_1,                                                                                 #home_player_hpid_1
                        'home_player_hpid_2'                        :       home_player_hpid_2,                                                                                 #home_player_hpid_2
                        'away_point_wins'                           :       match.get('away_point_wins', None),                                                                 #away_point_wins
                        'home_string'                               :       match.get('home', None),                                                                            #home_string
                        'home_lower'                                :       match.get('home_lower', None),                                                                      #home_lower
                        'home_game_wins'                            :       match.get('home_game_wins', None),                                                                  #home_game_wins
                        'home_point_wins'                           :       match.get('home_point_wins', None),                                                                 #home_point_wins
                        'sofa_category'                             :       match.get('sofa_category', None),                                                                   #sofa_category
                        'category'                                  :       match.get('category', None),                                                                        #category
                        'sofa_category_link'                        :       match.get('category_link', None),                                                                   #category_link
                        'collection'                                :       match.get('collection', None),                                                                      #collection
                        'current_url'                               :       match.get('current_url', None),                                                                     #current_url
                        'database'                                  :       match.get('database', None),                                                                        # database
                        'document_id'                               :       match.get('document_id', None),                                                                     # document_id
                        'document_id_flattened'                     :       match.get('document_id_flattened', None),                                                           # document_id_flattened
                        'filename'                                  :       match.get('filename', None),                                                                        # filename
                        'filename_flattened'                        :       match.get('filename_flattened', None),                                                              # filename_flattened
                        'hash'                                      :       match.get('hash', None),                                                                            # hash
                        'hash_wurl'                                 :       match.get('hash_wurl', None),                                                                       # hash_wurl
                        'id'                                        :       match.get('id', None),                                                                              # id
                        'match_length_string'                       :       match.get('match_length', None),                                                                    # match_length_string
                        'match_start_string'                        :       match.get('match_start', None),                                                                     # match_start_string
                        'hour_value'                                :       match.get('hour_value', None),                                                                      # hour_value
                        'minute_value'                              :       match.get('minute_value', None),                                                                      # minute_value
                        'match_type'                                :       match.get('match_type', None),                                                                      # match_type
                        'hour_string'                               :       match.get('hour_string', None),                                                                     #hour_string
                        'minute_string'                             :       match.get('minute_string', None),                                                                   #minute_string
                        'parent_id'                                 :       match.get('parent_id', None),                                                                       #parent_id
                        'scraper_url'                               :       match.get('scraper_url', None),                                                                     #scraper_url
                        'sport'                                     :       match.get('sport', None),                                                                           #sport
                        'sport_link'                                :       match.get('sport_link', None),                                                                      #sport_link
                        'title'                                     :       match.get('title', None),                                                                           #title
                        'total_game_number'                         :       match.get('total_game_number', None),                                                               #total_game_number
                        'total_point_number'                        :       match.get('total_point_number', None),                                                              #total_point_number
                        'total_match_minutes'                       :       match.get('total_match_minutes', None),                                                             #total_match_minutes
                        'tournament'                                :       match.get('tournament', None),                                                                      #tournament
                        'tournament_link'                           :       match.get('tournament_link', None),                                                                 #tournament_link

                        'away_percentage'                           :       match.get('votes', {}).get('away_percentage'),                                                      #away_percentage
                        'away_percentage_value'                     :       match.get('votes', {}).get('away_percentage_value'),                                                #away_percentage_value
                        'away_votes'                                :       match.get('votes', {}).get('away_votes'),                                                           #away_votes
                        'home_percentage'                           :       match.get('votes', {}).get('home_percentage'),                                                      #home_percentage
                        'home_percentage_value'                     :       match.get('votes', {}).get('home_percentage_value'),                                                #home_percentage_value
                        'home_votes'                                :       match.get('votes', {}).get('home_votes'),                                                           #home_votes
                        'total_votes'                               :       match.get('votes', {}).get('total_votes'),                                                          #total_votes

                        'court'                                     :       match.get('venue', {}).get('court'),                                                                #court
                        'location'                                  :       match.get('venue', {}).get('location'),                                                             #location
                        'start_date'                                :       match.get('venue', {}).get('start_date'),                                                           #start_date
                        'start_date_day'                            :       match.get('venue', {}).get('start_date_day'),                                                       #start_date_day
                        'start_date_day_of_week'                    :       match.get('venue', {}).get('start_date_day_of_week'),                                               #start_date_day_of_week
                        'start_date_hour'                           :       match.get('venue', {}).get('start_date_hour'),                                                      #start_date_hour
                        'start_date_minute'                         :       match.get('venue', {}).get('start_date_minute'),                                                    #start_date_minute
                        'start_date_month'                          :       match.get('venue', {}).get('start_date_month'),                                                     #start_date_month
                        'start_date_year'                           :       match.get('venue', {}).get('start_date_year'),                                                      #start_date_year
                        'surface'                                   :       match.get('venue', {}).get('surface'),                                                              #surface
                        'venue_string'                              :       match.get('venue', {}).get('venue_string'),                                                         #venue_string
                        'away_second_serve_points_percentage'       :       match.get('second_serve_points', {}).get('away_second_serve_points_percentage'),                    #away_second_serve_points_percentage
                        'away_second_serve_points_success'          :       match.get('second_serve_points', {}).get('away_second_serve_points_success'),                       #away_second_serve_points_success
                        'away_second_serve_points_totals'           :       match.get('second_serve_points', {}).get('away_second_serve_points_totals'),                        #away_second_serve_points_totals
                        'away_second_serve_points_stat'             :       match.get('second_serve_points', {}).get('away_stat'),                                              #away_second_serve_points_stat
                        'home_second_serve_points_stat'             :       match.get('second_serve_points', {}).get('home_stat'),                                              #home_second_serve_points_stat
                        'home_second_serve_points_percentage'       :       match.get('second_serve_points', {}).get('home_second_serve_points_percentage'),                    #home_second_serve_points_percentage
                        'home_second_serve_points_success'          :       match.get('second_serve_points', {}).get('home_second_serve_points_success'),                       #home_second_serve_points_success
                        'home_second_serve_points_totals'           :       match.get('second_serve_points', {}).get('home_second_serve_points_totals'),                        #home_second_serve_points_totals


                        'away_second_serve_percentage'              :       match.get('second_serve', {}).get('away_second_serve_percentage'),                                  #away_second_serve_percentage
                        'away_second_serve_success'                 :       match.get('second_serve', {}).get('away_second_serve_success'),                                     #away_second_serve_success
                        'away_second_serve_totals'                  :       match.get('second_serve', {}).get('away_second_serve_totals'),                                      #away_second_serve_totals
                        'away_second_serve_stat'                    :       match.get('second_serve', {}).get('away_stat'),                                                     #away_second_serve_stat
                        'home_second_serve_stat'                    :       match.get('second_serve', {}).get('home_stat'),                                                     #home_second_serve_stat
                        'home_second_serve_percentage'              :       match.get('second_serve', {}).get('home_second_serve_percentage'),                                  #home_second_serve_percentage
                        'home_second_serve_success'                 :       match.get('second_serve', {}).get('home_second_serve_success'),                                     #home_second_serve_success
                        'home_second_serve_totals'                  :       match.get('second_serve', {}).get('home_second_serve_totals'),                                      #home_second_serve_totals


                        'away_rank_p1'                              :       away_player_rank_1,                                                                                 # away_rank_p1
                        'away_rank_p2'                              :       away_player_rank_2,                                                                                 # away_rank_p2
                        'home_rank_p1'                              :       home_player_rank_1,                                                                                 # home_rank_p1
                        'home_rank_p2'                              :       home_player_rank_2,                                                                                 # home_rank_p2

                        'away_rank_text_p1'                         :       away_player_rank_text_1,                                                                            # away_rank_text_p1
                        'away_rank_text_p2'                         :       away_player_rank_text_2,                                                                            # away_rank_text_p2
                        'home_rank_text_p1'                         :       home_player_rank_text_1,                                                                            # home_rank_text_p1
                        'home_rank_text_p2'                         :       home_player_rank_text_2,                                                                            # home_rank_text_p2

                        'away_sofa_id_p1'                           :       away_player_sofa_id_1,                                                                              # away_sofa_id_p1
                        'away_sofa_id_p2'                           :       away_player_sofa_id_2,                                                                              # away_sofa_id_p2
                        'home_sofa_id_p1'                           :       home_player_sofa_id_1,                                                                              # home_sofa_id_p1
                        'home_sofa_id_p2'                           :       home_player_sofa_id_2,                                                                              # home_sofa_id_p2


                        'away_score_text'                           :       match.get('score_result', {}).get('away_score'),                                                    # away_score_text
                        'home_score_text'                           :       match.get('score_result', {}).get('home_score'),                                                    # home_score_text
                        'away_score'                                :       away_score_value,                                                                                   # away_score
                        'home_score'                                :       home_score_value,                                                                                   # home_score

                        'is_started'                                :       started,                                                                                            #is_started
                        'is_finished'                               :       finished,                                                                                           #is_finished
                        'winner'                                    :       winner,                                                                                             #winner
                        'start_date_time'                           :       match.get('venue', {}).get('start_date_value', None),                                               #start_date_time
                        'away_receiver_points_won_value'            :       match.get('receiver_points_won', {}).get('away_receiver_points_won_value', None),                   #away_receiver_points_won_value
                        'home_receiver_points_won_value'            :       match.get('receiver_points_won', {}).get('home_receiver_points_won_value', None),                   #home_receiver_points_won_value
                        'home_receiver_points_won_stat'             :       match.get('receiver_points_won', {}).get('home_stat', None),                                        #home_receiver_points_won_stat
                        'away_reciver_points_won_stat'              :       match.get('receiver_points_won', {}).get('away_stat', None),                                        #away_reciver_points_won_stat



                        'away_points_won_value'                     :       match.get('points_won', {}).get('away_points_won_value', None),                                     # away_points_won_value
                        'home_points_won_value'                     :       match.get('points_won', {}).get('home_points_won_value', None),                                     # home_points_won_value
                        'away_points_won_stat'                      :       match.get('points_won', {}).get('away_stat', None),                                                 # away_points_won_stat
                        'home_points_won_stat'                      :       match.get('points_won', {}).get('home_stat', None),                                                 # home_points_won_stat

                        'away_first_serve_points_percentage'        :       match.get('first_serve_points', {}).get('away_first_serve_points_percentage', None),                # away_first_serve_points_percentage
                        'home_first_serve_points_percentage'        :       match.get('first_serve_points', {}).get('home_first_serve_points_percentage', None),                # home_first_serve_points_percentage
                        'away_first_serve_points_success'           :       match.get('first_serve_points', {}).get('away_first_serve_points_success', None),                   # away_first_serve_points_success
                        'home_first_serve_points_success'           :       match.get('first_serve_points', {}).get('home_first_serve_points_success', None),                   # home_first_serve_points_success
                        'away_first_serve_points_totals'            :       match.get('first_serve_points', {}).get('away_first_serve_points_totals', None),                    # away_first_serve_points_totals
                        'home_first_serve_points_totals'            :       match.get('first_serve_points', {}).get('home_first_serve_points_totals', None),                    # home_first_serve_points_totals
                        'away_first_serve_points_stat'              :       match.get('first_serve_points', {}).get('away_stat', None),                                         # away_first_serve_points_stat
                        'home_first_serve_points_stat'              :       match.get('first_serve_points', {}).get('home_stat', None),                                         # home_first_serve_points_stat


                        'away_first_serve_percentage'               :       match.get('first_serve', {}).get('away_first_serve_percentage', None),                              # away_first_serve_points_stat
                        'home_first_serve_percentage'               :       match.get('first_serve', {}).get('home_first_serve_percentage', None),                              # home_first_serve_points_stat
                        'away_first_serve_success'                  :       match.get('first_serve', {}).get('away_first_serve_success', None),                                 # away_first_serve_points_stat
                        'home_first_serve_success'                  :       match.get('first_serve', {}).get('home_first_serve_success', None),                                 # home_first_serve_points_stat
                        'away_first_serve_totals'                   :       match.get('first_serve', {}).get('away_first_serve_totals', None),                                  # away_first_serve_points_stat
                        'home_first_serve_totals'                   :       match.get('first_serve', {}).get('home_first_serve_totals', None),                                  # home_first_serve_points_stat
                        'away_first_serve_stat'                     :       match.get('first_serve', {}).get('away_stat', None),                                                # away_first_serve_points_stat
                        'home_first_serve_stat'                     :       match.get('first_serve', {}).get('home_stat', None),                                                # home_first_serve_points_stat


                        'away_double_faults_value'                  :       match.get('double_faults', {}).get('away_double_faults_value', None),                               # away_first_serve_points_value
                        'home_double_faults_value'                  :       match.get('double_faults', {}).get('home_double_faults_value', None),                               # home_first_serve_points_value
                        'away_double_faults_stat'                   :       match.get('double_faults', {}).get('away_stat', None),                                              # away_first_serve_points_stat
                        'home_double_faults_stat'                   :       match.get('double_faults', {}).get('home_stat', None),                                              # home_first_serve_points_stat

                        # away_first_serve_points_stat
                        'away_break_points_percentage'              :       match.get('break_points', {}).get('away_break_points_percentage', None),                            # away_break_points_percentage
                        'home_break_points_percentage'              :       match.get('break_points', {}).get('home_break_points_percentage', None),                            # home_break_points_percentage
                        'away_break_points_success'                 :       match.get('break_points', {}).get('away_break_points_success', None),                               # away_break_points_success
                        'home_break_points_success'                 :       match.get('break_points', {}).get('home_break_points_success', None),                               # home_break_points_success
                        'away_break_points_totals'                  :       match.get('break_points', {}).get('away_break_points_totals', None),                                # away_break_points_totals
                        'home_break_points_totals'                  :       match.get('break_points', {}).get('home_break_points_totals', None),                                # home_break_points_totals
                        'away_break_points_stat'                    :       match.get('break_points', {}).get('away_stat', None),                                               # away_break_points_stat
                        'home_break_points_stat'                    :       match.get('break_points', {}).get('home_stat', None),                                               # away_break_points_stat


                        'away_aces_value'                           :       match.get('aces', {}).get('away_aces_value', None),                                                 # away_aces_value
                        'home_aces_value'                           :       match.get('aces', {}).get('home_aces_value', None),                                                 # home_aces_value
                        'away_aces_stat'                            :       match.get('aces', {}).get('away_stat', None),                                                       # away_aces_stat
                        'home_aces_stat'                            :       match.get('aces', {}).get('home_stat', None),                                                       # home_aces_stat


                        'is_deleted'                                :       'F',                                                                                                # is_deleted
                        'sequence_number'                           :       match.get('sequence_number', 1),                                                                    # sequence_number

                        'sofa_id'                                   :       match.get('scraper_url', None).split('/')[-1],                                                      # sofa_id
                        'sofa_guid'                                 :       match.get('scraper_url', None).split('/')[-2],                                                      # sofa_guid


                        'away_value_american'                       :       match.get('odds', {}).get('away_value_american', None),                                             # away_value_american
                        'home_value_american'                       :       match.get('odds', {}).get('home_value_american', None),                                             # home_value_american
                        'away_value_american_value'                 :       match.get('odds', {}).get('away_value_american_value', None),                                       # away_value_american_value
                        'home_value_american_value'                 :       match.get('odds', {}).get('home_value_american_value', None),                                       # home_value_american_value
                        'away_value_decimal'                        :       match.get('odds', {}).get('away_value_decimal', None),                                              # away_value_decimal
                        'away_value_decimal_value'                  :       match.get('odds', {}).get('away_value_decimal_value', None),                                        # away_value_decimal_value
                        'home_value_decimal'                        :       match.get('odds', {}).get('home_value_decimal', None),                                              # home_value_decimal
                        'home_value_decimal_value'                  :       match.get('odds', {}).get('home_value_decimal_value', None),                                        # home_value_decimal_value
                        'away_value_fractional'                     :       match.get('odds', {}).get('away_value_fractional', None),                                           # away_value_fractional
                        'away_value_fractional_numerator'           :       match.get('odds', {}).get('away_value_fractional_numerator', None),                                 # away_value_fractional_numerator
                        'away_value_fractional_denominator'         :       match.get('odds', {}).get('away_value_fractional_denominator', None),                               # away_value_fractional_denominator
                        'home_value_fractional'                     :       match.get('odds', {}).get('home_value_fractional', None),                                           # home_value_fractional
                        'home_value_fractional_numerator'           :       match.get('odds', {}).get('home_value_fractional_numerator', None),                                 # home_value_fractional_numerator
                        'home_value_fractional_denominator'         :       match.get('odds', {}).get('home_value_fractional_denominator', None)                                # home_value_fractional_denominator

                    }


                    cursor.execute(  """INSERT INTO public.tennis_match_details( away_value_american, home_value_american, away_value_american_value, home_value_american_value, away_value_decimal, away_value_decimal_value, home_value_decimal, home_value_decimal_value, away_value_fractional, away_value_fractional_numerator, away_value_fractional_denominator, home_value_fractional, home_value_fractional_numerator, home_value_fractional_denominator, sofa_id, sofa_guid, away_string, away_game_wins, away_lower, away_player_hpid_1, away_player_hpid_2, home_player_hpid_1, home_player_hpid_2, away_point_wins, home_string, home_lower, home_game_wins,  home_point_wins, sofa_category, sofa_category_link, collection, current_url, database, document_id, document_id_flattened, filename, filename_flattened, hash, hash_wurl, id, match_length_string, match_start_string, hour_value, match_type, hour_string, minute_string, minute_value, parent_id, scraper_url, sport, sport_link, title, total_game_number, total_point_number, total_match_minutes, tournament, tournament_link, away_percentage, away_percentage_value, away_votes, home_percentage, home_percentage_value, home_votes, total_votes, court, location, start_date, start_date_day, start_date_day_of_week, start_date_hour, start_date_minute,start_date_month, start_date_year, surface, venue_string,  away_second_serve_points_percentage, away_second_serve_points_success, away_second_serve_points_totals, away_second_serve_points_stat, home_second_serve_points_stat, home_second_serve_points_percentage, home_second_serve_points_success, home_second_serve_points_totals, away_second_serve_percentage, away_second_serve_success, away_second_serve_totals, away_second_serve_stat, home_second_serve_stat, home_second_serve_percentage, home_second_serve_success, home_second_serve_totals, away_rank_p1, away_rank_p2, home_rank_p1, home_rank_p2, away_rank_text_p1, away_rank_text_p2, home_rank_text_p1, home_rank_text_p2, away_sofa_id_p1, away_sofa_id_p2, home_sofa_id_p1, home_sofa_id_p2, away_score_text, home_score_text, away_score, home_score, is_started, is_finished, winner, start_date_time, away_receiver_points_won_value, home_receiver_points_won_value, home_receiver_points_won_stat, away_reciver_points_won_stat, away_points_won_value, home_points_won_value, away_points_won_stat, home_points_won_stat, away_first_serve_points_percentage, home_first_serve_points_percentage, away_first_serve_points_success, home_first_serve_points_success, away_first_serve_points_totals, home_first_serve_points_totals, away_first_serve_points_stat, home_first_serve_points_stat, away_first_serve_percentage, home_first_serve_percentage, away_first_serve_success, home_first_serve_success, away_first_serve_totals, home_first_serve_totals, away_first_serve_stat, home_first_serve_stat, away_double_faults_value, home_double_faults_value, away_double_faults_stat, home_double_faults_stat, away_break_points_percentage, away_break_points_success, away_break_points_totals, home_break_points_percentage, home_break_points_success, home_break_points_totals, home_break_points_stat, away_break_points_stat, away_aces_value, home_aces_value, away_aces_stat, home_aces_stat, sequence_number, is_deleted) VALUES (%(away_value_american)s, %(home_value_american)s, %(away_value_american_value)s, %(home_value_american_value)s, %(away_value_decimal)s, %(away_value_decimal_value)s, %(home_value_decimal)s, %(home_value_decimal_value)s, %(away_value_fractional)s, %(away_value_fractional_numerator)s, %(away_value_fractional_denominator)s, %(home_value_fractional)s, %(home_value_fractional_numerator)s, %(home_value_fractional_denominator)s,  %(sofa_id)s, %(sofa_guid)s, %(away_string)s, %(away_game_wins)s, %(away_lower)s, %(away_player_hpid_1)s, %(away_player_hpid_2)s, %(home_player_hpid_1)s, %(home_player_hpid_2)s,%(away_point_wins)s, %(home_string)s, %(home_lower)s, %(home_game_wins)s, %(home_point_wins)s, %(sofa_category)s, %(sofa_category_link)s, %(collection)s,  %(current_url)s,  %(database)s,  %(document_id)s,  %(document_id_flattened)s, %(filename)s, %(filename_flattened)s, %(hash)s, %(hash_wurl)s, %(id)s, %(match_length_string)s, %(match_start_string)s, %(hour_value)s, %(match_type)s, %(hour_string)s, %(minute_string)s, %(minute_value)s, %(parent_id)s, %(scraper_url)s, %(sport)s, %(sport_link)s, %(title)s, %(total_game_number)s, %(total_point_number)s, %(total_match_minutes)s, %(tournament)s, %(tournament_link)s, %(away_percentage)s, %(away_percentage_value)s, %(away_votes)s,  %(home_percentage)s,  %(home_percentage_value)s,  %(home_votes)s, %(total_votes)s, %(court)s, %(location)s, %(start_date)s, %(start_date_day)s,  %(start_date_day_of_week)s,  %(start_date_hour)s,  %(start_date_minute)s, %(start_date_month)s, %(start_date_year)s, %(surface)s, %(venue_string)s,%(away_second_serve_points_percentage)s, %(away_second_serve_points_success)s, %(away_second_serve_points_totals)s, %(away_second_serve_points_stat)s, %(home_second_serve_points_stat)s, %(home_second_serve_points_percentage)s, %(home_second_serve_points_success)s, %(home_second_serve_points_totals)s, %(away_second_serve_percentage)s, %(away_second_serve_success)s, %(away_second_serve_totals)s, %(away_second_serve_stat)s, %(home_second_serve_stat)s, %(home_second_serve_percentage)s, %(home_second_serve_success)s, %(home_second_serve_totals)s, %(away_rank_p1)s, %(away_rank_p2)s, %(home_rank_p1)s, %(home_rank_p2)s, %(away_rank_text_p1)s, %(away_rank_text_p2)s, %(home_rank_text_p1)s, %(home_rank_text_p2)s, %(away_sofa_id_p1)s, %(away_sofa_id_p2)s, %(home_sofa_id_p1)s, %(home_sofa_id_p2)s, %(away_score_text)s, %(home_score_text)s, %(away_score)s, %(home_score)s, %(is_started)s, %(is_finished)s, %(winner)s, %(start_date_time)s, %(away_receiver_points_won_value)s, %(home_receiver_points_won_value)s, %(home_receiver_points_won_stat)s, %(away_reciver_points_won_stat)s, %(away_points_won_value)s, %(home_points_won_value)s, %(away_points_won_stat)s, %(home_points_won_stat)s, %(away_first_serve_points_percentage)s, %(home_first_serve_points_percentage)s, %(away_first_serve_points_success)s, %(home_first_serve_points_success)s, %(away_first_serve_points_totals)s, %(home_first_serve_points_totals)s, %(away_first_serve_points_stat)s, %(home_first_serve_points_stat)s, %(away_first_serve_percentage)s, %(home_first_serve_percentage)s, %(away_first_serve_success)s, %(home_first_serve_success)s, %(away_first_serve_totals)s, %(home_first_serve_totals)s, %(away_first_serve_stat)s, %(home_first_serve_stat)s, %(away_double_faults_value)s, %(home_double_faults_value)s, %(away_double_faults_stat)s, %(home_double_faults_stat)s, %(away_break_points_percentage)s, %(away_break_points_success)s, %(away_break_points_totals)s, %(home_break_points_percentage)s, %(home_break_points_success)s, %(home_break_points_totals)s, %(home_break_points_stat)s, %(away_break_points_stat)s, %(away_aces_value)s, %(home_aces_value)s, %(away_aces_stat)s, %(home_aces_stat)s,  %(sequence_number)s, %(is_deleted)s) RETURNING *""", parameters)
                    row = cursor.fetchone()
                    match_hpid = row['hpid']
                    set_hpids = []
                    game_hpids = []
                    point_hpids = []
                    for s in match.get('sets', []):
                        set_parameters = dict()
                        set_parameters['match_hpid'] = match_hpid
                        set_parameters['is_deleted'] = 'F'
                        set_parameters['sofa_id'] = row['sofa_id']
                        set_parameters['game_count'] = len(s.get('games', []))
                        set_parameters['number_of_games'] = set_parameters['game_count']
                        set_parameters['is_tiebreak'] = 'T' if s.get('tiebreak', None) and len(s.get('games',[])) == 1 else 'F' if not s.get('tiebreak', None) or (s.get('tiebreak', None) and len(s.get('games',[])) > 1 ) else None
                        set_parameters['has_tiebreak'] = 'T' if s.get('tiebreak', None) else 'F' if not s.get('tiebreak', None) else None
                        set_parameters['number_of_points'] = 0
                        for g in s.get('games', []):
                            set_parameters['number_of_points'] += len(g.get('points', []))
                        s['hash'] = hashlib.sha224(str(json.dumps(OrderedDict(s))).encode('utf-8')).hexdigest()
                        stat_keys = ['aces', 'break_points', 'double_faults', 'first_serve', 'first_serve_points', 'max_points_in_a_row', 'points_won', 'receiver_points_won', 'second_serve', 'second_serve_points']
                        for k in stat_keys:
                            if s.get(k, None):
                                set_parameters.update(s.get(k, {}))
                                set_parameters['away_'+k+'_stat'] = s.get(k,{}).get('away_stat', None)
                                set_parameters['home_' + k + '_stat'] = s.get(k, {}).get('home_stat', None)
                            else:
                                set_parameters['away_'+k+'_percentage'] = None
                                set_parameters['home_'+k+'_percentage'] = None
                                set_parameters['away_'+k+'_value'] = None
                                set_parameters['home_'+k+'_value'] = None
                                set_parameters['away_'+k+'_stat'] = None
                                set_parameters['home_'+k+'_stat'] = None
                                set_parameters['away_'+k+'_success'] = None
                                set_parameters['home_'+k+'_success'] = None
                                set_parameters['away_'+k+'_totals'] = None
                                set_parameters['home_'+k+'_totals'] = None

                            for _key in s.keys():
                                if not _key in stat_keys:
                                    set_parameters[_key] = s.get(_key, None)
                        check_set_params = ['current_set_streak', 'hour_string', 'hour_value', 'minute_string', 'minute_value', 'set_time', 'set_title']
                        for csp in check_set_params:
                            if set_parameters.get(csp, None) is None:
                                set_parameters[csp] = None


                        cursor.execute(  """INSERT INTO public.tennis_match_details_sets( has_tiebreak, hash,game_count, away_aces_value, home_aces_value, away_aces_stat, home_aces_stat, away_score, home_score, away_tiebreak_score, home_tiebreak_score, current_set_streak, winner, away_break_points_percentage, away_break_points_success, away_break_points_totals, home_break_points_percentage, home_break_points_success, home_break_points_totals, home_break_points_stat, away_break_points_stat, away_double_faults_value, home_double_faults_value, away_double_faults_stat, home_double_faults_stat, away_first_serve_percentage, away_first_serve_success, away_first_serve_totals, home_first_serve_percentage, home_first_serve_success, home_first_serve_totals, home_first_serve_stat, away_first_serve_stat, away_first_serve_points_percentage, away_first_serve_points_success, away_first_serve_points_totals, away_first_serve_points_stat, home_first_serve_points_percentage, home_first_serve_points_success, home_first_serve_points_totals, home_first_serve_points_stat, number_of_games, hour_string, hour_value, minute_string, minute_value, set_name, set_number, set_time, set_title, is_tiebreak, away_max_points_in_a_row_value, home_max_points_in_a_row_value, away_max_points_in_a_row_stat, home_max_points_in_a_row_stat, away_points_won_value, away_points_won_stat, home_points_won_value, home_points_won_stat, away_receiver_points_won_value, home_receiver_points_won_value, away_receiver_points_won_stat, home_receiver_points_won_stat, away_second_serve_percentage, home_second_serve_percentage, away_second_serve_success, home_second_serve_success, away_second_serve_totals, home_second_serve_totals, away_second_serve_stat, home_second_serve_stat, away_second_serve_points_percentage, home_second_serve_points_percentage, away_second_serve_points_success, home_second_serve_points_success, away_second_serve_points_totals, home_second_serve_points_totals, away_second_serve_points_stat, home_second_serve_points_stat, number_of_points, match_hpid, is_deleted) VALUES ( %(has_tiebreak)s, %(hash)s, %(game_count)s, %(away_aces_value)s, %(home_aces_value)s, %(away_aces_stat)s, %(home_aces_stat)s, %(away_score)s, %(home_score)s, %(away_tiebreak_score)s, %(home_tiebreak_score)s, %(current_set_streak)s, %(winner)s, %(away_break_points_percentage)s, %(away_break_points_success)s, %(away_break_points_totals)s, %(home_break_points_percentage)s, %(home_break_points_success)s, %(home_break_points_totals)s, %(home_break_points_stat)s, %(away_break_points_stat)s, %(away_double_faults_value)s, %(home_double_faults_value)s, %(away_double_faults_stat)s, %(home_double_faults_stat)s, %(away_first_serve_percentage)s, %(away_first_serve_success)s, %(away_first_serve_totals)s, %(home_first_serve_percentage)s, %(home_first_serve_success)s, %(home_first_serve_totals)s, %(home_first_serve_stat)s, %(away_first_serve_stat)s, %(away_first_serve_points_percentage)s, %(away_first_serve_points_success)s, %(away_first_serve_points_totals)s, %(away_first_serve_points_stat)s, %(home_first_serve_points_percentage)s, %(home_first_serve_points_success)s,  %(home_first_serve_points_totals)s, %(home_first_serve_points_stat)s, %(number_of_games)s, %(hour_string)s, %(hour_value)s, %(minute_string)s, %(minute_value)s, %(set_name)s, %(set_number)s, %(set_time)s, %(set_title)s, %(is_tiebreak)s, %(away_max_points_in_a_row_value)s, %(home_max_points_in_a_row_value)s, %(away_max_points_in_a_row_stat)s, %(home_max_points_in_a_row_stat)s, %(away_points_won_value)s, %(away_points_won_stat)s, %(home_points_won_value)s, %(home_points_won_stat)s, %(away_receiver_points_won_value)s, %(home_receiver_points_won_value)s, %(away_receiver_points_won_stat)s, %(home_receiver_points_won_stat)s, %(away_second_serve_percentage)s, %(home_second_serve_percentage)s, %(away_second_serve_success)s, %(home_second_serve_success)s, %(away_second_serve_totals)s, %(home_second_serve_totals)s, %(away_second_serve_stat)s, %(home_second_serve_stat)s, %(away_second_serve_points_percentage)s, %(home_second_serve_points_percentage)s, %(away_second_serve_points_success)s, %(home_second_serve_points_success)s, %(away_second_serve_points_totals)s, %(home_second_serve_points_totals)s, %(away_second_serve_points_stat)s,  %(home_second_serve_points_stat)s, %(number_of_points)s,  %(match_hpid)s, %(is_deleted)s ) RETURNING *""", set_parameters)
                        row_set = cursor.fetchone()
                        s['hpid'] = row_set['hpid']
                        set_hpids.append(s['hpid'])
                        for g in s.get('games', []):
                            g['hash'] = hashlib.sha224(str(json.dumps(OrderedDict(g))).encode('utf-8')).hexdigest()
                            g['match_hpid'] = row['hpid']
                            g['set_hpid'] = s['hpid']
                            game_parameters = dict()
                            if not g.get('home_score', None) is None:
                                g['home_score_value'] =  int(g.get('home_score'))
                                g['end_home_score_value'] = int(g.get('home_score'))
                                if g['winner'] == 'home':
                                    g['home_score_value'] -= 1
                            if not g.get('away_score', None) is None:
                                g['away_score_value'] =  int(g.get('away_score'))
                                g['end_away_score_value'] = int(g.get('away_score'))
                                if g['winner'] == 'away':
                                    g['away_score_value'] -= 1
                            g['score_differential'] = -1000
                            g['home_to_6'] = -1000
                            g['away_to_6'] = -1000
                            try:
                                g['score_differential'] = g['away_score_value'] - g['home_score_value']
                            except:
                                pass
                            g['is_deleted'] = 'F'
                            try:
                                g['home_to_6'] = 6 - g['home_score_value']
                            except:
                                pass
                            try:
                                g['away_to_6'] = 6 - g['away_score_value']
                            except:
                                pass
                            for k in stat_keys:
                                game_parameters.update(g.get(k, {}))
                                game_parameters['away_' + k + '_stat'] = g.get(k, {}).get('away_stat', None)
                                game_parameters['home_' + k + '_stat'] = g.get(k, {}).get('home_stat', None)
                            for _key in g.keys():
                                if not _key in stat_keys:
                                    if isinstance(g.get(_key, None), bool):
                                        game_parameters[_key] = 'T' if g.get(_key, None) else 'F' if not g.get(_key, None) else None
                                    else:
                                        game_parameters[_key] = g.get(_key, None)
                            previous_point = None
                            for idx, p in enumerate(g.get('points', [])):
                                p['home_to_6'] =  g.get('home_to_6', -1000)
                                p['away_to_6'] =  g.get('away_to_6', -1000)
                                p['match_hpid'] = row['hpid']
                                p['set_hpid']   = row_set['hpid']
                                p['end_away_score_value'] = g.get('end_away_score_value', -1000)
                                p['end_home_score_value'] = g.get('end_home_score_value', -1000)
                                p['start_away_score_value'] = g.get('away_score_value', -1000)
                                p['start_home_score_value'] = g.get('home_score_value', -1000)
                                p['is_deleted'] = 'F'
                                p['broken'] = 'F' if p['serving'] == p['winner'] else 'T'
                                p['score_differential'] = g.get('score_differential', -1000)
                                obj_val = self.__get_prev_point(point=p, idx=idx, previous_point=previous_point)
                                p['start_home_score'] = obj_val['home']
                                p['start_away_score'] = obj_val['away']
                                obj_points_remaining = self.__get_points_left(point=p, idx=idx, previous_point=previous_point)
                                p['home_points_to_win'] = obj_points_remaining['home']
                                p['away_points_to_win'] = obj_points_remaining['away']
                                p['point_differential'] = p['away_points_to_win'] - p['home_points_to_win']
                                previous_point=p
                                p['hash'] = hashlib.sha224(str(json.dumps(OrderedDict(p))).encode('utf-8')).hexdigest()
                            if len(g.get('points', [])) > 0:
                                previous_point = g.get('points', [])[-1]
                                if previous_point['point_differential'] > 0:
                                    game_parameters['point_differential'] = previous_point['point_differential'] + 1
                                elif previous_point['point_differential'] < 0:
                                    game_parameters['point_differential'] = previous_point['point_differential'] - 1
                                else:
                                    game_parameters['point_differential'] = None

                            cursor.execute("""INSERT INTO public.tennis_match_details_games( point_differential, end_home_score_value, end_away_score_value, home_to_6, away_to_6, score_differential, hash, away_game_wins, home_game_wins, broken_serve_by, broken_serve, current_game_streak, game_number, home_score, serving, set_number, away_score, home_score_value, away_score_value, winner, total_game_number, set_hpid, match_hpid, is_deleted) VALUES (   %(point_differential)s, %(end_home_score_value)s, %(end_away_score_value)s, %(home_to_6)s, %(away_to_6)s, %(score_differential)s, %(hash)s, %(away_game_wins)s, %(home_game_wins)s, %(broken_serve_by)s, %(broken_serve)s, %(current_game_streak)s, %(game_number)s, %(home_score)s, %(serving)s, %(set_number)s, %(away_score)s, %(home_score_value)s, %(away_score_value)s, %(winner)s, %(total_game_number)s, %(set_hpid)s, %(match_hpid)s, %(is_deleted)s )  RETURNING *""", game_parameters)
                            row_game = cursor.fetchone()
                            game_hpids.append(row_game['hpid'])
                            for idx, p in enumerate(g.get('points', [])):
                                point_parameters = dict()
                                p['game_hpid'] = row_game['hpid']
                                for _key in p.keys():
                                    if not _key in stat_keys:
                                        if isinstance(p.get(_key, None), bool):
                                            point_parameters[_key] = 'T' if p.get(_key, None) else 'F' if not p.get(_key, None) else None
                                        else:
                                            if p.get(_key, None) and isinstance( p.get(_key, None), str) and p.get(_key, None).isdigit():
                                                point_parameters[_key] = int(p.get(_key, None))
                                            else:
                                                point_parameters[_key] =  p.get(_key, None)
                                cursor.execute("""INSERT INTO public.tennis_match_details_points(  point_differential, start_home_score, start_away_score, away_points_to_win, home_points_to_win,  start_home_score_value, start_away_score_value, end_home_score_value, end_away_score_value, home_to_6, away_to_6, score_differential, broken, game_hpid, set_hpid, match_hpid, away_point_wins, away_score, home_score, is_deleted, home_point_wins, serving, set_number, tiebreaker_point, total_point_number, winner) VALUES ( %(point_differential)s, %(start_home_score)s, %(start_away_score)s, %(away_points_to_win)s, %(home_points_to_win)s,  %(start_home_score_value)s, %(start_away_score_value)s, %(end_home_score_value)s, %(end_away_score_value)s, %(home_to_6)s, %(away_to_6)s, %(score_differential)s, %(broken)s, %(game_hpid)s, %(set_hpid)s, %(match_hpid)s, %(away_point_wins)s, %(away_score)s, %(home_score)s, %(is_deleted)s, %(home_point_wins)s, %(serving)s, %(set_number)s, %(tiebreaker_point)s, %(total_point_number)s, %(winner)s )  RETURNING *""", point_parameters)
                                row_point = cursor.fetchone()
                                point_hpids.append(row_point['hpid'])
                    connection.commit()
                    return row['sofa_id'], row['hpid'], set_hpids, game_hpids, point_hpids
                except:
                    traceback.print_exc()
                    connection.rollback()
                finally:
                    self.db_close(cursor=cursor, connection=connection)

    def insert_player(self, player=None):
        connection, cursor = self.db_connect()
        if player:
            player['hash'] = hashlib.sha224(str(json.dumps(OrderedDict(player))).encode('utf-8')).hexdigest()
            if connection:
                try:
                    gender = 'female' if player.get('female', None) else 'male'
                    parameters = [player.get('firstname', None), player.get('firstname_lower', None), player.get('fullname', None), player.get('fullname_lower', None), gender, player.get('lastname', None), player.get('lastname_lower', None), player.get('rank', None), player.get('rank_text', None),  player.get('sofa_id', None),  player.get('sofa_href', None),  player.get('sofa_img', None),  player.get('sofa_img_alt', None),  player.get('sofa_img_title', None), player.get('hash', None) ]
                    cursor.execute('INSERT INTO public.tennis_players( firstname, firstname_lower, fullname, fullname_lower, gender, lastname, lastname_lower, rank, rank_text, sofa_id, sofa_href, sofa_img, sofa_img_alt, sofa_img_title,  hashvalue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *', parameters)
                    row = cursor.fetchone()
                    print(row)
                    connection.commit()
                    return row['sofa_id'], row['hpid']
                except:
                    connection.rollback()
                    traceback.print_exc()
                finally:
                    self.db_close(cursor=cursor, connection=connection)

    def store(self, data=None):
        connection, cursor = self.db_connect()
        if connection:
            try:
                if not data.get('home_players',None) is None:
                    for p in data['home_players']:
                        if not p.get('sofa_id', None) is None:
                            sofa_id, hpid = self.user_exists(sofa_id=p['sofa_id'])
                            if hpid:
                                p['hpid'] = hpid
                            else:
                                sofa_id, hpid = self.insert_player(player=p)
                                if sofa_id and hpid and sofa_id == p['sofa_id']:
                                    p['hpid'] = hpid
                if not data.get('away_players', None) is None:
                    for p in data['away_players']:
                        if not p.get('sofa_id', None) is None:
                            sofa_id, hpid = self.user_exists(sofa_id=p['sofa_id'])
                            if hpid:
                                p['hpid'] = hpid
                            else:
                                sofa_id, hpid = self.insert_player(player=p)
                                if sofa_id and hpid and sofa_id == p['sofa_id']:
                                    p['hpid'] = hpid
                scraper_url, hpid, hash, hash_wurl, sequence_number, removed = self.match_exists(hash=data['hash'], hash_wurl=data['hash_wurl'], scraper_url=data['scraper_url'], remove_older=True)
                if removed is None or removed:
                    data['sequence_number'] = 1
                    if not sequence_number is None:
                        data['sequence_number'] = (sequence_number+1)
                    self.insert_match_details(match=data)
            except:
                traceback.print_exc()
            finally:
                self.db_close(cursor=cursor, connection=connection)

    class Factory:
        def create(self, **kwargs): return HitParadeMatchDetailsSofaDbSerializer(**kwargs)
