import traceback
from datetime import datetime
from pytz import timezone
import pytz
from events_hitparade_co.reformatters.reformatter import HitParadeReformatter
class HitParadeMatchDetailsSofaReformatter(HitParadeReformatter):

    GAME_POINT_VALUES = ['0', '15','30','40', 'A', 'X']

    def __init__(self, **kwargs):
        self.data = kwargs['data']


    def __reformat_players(self,player_value=None, player_value_lower=None):
        p1 = dict()
        p2  =None
        if '/' in player_value:
            p2 = dict()
            p1['fullname'] = player_value.split('/')[0].strip()
            p2['fullname'] = player_value.split('/')[1].strip()
            p1['lastname'] = p1['fullname'].split(' ')[0].strip()
            p1['firstname'] = ' '.join(p1['fullname'].split(' ')[1:]).strip()
            p2['lastname'] = p2['fullname'].split(' ')[0].strip()
            p2['firstname'] = ' '.join(p2['fullname'].split(' ')[1:]).strip()

            p1['fullname_lower'] = player_value_lower.split('/')[0].strip()
            p2['fullname_lower'] = player_value_lower.split('/')[1].strip()
            p1['lastname_lower'] = p1['fullname_lower'].split(' ')[0].strip()
            p1['firstname_lower'] = ' '.join(p1['fullname_lower'].split(' ')[1:]).strip()
            p2['lastname_lower'] = p2['fullname_lower'].split(' ')[0].strip()
            p2['firstname_lower'] = ' '.join(p2['fullname_lower'].split(' ')[1:]).strip()

        else:
            p1['fullname'] = player_value
            p1['fullname_lower'] = player_value_lower
            p1['lastname'] = p1['fullname'].split(' ')[0].strip()
            p1['firstname'] = ' '.join(p1['fullname'].split(' ')[1:]).strip()
            p1['lastname_lower'] = p1['lastname'].lower().strip()
            p1['firstname_lower'] = p1['firstname'].lower().strip()
        return p1, p2

    def __parse_player_score_data(self, player_scores=None):
        meta_data_values = dict()
        meta_data_values['score_result'] = dict()
        for idx, score_data in enumerate(player_scores):
            if (len(player_scores) == 3 and (idx == 0 or idx == 2)) or (len(player_scores) == 5 and not idx == 2):
                meta_data_values['match_type'] = 'singles'
                obj_values = score_data.get('div.details__emblem-container > div > a', {})
                key_start = 'home' if idx == 2 and len(player_scores) == 3 or idx > 2 and len(  player_scores ) == 5 else 'away'
                if len(obj_values) > 0 and len(player_scores) == 3:
                    meta_data_values['score_result'][key_start + '_rank'] = obj_values[0].get('data-rank', None)
                    meta_data_values['score_result']['singles'] = True
                    meta_data_values['score_result']['doubles'] = False
                    meta_data_values['score_result'][key_start + '_rank_text'] = obj_values[0].get('text', {}).get('text', None)
                    meta_data_values['score_result'][key_start + '_sofa_id'] = obj_values[0].get('data-team-id', None)
                    meta_data_values['score_result'][key_start + '_sofa_href'] = obj_values[0].get('href', None)
                    meta_data_values['score_result'][key_start + '_sofa_img'] = obj_values[0].get('img.img--x56', {}).get('src', None)
                    meta_data_values['score_result'][key_start + '_sofa_img_title'] = obj_values[0].get('img.img--x56', {}).get('title', None)
                    meta_data_values['score_result'][key_start + '_sofa_img_alt'] = obj_values[0].get('img.img--x56', {}).get('alt', None)
                    if meta_data_values['score_result'][key_start + '_sofa_id'] is None and not meta_data_values['score_result'][key_start + '_sofa_href'] is None:
                        meta_data_values['score_result'][key_start + '_sofa_id'] = meta_data_values['score_result'][key_start + '_sofa_href'].split('/')[-1]
                elif len(obj_values) > 0 and len(player_scores) == 5:
                    meta_data_values['match_type'] = 'doubles'
                    suffix = '_' + str((idx + 1)) if idx < 2 else '_' + str((idx - 2))
                    meta_data_values['score_result']['singles'] = False
                    meta_data_values['score_result']['doubles'] = True
                    meta_data_values['score_result'][key_start + '_rank' + suffix] = obj_values[0].get('data-rank',  None)
                    meta_data_values['score_result'][key_start + '_rank_text' + suffix] = obj_values[0].get('text', {}).get(
                        'text', None)
                    meta_data_values['score_result'][key_start + '_sofa_id' + suffix] = obj_values[0].get( 'data-team-id', None)
                    meta_data_values['score_result'][key_start + '_sofa_href' + suffix] = obj_values[0].get('href', None)
                    meta_data_values['score_result'][key_start + '_sofa_img' + suffix] = obj_values[0].get( 'img.img--x56', {}).get('src', None)
                    meta_data_values['score_result'][key_start + '_sofa_img_title' + suffix] = obj_values[0].get(  'img.img--x56', {}).get('title', None)
                    meta_data_values['score_result'][key_start + '_sofa_img_alt' + suffix] = obj_values[0].get( 'img.img--x56', {}).get('alt', None)
                    if meta_data_values['score_result'][key_start + '_sofa_id' + suffix] is None and not meta_data_values['score_result'][key_start + '_sofa_href' + suffix] is None:
                        meta_data_values['score_result'][key_start + '_sofa_id' + suffix] = meta_data_values['score_result'][key_start + '_sofa_href' + suffix].split('/')[-1]
            if (idx == 1 and len(player_scores)==3) or (idx==2 and len(player_scores)==5):
                winner_index = self.__get_idx(list_value=score_data.get('class', []), indx_val=['winner'])
                if len(score_data.get('div.cell__section--main > div.cell__content > span > span', [])) > 0:
                    meta_data_values['score_result']['started'] = True
                    meta_data_values['score_result']['away_score'] = score_data.get('div.cell__section--main > div.cell__content > span > span', [])[0].get('text', None)
                    meta_data_values['score_result']['home_score'] = score_data.get('div.cell__section--main > div.cell__content > span > span', [])[1].get('text', None)
                else:
                    meta_data_values['score_result']['started'] = False
                meta_data_values['score_result']['home_winner'] = winner_index == 1
                meta_data_values['score_result']['away_winner'] = winner_index == 0
                meta_data_values['score_result']['finished'] = winner_index > -1
        return meta_data_values

    def __date_time_reformat(self, date_time=None):
        hour_string = None
        minute_string = None
        hour_value = -1
        minute_value = -1
        total_time_value = -1
        if not date_time == '' and not date_time is None or not 'not started' in date_time.lower():
            date_time_copy = date_time if not 'after' in date_time else date_time.split('after')[1].strip()
            if 'after' in date_time_copy:
                date_time_copy = date_time_copy.split('after')[1].strip()

            if 'h' in date_time_copy:
                hour_string =  date_time_copy.split('h')[0].strip()
                date_time_copy = ' '.join( date_time_copy.split('h')[1:]).strip()
            if 'm' in date_time_copy:
                minute_string = date_time_copy.split('m')[0].strip()
            if hour_string:
                hour_value = int(hour_string)
            else:
                hour_value = 0
            if minute_string:
                minute_value = int(minute_string)
            else:
                minute_value = 0

            total_time_value = (60 * hour_value) + minute_value
        return hour_string, minute_string, hour_value, minute_value, total_time_value

    def __player_is_male_female(self,rank_text=None, category_text=None):
        male = False
        female = False
        if rank_text:
            male = 'atp' in rank_text.lower()
            female = 'wta' in rank_text.lower()
        if not male and not female and category_text:
            female = 'women' in category_text.strip().lower()

        return male, female

    def __reformat_player_ids(self, meta_data_values=None):
        if len(meta_data_values['home_players']) == 1:
            meta_data_values['home_players'][0]['rank'] = meta_data_values['score_result']['home_rank']
            meta_data_values['home_players'][0]['rank_text'] = meta_data_values['score_result']['home_rank_text']
            meta_data_values['home_players'][0]['sofa_id'] = meta_data_values['score_result']['home_sofa_id']
            meta_data_values['home_players'][0]['sofa_href'] = meta_data_values['score_result']['home_sofa_href']
            meta_data_values['home_players'][0]['sofa_img'] = meta_data_values['score_result']['home_sofa_img']
            meta_data_values['home_players'][0]['sofa_img_title'] = meta_data_values['score_result']['home_sofa_img_title']
            meta_data_values['home_players'][0]['sofa_img_alt'] = meta_data_values['score_result']['home_sofa_img_alt']
            meta_data_values['home_players'][0]['male'], meta_data_values['home_players'][0]['female'] = self.__player_is_male_female(category_text=meta_data_values['category'],rank_text=meta_data_values['home_players'][0]['rank_text'])


            meta_data_values['away_players'][0]['rank'] = meta_data_values['score_result']['away_rank']
            meta_data_values['away_players'][0]['rank_text'] = meta_data_values['score_result']['away_rank_text']
            meta_data_values['away_players'][0]['sofa_id'] = meta_data_values['score_result']['away_sofa_id']
            meta_data_values['away_players'][0]['sofa_href'] = meta_data_values['score_result']['away_sofa_href']
            meta_data_values['away_players'][0]['sofa_img'] = meta_data_values['score_result']['away_sofa_img']
            meta_data_values['away_players'][0]['sofa_img_title'] = meta_data_values['score_result']['away_sofa_img_title']
            meta_data_values['away_players'][0]['sofa_img_alt'] = meta_data_values['score_result']['away_sofa_img_alt']
            meta_data_values['away_players'][0]['male'], meta_data_values['away_players'][0]['female'] = self.__player_is_male_female(category_text=meta_data_values['category'],rank_text=meta_data_values['score_result'][ 'away_rank_text'])


        elif len(meta_data_values['home_players']) == 2:
            meta_data_values['home_players'][0]['rank'] = meta_data_values['score_result']['home_rank_1']
            meta_data_values['home_players'][0]['rank_text'] = meta_data_values['score_result']['home_rank_text_1']
            meta_data_values['home_players'][0]['sofa_id'] = meta_data_values['score_result']['home_sofa_id_1']
            meta_data_values['home_players'][0]['sofa_href'] = meta_data_values['score_result']['home_sofa_href_1']
            meta_data_values['home_players'][0]['sofa_img'] = meta_data_values['score_result']['home_sofa_img_1']
            meta_data_values['home_players'][0]['sofa_img_title'] = meta_data_values['score_result']['home_sofa_img_title_1']
            meta_data_values['home_players'][0]['sofa_img_alt'] = meta_data_values['score_result']['home_sofa_img_alt_1']
            meta_data_values['home_players'][0]['male'], meta_data_values['home_players'][0]['female'] = self.__player_is_male_female(category_text=meta_data_values['category'],rank_text=meta_data_values['home_players'][0]['rank_text'])


            meta_data_values['away_players'][0]['rank'] = meta_data_values['score_result']['away_rank_1']
            meta_data_values['away_players'][0]['rank_text'] = meta_data_values['score_result']['away_rank_text_1']
            meta_data_values['away_players'][0]['sofa_id'] = meta_data_values['score_result']['away_sofa_id_1']
            meta_data_values['away_players'][0]['sofa_href'] = meta_data_values['score_result']['away_sofa_href_1']
            meta_data_values['away_players'][0]['sofa_img'] = meta_data_values['score_result']['away_sofa_img_1']
            meta_data_values['away_players'][0]['sofa_img_title'] = meta_data_values['score_result']['away_sofa_img_title_1']
            meta_data_values['away_players'][0]['sofa_img_alt'] = meta_data_values['score_result']['away_sofa_img_alt_1']
            meta_data_values['away_players'][0]['male'], meta_data_values['away_players'][0]['female'] = self.__player_is_male_female(category_text=meta_data_values['category'],rank_text=meta_data_values['away_players'][0]['rank_text'])

            meta_data_values['home_players'][1]['rank'] = meta_data_values['score_result']['home_rank_2']
            meta_data_values['home_players'][1]['rank_text'] = meta_data_values['score_result']['home_rank_text_2']
            meta_data_values['home_players'][1]['sofa_id'] = meta_data_values['score_result']['home_sofa_id_2']
            meta_data_values['home_players'][1]['sofa_href'] = meta_data_values['score_result']['home_sofa_href_2']
            meta_data_values['home_players'][1]['sofa_href'] = meta_data_values['score_result']['home_sofa_href_2']
            meta_data_values['home_players'][1]['sofa_img'] = meta_data_values['score_result']['home_sofa_img_2']
            meta_data_values['home_players'][1]['sofa_img_title'] = meta_data_values['score_result']['home_sofa_img_title_2']
            meta_data_values['home_players'][1]['sofa_img_alt'] = meta_data_values['score_result']['home_sofa_img_alt_2']
            meta_data_values['home_players'][1]['male'], meta_data_values['home_players'][1]['female'] = self.__player_is_male_female(category_text=meta_data_values['category'],rank_text=meta_data_values['home_players'][1]['rank_text'])

            meta_data_values['away_players'][1]['rank'] = meta_data_values['score_result']['away_rank_2']
            meta_data_values['away_players'][1]['rank_text'] = meta_data_values['score_result']['away_rank_text_2']
            meta_data_values['away_players'][1]['sofa_id'] = meta_data_values['score_result']['away_sofa_id_2']
            meta_data_values['away_players'][1]['sofa_href'] = meta_data_values['score_result']['away_sofa_href_2']
            meta_data_values['away_players'][1]['sofa_href'] = meta_data_values['score_result']['away_sofa_href_2']
            meta_data_values['away_players'][1]['sofa_img'] = meta_data_values['score_result']['away_sofa_img_2']
            meta_data_values['away_players'][1]['sofa_img_title'] = meta_data_values['score_result']['away_sofa_img_title_2']
            meta_data_values['away_players'][1]['sofa_img_alt'] = meta_data_values['score_result']['away_sofa_img_alt_2']
            meta_data_values['away_players'][1]['male'], meta_data_values['away_players'][1]['female'] =self.__player_is_male_female(category_text=meta_data_values['category'],rank_text=meta_data_values['away_players'][1]['rank_text'])

    def __reformat_title(self, title=None):
        storage_dict = dict()
        away_p1 = None
        away_p2 = None
        home_p1 = None
        home_p2 = None
        if '-' in title:
            tsplit = [x.strip() for x in title.split('-')]
            tsplit_lower = [x.strip().lower() for x in title.split('-')]
            storage_dict['away'], storage_dict['home'] = tsplit[0], tsplit[1]
            storage_dict['away_lower'], storage_dict['home_lower'] = tsplit_lower[0], tsplit_lower[1]
            away_p1, away_p2 = self.__reformat_players(player_value=storage_dict['away'], player_value_lower=storage_dict['away_lower'])
            home_p1, home_p2 = self.__reformat_players(player_value=storage_dict['home'], player_value_lower=storage_dict['home_lower'])
        else:
            print('no - found in title %s ' % title)
        storage_dict['away_players'] = [x for x in [away_p1, away_p2] if x]
        storage_dict['home_players'] = [x for x in [home_p1, home_p2] if x]
        return storage_dict

    def __reformat_categories(self, meta_data=None):
        meta_data_values = dict()
        for obj in meta_data:
            if 'ul.breadcrumb > li > a' in obj.keys():
               for idx,class_values in enumerate(obj.get('class', [])):
                   if 'js-breadcrumb-sport-link' in class_values:
                        meta_data_values['sport_link'] = obj.get('href', [])[idx]
                        meta_data_values['sport'] = obj.get('ul.breadcrumb > li > a', [])[idx].get('text', None)
                   if 'js-breadcrumb-category-link' in class_values:
                        meta_data_values['category_link'] = obj.get('href', [])[idx]
                        meta_data_values['category'] = obj.get('ul.breadcrumb > li > a', [])[idx].get('text', None)
                   if 'js-breadcrumb-tournament-link' in class_values:
                        meta_data_values['tournament_link'] = obj.get('href', [])[idx]
                        meta_data_values['tournament'] = obj.get('ul.breadcrumb > li > a', [])[idx].get('text', None)
            if 'div.page-title-container > h2.page-title' in obj.keys():
                obj_data = obj.get('div.page-title-container > h2.page-title', {})
                for objdata in obj_data:
                    meta_data_values['title'] = objdata.get('text', None)
                    meta_data_values.update( self.__reformat_title(title=meta_data_values['title']) )
        return meta_data_values

    def __reformat_set_score(self, set_score=None):
        set_val = None
        tiebreaker_val = None
        tiebreaker = False
        if set_score and set_score.isdigit():
            set_val = int(set_score)
            if set_val > 59:
                tiebreaker = True
                if set_val > 59 and set_val < 70:
                    tiebreaker_val = set_val - 60
                    set_val = 6
                elif set_val > 59 and set_val > 69 and set_val < 100:
                    tiebreaker_val = set_val - 70
                    set_val = 7
                elif set_val > 599 and set_val < 700:
                    tiebreaker_val = set_val - 600
                    set_val = 6
                elif set_val > 699:
                    tiebreaker_val = set_val - 700
                    set_val = 7

        return set_val, tiebreaker_val, tiebreaker

    def __get_idx(self, list_value=None, indx_val=None):
        idx = -1
        if list_value and isinstance(list_value, list):
            try:
                idx = list_value.index(indx_val)
            except:
                pass
        return idx

    def __title_to_index(self, text_val=None):
        idx = -1
        if text_val:
            tvc = text_val.strip().lower()
            if 'first' in tvc or '1st' in tvc:
                idx = 1
            elif 'second' in tvc or '2nd' in tvc:
                idx = 2
            elif 'third' in tvc or '3rd' in tvc:
                idx = 3
            elif 'fourth' in tvc or '4th' in tvc:
                idx = 4
            elif 'fifth' in tvc or '5th' in tvc:
                idx = 5
        return idx

    def __parse_list_of_lists(self, class_listing=None, meta_data=None):
        todict = dict()
        if class_listing and isinstance(class_listing, list) and meta_data and isinstance(meta_data, list):
            for idx, clist in enumerate(class_listing):
                for find_value in meta_data:
                    if todict.get(find_value, -1) == -1:
                        todict[find_value] = self.__get_idx(list_value=clist, indx_val=find_value)
                        if todict[find_value] > -1:
                            todict[find_value] = idx
        return todict

    def __parse_point_content(self, point_content=None, serving=None, set_number=None):
        all_points = []
        winner_index = -1
        if point_content:
            for idx,point in enumerate(point_content):
                new_point = dict()
                new_point['set_number'] = set_number
                winner_index = self.__parse_list_of_lists(class_listing=point.get('class', []), meta_data=['regular']).get('regular', -1)
                new_point['winner'] = 'away' if winner_index == 0 else 'home' if winner_index == 1 else None
                new_point['away_score'] =  str(point.get('div.pbp__game-content', [])[0].get('text', None))
                new_point['home_score'] = str(point.get('div.pbp__game-content', [])[1].get('text', None))
                new_point['point_number'] = (idx+1)
                new_point['tiebreaker_point'] = not (new_point['away_score'] in HitParadeMatchDetailsSofaReformatter.GAME_POINT_VALUES  and new_point['home_score'] in  HitParadeMatchDetailsSofaReformatter.GAME_POINT_VALUES)
                if  new_point['tiebreaker_point']:
                    new_point['serving'] = None
                    new_point['broken_point'] = None
                else:
                    new_point['serving'] = serving
                    new_point['broken_point'] = not new_point['serving'] == new_point['winner']
                if new_point['winner'] is None and len(all_points) > 0:
                    new_point['winner'] = 'home' if self.__get_idx(list_value=HitParadeMatchDetailsSofaReformatter.GAME_POINT_VALUES, indx_val=new_point['home_score']) > self.__get_idx(list_value=HitParadeMatchDetailsSofaReformatter.GAME_POINT_VALUES, indx_val=all_points[-1]['home_score']) else 'away'
                elif new_point['winner'] is None and len(all_points) == 0:
                    new_point['winner'] = 'home' if self.__get_idx( list_value=HitParadeMatchDetailsSofaReformatter.GAME_POINT_VALUES, indx_val=new_point['home_score']) > self.__get_idx( list_value=HitParadeMatchDetailsSofaReformatter.GAME_POINT_VALUES, indx_val=new_point['away_score']) else 'away'
                if len(new_point) > 0:
                    all_points.append(new_point)
        return all_points

    def __parse_broken_served_values(self, class_listing=None):
        serving_stats = self.__parse_list_of_lists(class_listing=class_listing, meta_data=['u-text-orange', 'served'])
        return 'away' if serving_stats.get('served', -1) == 0 else 'home' if serving_stats.get('served', -1) == 1 else None, serving_stats.get('u-text-orange', -1) > -1, 'away' if serving_stats.get('u-text-orange', -1) == 0 else 'home' if serving_stats.get('u-text-orange', -1) == 1 else None

    def __parse_game_content(self, div_content=None, set_number=None):
        new_game = None
        if 'div.pbp > div.pbp__game-row > div.pbp__game' in div_content.keys():
            new_game = dict()
            new_game['set_number'] = set_number
            new_game['serving'],new_game['broken_serve'], new_game['broken_serve_by']  = self.__parse_broken_served_values(class_listing=div_content.get('class', []))
            new_game['winner'] = 'away' if (new_game['serving'] == 'away' and not new_game['broken_serve']) or (new_game['serving'] == 'home' and new_game['broken_serve']) else 'home' if (new_game['serving'] == 'home' and not new_game['broken_serve']) or (new_game['serving'] == 'away' and new_game['broken_serve']) else None
            new_game['points'] = self.__parse_point_content(point_content=div_content.get('div.pbp > div.pbp__game-row > div.pbp__game', []), serving=new_game['serving'], set_number=set_number)
            if len(new_game['points']) > 0 and not new_game['points'][-1]['tiebreaker_point']:
                final_point = dict()
                final_point['winner'] = 'home' if (new_game['serving'] == 'home' and not new_game['broken_serve']) or ( new_game['serving'] == 'away' and new_game['broken_serve']) else 'away'
                if not final_point['winner'] == new_game['winner']:
                    new_game['winner'] = final_point['winner']
                final_point['point_number'] = new_game['points'][-1]['point_number'] + 1
                final_point['tiebreaker_point'] = False
                final_point['serving'] = new_game['serving']
                final_point['set_number'] = set_number
                final_point['home_score' if final_point['winner'] == 'home' else 'away_score'] = 'X'
                final_point['away_score' if final_point['winner'] == 'home' else 'home_score'] = new_game['points'][-1]['away_score' if final_point['winner'] == 'home' else 'home_score']
                new_game['points'].append(final_point)
            if new_game['winner'] is None:
                new_game['winner'] = new_game['points'][-1].get('winner', None)
        return new_game

    def __pbp_listing(self, div_list=None):
        print(div_list)
        set_title = None
        set_number = None
        all_sets = []
        set_values = None
        for div_content in div_list:
            idx_title = self.__get_idx(list_value=div_content.get('class', []), indx_val='pbp-set-title')
            if idx_title > -1:
                if set_values and len(set_values)>0:
                    all_sets.append(set_values)
                set_values = dict()
                set_title = div_content.get('text', {}).get('text', None)
                set_number = self.__title_to_index(set_title)
            elif idx_title == -1:
                set_values['set_number'] = set_number
                set_values['set_title'] = set_title
                set_values['games'] = [] if set_values.get('games', None) is None else set_values['games']
                game_value = self.__parse_game_content(div_content=div_content, set_number=set_number)
                try:
                    game_value['away_score'] =  div_content.get('div.pbp > div.pbp__setcell > div.pbp__setcell-content', [])[0].get('text', None)
                except:
                    pass
                try:
                    game_value['home_score'] =  div_content.get('div.pbp > div.pbp__setcell > div.pbp__setcell-content', [])[1].get('text', None)
                except:
                    pass
                set_values['games'].append(game_value)


        if set_values and len(set_values)>0:
            all_sets.append(set_values)
        total_game_number  = 0
        total_point_number = 0
        current_set_streak = 1
        current_game_streak = 1
        current_point_streak = 1
        away_game_wins = 0
        home_game_wins = 0
        away_point_wins = 0
        home_point_wins = 0

        last_set_winner = None
        last_game_winner = None
        last_point_winner = None
        for i in range(len(all_sets)):
            s = dict()
            try:
                s = list(filter(lambda x: x['set_number'] == (i+1), all_sets))[0]
            except:
                pass
            s['games'] = s['games'][::-1]
            s['winner'] = s['games'][-1]['points'][-1]['winner']
            if i > 0:
                if s['winner'] == last_set_winner:
                    current_set_streak += 1
                else:
                    current_set_streak = 1
            s['current_set_streak'] = current_set_streak
            last_set_winner = s['winner']
            for idx, ss in enumerate(s['games']):
                ss['game_number'] = idx+1
                total_game_number += 1
                ss['total_game_number'] = total_game_number
                if i > 0 or idx > 0:
                    if ss['winner'] == last_game_winner:
                        current_game_streak += 1
                    else:
                        current_game_streak = 1
                ss['current_game_streak'] = current_game_streak
                last_game_winner = ss['winner']
                if ss['winner'] == 'away':
                    away_game_wins += 1
                elif ss['winner'] == 'home':
                    home_game_wins += 1
                ss['home_game_wins'] = home_game_wins
                ss['away_game_wins'] = away_game_wins
                for jdx, p in enumerate(ss['points']):
                    total_point_number += 1
                    p['total_point_number'] = total_point_number
                    if i > 0 or idx > 0 or jdx > 0:
                        if p['winner'] == last_point_winner:
                            current_point_streak += 1
                        else:
                            current_point_streak = 1
                    p['current_point_streak'] = current_point_streak
                    last_point_winner = p['winner']
                    if p['winner'] == 'home':
                        home_point_wins += 1
                    elif p['winner'] == 'away':
                        away_point_wins += 1
                    p['home_point_wins'] = home_point_wins
                    p['away_point_wins'] = away_point_wins
        return all_sets, total_game_number, total_point_number, away_game_wins, home_game_wins, away_point_wins, home_point_wins

    def __set_listing(self, div_list=None):
        all_sets =  []
        if len(div_list) > 1:
            for idx, obj_val in enumerate(div_list[0].get('thead > tr > th', [])):
                new_set = dict()
                new_set['set_name'] = obj_val.get('text', None).strip()
                class_values = div_list[1].get('class', [])
                index_winner = -1
                if class_values and len(class_values) > 0:
                    index_winner = self.__get_idx(list_value=class_values[(2 * idx):(2 * idx)+2], indx_val=['winner'])
                    new_set['winner'] = 'away' if index_winner == 0 else 'home' if index_winner > -1 else None
                new_set['away_score'], new_set['away_tiebreak_score'], new_set['tiebreak'] = self.__reformat_set_score( set_score=div_list[1].get('tbody > tr > td > span', [])[(2 * idx)].get('text', None))
                new_set['home_score'], new_set['home_tiebreak_score'], new_set['tiebreak'] = self.__reformat_set_score(set_score=div_list[1].get('tbody > tr > td > span', [])[((2 * idx) + 1)].get('text', None))
                if len(div_list) == 3:
                    new_set['set_time'] = div_list[2].get('tfoot > tr > th', [])[idx].get('text', None)
                    new_set['set_time'] = div_list[2].get('tfoot > tr > th', [])[idx].get('text', None)
                    new_set['hour_string'], new_set['minute_string'], new_set['hour_value'], new_set['minute_value'], new_set['total_time'] = self.__date_time_reformat(date_time=new_set['set_time'])
                new_set['set_number'] = (idx + 1)
                if not new_set['away_score'] is None and not new_set['home_score'] is  None:
                    all_sets.append(new_set)
        return all_sets

    def __parse_vote_data(self, voting_data=None):
        vote_data = dict()
        for idx,vd in enumerate(voting_data):
            key_val = 'away'
            if idx == 1:
                key_val = 'home'
            vote_data[key_val+'_percentage'] = vd.get('data-width', None)
            vote_data[key_val+'_votes'] = int(vd.get('div.cell__content > div.vote__count', [0])[0].get('text', None))
            percentage_to_parse = vd.get('div.cell__content > span.vote__pct', [0])[0].get('text', None)
            if percentage_to_parse is None or percentage_to_parse == '':
                percentage_to_parse = vd.get('data-width', None)
            vote_data[key_val+'_percentage_value'] = float(percentage_to_parse[0:-1])
        vote_data['total_votes'] = vote_data['away_votes'] + vote_data['home_votes']
        return vote_data

    def __datefrom_datestring(self, sprop=None, date_time_string=None, parse_value=None):
        dict_vals = dict()
        if sprop:
            dict_vals[sprop] = date_time_string
        dict_vals['start_date_value'] = datetime.strptime(date_time_string, parse_value)
        dict_vals['start_date_value'] = pytz.utc.localize(dict_vals['start_date_value'])
        dict_vals['start_date_month'] = dict_vals['start_date_value'].month
        dict_vals['start_date_year'] = dict_vals['start_date_value'].year
        dict_vals['start_date_day'] = dict_vals['start_date_value'].day
        dict_vals['start_date_hour'] = dict_vals['start_date_value'].hour
        dict_vals['start_date_minute'] = dict_vals['start_date_value'].minute
        dict_vals['start_date_day_of_week'] = dict_vals['start_date_value'].strftime('%A')
        return dict_vals

    def __parse_venue(self, venue_details=None):
        dict_vals = dict()
        if venue_details:
            for idx,venue_detail in enumerate(venue_details):
                if venue_detail.get('text', None):
                    venue_value = venue_detail.get('text', None).strip().lower()
                    if 'surface:' in venue_value:
                        dict_vals['surface'] = venue_value.split('surface:')[1].strip()
                    elif 'location:' in venue_value:
                        dict_vals['location'] = venue_value.split('location:')[1].strip()
                        if 'court' in dict_vals['location']:
                            dict_vals['court'] = dict_vals['location'].split('court')[1].strip()
                        if 'venue' in dict_vals['location']:
                            dict_vals['venue_string'] = dict_vals['location'].split('venue')[0].strip()
                    elif 'start date:' in venue_value:
                        dict_vals.update(self.__datefrom_datestring(sprop='start_date',date_time_string=venue_value.split('start date:')[1].strip(), parse_value='%d. %b %Y, %H:%M' ))
        return dict_vals

    def __parse_statistics(self, statistics_values=None):
        dict_values = dict()
        if statistics_values:
            for idx, tab in enumerate(statistics_values.get('div.js-event-page-statistics-container > div.statistics-container > ul.nav > li.nav__item > a', None)):
                if tab.get('text', None):
                    object_value = None
                    stats_object = statistics_values.get('div.js-event-page-statistics-container > div.statistics-container > div.statistics > div[id^=statistics-period-]', None)[idx]
                    if stats_object:
                        if dict_values.get(tab.get('text', None)) is None:
                            dict_values[tab.get('text', None).strip().lower()] = dict()
                        for stat_val in stats_object.get('div.stat-group-event > div.cell--incident', []):
                            object_value = dict()
                            object_value['away_stat'] = stat_val.get('div[class^=cell__section] > div.cell__content', None)[0].get('text', None)
                            object_value['stat'] =      stat_val.get('div[class^=cell__section] > div.cell__content', None)[1].get('text', None)
                            object_value['home_stat'] = stat_val.get('div[class^=cell__section] > div.cell__content', None)[2].get('text', None)
                            object_value['stat'] = '_'.join(object_value['stat'].split(' '))
                            away_stat_num, away_stat_denom, away_stat_percent = self.__reformat_numbers(numb_str=object_value['away_stat'])
                            home_stat_num, home_stat_denom, home_stat_percent = self.__reformat_numbers(numb_str=object_value['home_stat'])
                            if not away_stat_percent is None:
                                object_value['away_' + object_value['stat'].strip().lower() +'_percentage'] = away_stat_percent
                                object_value['away_' + object_value['stat'].strip().lower() +'_success'] = away_stat_num
                                object_value['away_' + object_value['stat'].strip().lower() +'_totals'] = away_stat_denom
                                object_value['away_' + object_value['stat'].strip().lower() + '_value'] = None
                            else:
                                object_value['away_' + object_value['stat'].strip().lower() + '_percentage'] = None
                                object_value['away_' + object_value['stat'].strip().lower() + '_success'] = None
                                object_value['away_' + object_value['stat'].strip().lower() + '_totals'] = None
                                object_value['away_' + object_value['stat'].strip().lower() +'_value'] = away_stat_num
                            if not home_stat_percent is None:
                                object_value['home_' + object_value['stat'].strip().lower() + '_percentage'] = home_stat_percent
                                object_value['home_' + object_value['stat'].strip().lower() + '_success'] = home_stat_num
                                object_value['home_' + object_value['stat'].strip().lower() + '_totals'] = home_stat_denom
                                object_value['home_' + object_value['stat'].strip().lower() + '_value'] = None
                            else:
                                object_value['home_' + object_value['stat'].strip().lower() + '_percentage'] = None
                                object_value['home_' + object_value['stat'].strip().lower() + '_success'] = None
                                object_value['home_' + object_value['stat'].strip().lower() + '_totals'] = None
                                object_value['home_' + object_value['stat'].strip().lower() + '_value'] = home_stat_num


                            if ' ' in object_value['stat']:
                                object_value['stat'] = '_'.join([x.strip().lower() for x in object_value['stat'].split(' ')])
                            else:
                                object_value['stat'] = object_value['stat'].strip().lower()
                            if object_value:
                                dict_values[tab.get('text', None).strip().lower()][object_value['stat'].strip().lower()] = object_value
        return dict_values

    def __reformat_numbers(self, numb_str=None):
        if numb_str:
            if '/' in numb_str and '(' in numb_str and ')' in numb_str and '%' in numb_str:
                return int(numb_str.split('/')[0].strip()), int(numb_str.split('/')[1].split('(')[0].strip()), int(numb_str.split('(')[1].split('%')[0].strip())
            elif '/' in numb_str and '(' in numb_str and ')' in numb_str and not '%' in numb_str:
                return int(numb_str.split('/')[0].strip()), int(numb_str.split('/')[1].split('(')[0].strip()), int(numb_str.split('(')[1].split('%')[0].strip())
            elif numb_str.isdigit():
                return int(numb_str), None, None

    def __parse_odds(self, odds=None):

        dict_values = dict()
        dict_values['away_value_american'] = None
        dict_values['home_value_american'] = None
        dict_values['away_value_american_value'] = None
        dict_values['home_value_american_value'] = None
        dict_values['away_value_decimal'] = None
        dict_values['away_value_decimal_value'] = None
        dict_values['home_value_decimal'] = None
        dict_values['home_value_decimal_value'] = None
        dict_values['away_value_fractional'] = None
        dict_values['away_value_fractional_numerator'] = None
        dict_values['away_value_fractional_denominator'] = None
        dict_values['home_value_fractional'] = None
        dict_values['home_value_fractional_numerator'] = None
        dict_values['home_value_fractional_denominator'] = None

        if odds and isinstance(odds, list):
            object_value = odds[0].get('table.odds__table',[])[1].get('tbody > tr > td > a > div > span > span', [])
            class_values = odds[0].get('table.odds__table',[])[1].get('class', [])
            for idx, c in enumerate(class_values):
                dict_values['away_' + '_'.join(c[0].split('-')[2:]) if idx < 3 else 'home_' + '_'.join(c[0].split('-')[2:])] = object_value[idx].get('text', None)
        vals_dict = dict()
        for k in dict_values.keys():
            if not dict_values[k] is None and not dict_values[k] == '':
                if not 'NaN' in dict_values[k]:
                    if 'american' in k:
                            vals_dict[k+'_value'] = int(dict_values[k])
                    elif 'decimal' in k:
                        vals_dict[k + '_value'] = float(dict_values[k])
                    else:
                        if '/' in dict_values[k]:
                            vals_dict[k + '_numerator'] = int(dict_values[k].split('/')[0].strip())
                            vals_dict[k + '_denominator'] = int(dict_values[k].split('/')[1].strip())
        dict_values.update(vals_dict)
        return dict_values

    def reformat(self, data=None):
        meta_data_values = None
        try:
            if data:
                root_css = data.get('div#pjax-container-main', [])
                meta_data_values = dict()
                meta_data_values['collection'] = data.get('collection', None)
                meta_data_values['current_url'] = data.get('current_url', None)
                meta_data_values['database'] = data.get('database', None)
                meta_data_values['document_id'] = data.get('document.id', None)
                meta_data_values['filename'] = data.get('filename', None)
                meta_data_values['forwarded'] = data.get('forwarded', None)
                meta_data_values['hash'] = data.get('hash', None)
                meta_data_values['hash_wurl'] = data.get('hash_wurl', None)
                meta_data_values['id'] = data.get('id', None)
                meta_data_values['parent_id'] = data.get('parent_id', None)
                meta_data_values['scraper_url'] = data.get('scraper_url', None) if not '?' in data.get('scraper_url', None) else  data.get('scraper_url', None).split('?')[0]
                for root_object in root_css:
                    meta_data_values.update(
                        self.__reformat_categories(meta_data=root_object.get('div > div.breadcrumb-container', [])))
                    meta_data = root_object.get('div > div.page-container > div.l__grid.js-page-layout', [])
                    for obj in meta_data:
                        div_list = obj.get('div', [])
                        for div in div_list:
                            if 'div.odds-wrapper-details > div.js-event-page-odds-container > div.rel > div.odds__group' in div.keys():
                                meta_data_values['odds'] = self.__parse_odds(odds=div.get(
                                    'div.odds-wrapper-details > div.js-event-page-odds-container > div.rel > div.odds__group',None))
                            if 'div.js-event-page-statistics-container > div.statistics-container > ul.nav > li.nav__item > a' in div.keys():
                                meta_data_values['statistics'] = self.__parse_statistics(statistics_values=div)
                            if 'div#event-info-root > div > div > p' in div.keys():
                                meta_data_values['venue'] = self.__parse_venue(venue_details=div.get('div#event-info-root > div > div > p', []))
                            if 'div.js-event-page-vote-container > div.vote-container > div.vote > div.vote__stats > div.vote__team' in div.keys():
                                meta_data_values['votes'] = self.__parse_vote_data(voting_data=div.get('div.js-event-page-vote-container > div.vote-container > div.vote > div.vote__stats > div.vote__team', None))
                            if 'div.js-event-page-incidents-container > div.card__content > div.pbp-wrapper > div' in div.keys():
                                meta_data_values['sets_merge'], meta_data_values['total_game_number'], meta_data_values['total_point_number'], meta_data_values['away_game_wins'], meta_data_values['home_game_wins'], meta_data_values['away_point_wins'], meta_data_values['home_point_wins'] = self.__pbp_listing(div.get('div.js-event-page-incidents-container > div.card__content > div.pbp-wrapper > div',[]))
                            if 'div.js-event-page-rounds-container > div.js-event > div.rounds-container > table.table--rounds' in div.keys():
                                meta_data_values['sets'] = self.__set_listing(div_list=div.get('div.js-event-page-rounds-container > div.js-event > div.rounds-container > table.table--rounds', []))
                            if 'div.js-details-component-startTime-container > div.cell > div.cell__section >  div.cell__content' in div.keys():
                                match_values = div.get('div.js-details-component-startTime-container > div.cell > div.cell__section >  div.cell__content', [])
                                if len(match_values) > 0:
                                    meta_data_values['match_start'] = match_values[0].get('text', None)
                                    if meta_data_values['match_start'] and 'Today' in meta_data_values['match_start']:
                                        meta_data_values['match_start_date'] = datetime.today().strftime('%Y-%m-%d')
                                    meta_data_values['match_length'] = match_values[1].get('text', None)
                                    meta_data_values['total_match_minutes'] = 0
                                    meta_data_values['hour_string'], meta_data_values['minute_string'], meta_data_values['hour_value'], meta_data_values['minute_value'], meta_data_values['total_match_minutes'] = self.__date_time_reformat(date_time=meta_data_values['match_length'])
                            if 'page-header-container' in div.get('class', []):
                                # img of players
                                meta_data_values.update(self.__parse_player_score_data(player_scores=div.get('div.js-event-page-header-container > div.details__score-cell', [])))

                self.__reformat_player_ids(meta_data_values=meta_data_values)
                if meta_data_values.get('sets_merge', None) and meta_data_values.get('sets', None):
                    set_dict = dict()
                    for x, y in zip(meta_data_values['sets'], meta_data_values['sets_merge']):
                        if set_dict.get(x['set_number'], None) is None:
                            set_dict[x['set_number']] = x
                        else:
                            set_dict[x['set_number']].update(x)
                        if set_dict.get(y['set_number'], None) is None:
                            set_dict[y['set_number']] = y
                        else:
                            set_dict[y['set_number']].update(y)
                    del meta_data_values['sets_merge']
                    del meta_data_values['sets']
                    meta_data_values['sets'] = [set_dict[x] for x in set_dict.keys()]

            for k in meta_data_values.get('statistics', {}).keys():
                if k == 'all':
                    meta_data_values.update(meta_data_values['statistics'][k])
                else:
                    set_index = self.__title_to_index(k)
                    if set_index > -1:
                        list(filter(lambda x: x['set_number'] == set_index, meta_data_values['sets']))[0].update(meta_data_values['statistics'][k])
            if not meta_data_values.get('statistics', None) is None:
                del meta_data_values['statistics']


        except:
            traceback.print_exc()
        return meta_data_values


    class Factory:
        def create(self, **kwargs): return HitParadeMatchDetailsSofaReformatter(**kwargs)
