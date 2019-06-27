from events_hitparade_co.url_republishers.republisher import UrlRepublisher
class SofaScoreDetailsUrlRepublisher(UrlRepublisher):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def republish(self, **kwargs):
        json_data = self.load_data_from_string( property=kwargs.get('property', 'data') ,  **kwargs )
        if json_data and isinstance(json_data, dict):
            match_url_dict = dict()
            match_url_dict['id_property'] = json_data.get('id_property', None)
            match_url_dict['parent_id_property'] = json_data.get('parent_id_property', None)
            if  match_url_dict['id_property']:
                match_url_dict[json_data.get('id_property', None)] = json_data.get(json_data.get('id_property', None), None)
            if  match_url_dict['parent_id_property']:
                match_url_dict[json_data.get('parent_id_property', None)] = json_data.get(json_data.get('parent_id_property', None), None)
            match_urls = []
            if not isinstance(json_data, int) and len(json_data.get(self.root_css, [])) > 0:
                for tournament in json_data.get(self.root_css, [])[0].get(self.tournament_css, []):
                    for match in tournament.get(self.match_list_css, [])[0].get( self.match_css, [] ):
                        match_urls.append(self.base_url + match[self.match_attribute])
            match_url_dict['match_urls'] = match_urls
            publish_to_event = json_data.get('publish_to_event', None) if isinstance(json_data.get('publish_to_event', None), str) else json_data.get('publish_to_event', None)[0]
            return match_url_dict, self.data_selector,  publish_to_event
        else:
            print('jsondata is not a dict %s ' % str(json_data))
            return None, None, None


    class Factory:
        """
        Factory class used by the SofaScoreDetailsUrlRepublisher
        """
        def create(self, **kwargs): return SofaScoreDetailsUrlRepublisher( **kwargs )

