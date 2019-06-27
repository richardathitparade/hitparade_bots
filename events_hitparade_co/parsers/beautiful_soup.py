from events_hitparade_co.parsers.parser import HitParadeParser
from bs4 import BeautifulSoup
from bs4.element import NavigableString
import traceback
import html2text
import hashlib
class BeautifulSoupParser(HitParadeParser):
    """
    HitParadeParser that combines Selenium with BeatifulSoup framework.
    BeautifulSoup parses much faster than Selenium on average.
    BeautifulSoup cannot click buttons etc.
    """
    def __init__(self,**kwargs):
        """
        Constructor
        :param kwargs: dict data to retrieve info if necessary.
        """
        super().__init__(**kwargs)
        self.driver = kwargs.get('driver', None)
        if self.driver:
            html = self.driver.page_source
            self.soup = BeautifulSoup(html, "html.parser")
        else:
            self.soup = None
        self.last_hashes = []

    def get_last_hash(self):
        if len(self.last_hashes) > 0:
            return self.last_hashes[-1]
        return None

    def reload_content(self):
        """
        Reload content
        :return:
        """
        try:
            if not self.soup is None:
                old_soup = self.soup
                del old_soup
            try:
                html = self.driver.page_source
                self.soup = BeautifulSoup(html, "html.parser")
                return True
            except:
                traceback.print_exc()
                return False
        except:
            traceback.print_exc()
            return False


        # last_hash_value = hashlib.md5( html2text.html2text(html).encode()).hexdigest()
        # last_hash = self.get_last_hash()
        # if last_hash == last_hash_value:
        #     print('hashes match at %s ' % last_hash)
        #     return False
        # else:
        #     print('hashes do not match %s != %s' % (last_hash, last_hash_value))
        #     self.soup = BeautifulSoup(html, "html.parser")
        #     return True

    def selector_trim(self, selector=None):
        selector_trim = None
        if not selector is None:
            selector_trim = ''.join( selector.split(' ') )
        return selector_trim

    def get_text(self, css_selector=None, is_multiple=False, sub_element=None, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        element_dict = dict()
        element = self.get_elements(css_selector=self.selector_trim(selector=css_selector), is_multiple=is_multiple, sub_element=sub_element,log_exceptions=log_exceptions )
        if not element is None and not element.get('element', None) is None:
            if is_multiple:
                element_dict['text'] = []
                for e in element.get('element' , []):
                    try:
                        element_dict['text'].append({'text' : str(e.text).strip()})
                    except:
                        if log_exceptions:
                            traceback.print_exc()
            else:
                try:
                    element_dict['text'] = str(element.get('element', None).text).strip()
                except:
                    if log_exceptions:
                        traceback.print_exc()
        return element_dict

    def get_elements(self, css_selector=None, sub_element=None, is_multiple=False, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        element_dict = dict()
        try:
            if is_multiple:
                if sub_element is None:
                    try:
                        element_dict['element'] = self.soup.select(self.selector_trim(selector=css_selector))
                    except:
                        if log_exceptions:
                            traceback.print_exc()
                else:
                    try:
                        if not isinstance(sub_element, NavigableString):
                            element_dict['element'] = sub_element.select(self.selector_trim(selector=css_selector))
                    except:
                        if log_exceptions:
                            traceback.print_exc()
                        try:
                            element_dict['element'] = self.soup.select(self.selector_trim(selector=css_selector))
                        except:
                            if log_exceptions:
                                traceback.print_exc()

            else:
                if sub_element is None:
                    try:
                        element_dict['element'] = self.soup.select_one(self.selector_trim(selector=css_selector))
                    except:
                        if log_exceptions:
                            traceback.print_exc()
                else:
                    try:
                        if not isinstance(sub_element, NavigableString):
                            element_dict['element'] = sub_element.select_one(self.selector_trim(selector=css_selector))
                    except:
                        if log_exceptions:
                            traceback.print_exc()
                        try:
                            element_dict['element'] = self.soup.select_one(self.selector_trim(selector=css_selector))
                        except:
                            if log_exceptions:
                                traceback.print_exc()
        except:
            if log_exceptions:
                traceback.print_exc()
        return element_dict


    def get_attributes(self, css_selector=None, sub_element=None,is_multiple=False, attributes=[], log_exceptions=False):
        """
         returns elements based upon css selector.
         :param css_selector: str css selector
         :param sub_element: element if None, do not select off sub_element
         :param attributes: list [] of attributes to retrieve.
         :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
         :return: dict with the property of attributes
         """
        element_dict = dict()
        if len(attributes) > 0:
            element = self.get_elements( css_selector=self.selector_trim(selector=css_selector), sub_element=sub_element,
                                        log_exceptions=log_exceptions,is_multiple=is_multiple )
            if not element is None and not element.get('element', None) is None:
                if is_multiple:
                    for attribute in attributes:
                        element_dict[attribute] = []
                        for e in element.get('element', []):
                            try:
                                if not e is None and not e.get(attribute, None) is None:
                                    element_dict[attribute].append( e[attribute] )
                            except:
                                if log_exceptions:
                                    print('attribute failure is %s ' % attribute )
                                    traceback.print_exc()
                else:
                    for attribute in attributes:
                        try:
                            element_dict[attribute] = str(element.get('element', None)[attribute])
                        except:
                            if log_exceptions:
                                traceback.print_exc()
        return element_dict

    class Factory:
        def create(self, **kwargs): return BeautifulSoupParser(**kwargs)
