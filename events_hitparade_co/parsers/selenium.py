from events_hitparade_co.parsers.parser import HitParadeParser
import traceback
class SeleniumParser(HitParadeParser):
    """
    HitParadeParser that uses Selenium
    """
    def __init__(self,**kwargs):
        """
        Constructor
        :param kwargs: dict data to retrieve info if necessary.
        """
        super().__init__(**kwargs)

    def reload_content(self):
        """
        Reload content
        :return:
        """
        html = self.driver.page_source

    def get_text(self, css_selector=None, sub_element=None, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        element_dict = dict()
        element = self.get_elements(css_selector=css_selector, sub_element=sub_element,log_exceptions=log_exceptions, is_multiple=False)
        if not element is None and not element.get('element', None) is None:
            try:
                element_dict['text'] = element.get('element', None).text
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
                    element_dict['element'] = self.driver.find_elements_by_css_selector(css_selector)
                else:
                    element_dict['element'] = sub_element.find_elements_by_css_selector(css_selector)
            else:
                if sub_element is None:
                    element_dict['element'] = self.driver.find_element_by_css_selector(css_selector)
                else:
                    element_dict['element'] = sub_element.find_element_by_css_selector(css_selector)
        except:
            if log_exceptions:
                traceback.print_exc()
        return element_dict

    def get_attributes(self, css_selector=None, sub_element=None, is_multiple=False, attributes=[], log_exceptions=False):
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
            element = self.get_elements(css_selector=css_selector, sub_element=sub_element, log_exceptions=log_exceptions, is_multiple=is_multiple)
            if not element is None and not element.get('element' , None) is None:
                for attribute in attributes:
                    try:
                        element_dict[attribute] = element.get('element', None).get_attribute(attribute)
                    except:
                        if log_exceptions:
                            traceback.print_exc()
        return element_dict

    class Factory:
        def create(self, **kwargs): return SeleniumParser(**kwargs)