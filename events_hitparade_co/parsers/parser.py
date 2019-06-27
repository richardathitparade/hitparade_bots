from abc import abstractmethod
from events_hitparade_co.registration.registration import RegisterLeafClasses
class HitParadeParser:
    __metaclass__ = RegisterLeafClasses
    """
    This is the base class of all ScraperComponent html parsers.
    """
    def __init__(self, **kwargs):
        """
        Constructor
        :param kwargs: dict data to use
        """
        self.driver = kwargs.get('driver', None)

    @abstractmethod
    def reload_content(self):
        """
        Reloads content for the parser if a url changes.
        :return:
        """
        pass

    @abstractmethod
    def get_text(self, css_selector=None, sub_element=None, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        pass

    @abstractmethod
    def get_elements(self, css_selector=None,sub_element=None ,is_multiple=False, log_exceptions=False):
        """
        returns elements based upon css selector.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param is_multiple: bool True if multiple elements and False if only one element.
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of element
        """
        pass

    @abstractmethod
    def get_attributes(self, css_selector=None, sub_element=None, attributes=[],is_multiple=False, log_exceptions=False):
        """
        returns elements based upon css selector.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param attributes: list [] of attributes to retrieve.
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of attributes
        """
        pass

    def get_atomic_element(self,scrape_types=['text'],selector=None, iframe_index=0, attributes=[],sub_element=None,iframe_css='iframe',is_multiple=None,log_exceptions=False):
        """
        returns a dict of all tye types of selections based upon css selector and sub element and/or iframe.
        :param scrape_types: list[] types to retrieve    text,attributes,element(s),iframe
        :param selector: str css selector
        :param iframe_index: int index of iframe if necessary
        :param attributes: list[] list of attributes to retrieve if necessary
        :param sub_element: element to select against.  do not select against it if it is None
        :param iframe_css: str css to find the iframe.
        :param log_exceptions: bool True if exceptions should be logged and false otherwise.
        :return:
        """
        vals = dict()
        ismultiple = 'elements' in scrape_types if is_multiple is None else is_multiple
        if 'iframe' in scrape_types:
            iframe_value = self.get_elements(css_selector=iframe_css, sub_element=sub_element, log_exceptions=log_exceptions, is_multiple=False)
            se = sub_element
            if not iframe_value is None and len(iframe_value.items()) > 0:
                se = None
                self.driver.switch_to.frame(iframe_index)
            elements_values = self.get_atomic_element(scrape_types=[x for x in scrape_types if not x == 'iframe'], selector=selector, attributes=attributes, sub_element=se, iframe_css=iframe_css, log_exceptions=log_exceptions)
            vals.update(elements_values)
            if not iframe_value is None and len(iframe_value.items()) > 0:
                self.driver.switch_to.default_content()
        if 'text' in scrape_types:
            text_values = self.get_text(css_selector=selector, is_multiple=ismultiple, sub_element=sub_element, log_exceptions=log_exceptions)
            vals.update(text_values)
        if 'attributes' in scrape_types or len(attributes) > 0:
            attribute_values = self.get_attributes(css_selector=selector, sub_element=sub_element, is_multiple=ismultiple, attributes=attributes, log_exceptions=log_exceptions)
            vals.update(attribute_values)
        if 'element' in scrape_types:
            element_value = self.get_elements(css_selector=selector, sub_element=sub_element, log_exceptions=log_exceptions, is_multiple=False)
            vals.update(element_value)
        if 'elements' in scrape_types:
            elements_values = self.get_elements(css_selector=selector, sub_element=sub_element, log_exceptions=log_exceptions, is_multiple=True)
            vals.update(elements_values)
        return vals

