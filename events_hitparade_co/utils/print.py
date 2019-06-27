import pprint as pp
import traceback
import json
import hashlib
class PrintUtil:
    """
    PrintUtil pretty print dict or str
    """
    class __PrintUtil:

        """
        Private PrintUtil class for the singleton.
        """
        def __init__(self, indent=4):
            """
            Constructor
            :param indent: int tabs to indent.
            """
            self.pp = pp.PrettyPrinter(indent=indent)

        def pretty_print(self, obj):
            """
            Pretty Print to use
            :param obj: dict or str object to pretty print
            :return:
            """
            try:
                if not obj is None:
                    self.pp.pprint(obj)
            except:
                traceback.print_exc()

        def pretty_print_json(self, obj):
            """
            Pretty Print to use
            :param obj: dict or str object to pretty print
            :return:
            """
            try:
                if not obj is None and isinstance(obj, dict):
                    dict_str = json.dumps(obj)
                    sha_string = hashlib.sha256(dict_str.encode('utf-8')).hexdigest()
                    print('hash is %s and has changed or is first pull' % sha_string)
                    if not obj is None:
                        self.pp.pprint(obj)
            except:
                traceback.print_exc()

        def get_pp_json(self, obj=None, indent=4):
            """
                     Pretty Print to use
                     :param obj: dict or str object to pretty print
                     :return:
                     """
            try:
                if not obj is None and isinstance(obj, dict):
                    dict_str = json.dumps(obj)
                    sha_string = hashlib.sha256(dict_str.encode('utf-8')).hexdigest()
                    print('hash is %s and has changed or is first pull' % sha_string)
                    if not obj is None:
                        return self.pp.pformat(obj, indent=indent)
                    else:
                        return None
            except:
                traceback.print_exc()
                return None


    def __init__(self, indent=4):
        """
        Constructor of PrintUtil initializes the instance
        :param indent:
        """
        if not PrintUtil.instance:
            PrintUtil.instance = PrintUtil.__PrintUtil(indent=indent)
        else:
            PrintUtil.instance.pp = pp.PrettyPrinter(indent=indent)
    instance = __PrintUtil()

    @staticmethod
    def ppr(obj):
        """
        static method to call pretty_print
        :param obj: str or dict to string
        :return:
        """
        if isinstance(obj,str):
            PrintUtil.instance.pretty_print(obj)
        else:
            PrintUtil.instance.pretty_print_json(obj)

    @staticmethod
    def pprj(obj):
        """
        static method to call pretty_print
        :param obj: str or dict to string
        :return:
        """
        if isinstance(obj,str):
            PrintUtil.instance.prety_print(obj)
        else:
            PrintUtil.instance.pretty_print_json(obj)

    @staticmethod
    def get_pprj(self, obj, indent=4):
        return PrintUtil.instance.get_pp_json(obj=obj, indent=indent)

    ppr = staticmethod(ppr)
