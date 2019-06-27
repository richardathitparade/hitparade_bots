from events_hitparade_co.output.output import HitParadeOutput
from  ..utils.print import PrintUtil
class HitParadeDefaultOuput(HitParadeOutput):
    """
    The default HitParadeOutput object.
    This pretty prints data to the commandline unless the data is none.  Then it just prints 'None'
    """
    def __init__(self,**kwargs):
        """
        Pull data for an output from kwargs
        :param kwargs: Data Dictionary passed to HitParadeOutput
        """
        super().__init__(**kwargs)

    def output(self,output=None):
        """
        overridden output command for the HitParadeOutput object.
        :param output: dict or str object to process.
        :return: none
        """
        if output is None:
            print('None')
        else:
            PrintUtil.pprj(output)

    class Factory:
        """
        Factory class used by the HitParadeOutputFactory
        """
        def create(self, **kwargs): return HitParadeDefaultOuput(**kwargs)