from events_hitparade_co.boot.bootstrap import HitParadeScrapingBootstrapper
import os
import sys
import traceback
class BootData:

    @staticmethod
    def get_env_variables(override=False):
        bot_data_args = dict()
        command_line_args = os.environ['hpbots']
        if not command_line_args is None:
            itemized_args = command_line_args.split(' ')
            for idx, av in enumerate(itemized_args):
                if idx % 2 == 1:
                    bot_data_args[itemized_args[(idx - 1)]] = itemized_args[idx]

        for i in range(len(sys.argv)):
            if i > 0 and i % 2 == 0:
                if bot_data_args.get(sys.argv[(i - 1)], None) is None or override:
                    bot_data_args[sys.argv[(i - 1)]] = sys.argv[i]
        return bot_data_args

    @staticmethod
    def get_bots(override=False):
        try:
            environment_variables = BootData.get_env_variables()
        except:
            print('hpbots or command line arguments are not found - if this is not a consumer this is a warning')
            traceback.print_exc()

        bots_data = HitParadeScrapingBootstrapper.get_bots()
        if not environment_variables is None:
            if not bots_data is None:
                if isinstance(bots_data, dict):
                    bots_data.update(environment_variables)
                elif isinstance(bots_data, list):
                    for b in bots_data:
                        if not b is None and isinstance(b, dict):
                            b.update(environment_variables)
        return bots_data
