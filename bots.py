from __future__ import generators
import traceback
from events_hitparade_co.boot.boot_data import BootData
from events_hitparade_co.boot.bot_creator import HitParadeBotCreator
import sys
import os
def get_env_variables(override=False):
    bot_data_args = dict()
    command_line_args = os.environ['hpbots']
    if not command_line_args is None:
        itemized_args = command_line_args.split(' ')
        for idx, av in enumerate(itemized_args):
            if idx % 2 == 1:
                bot_data_args[itemized_args[(idx-1)]] = itemized_args[idx]

    for i in range(len(sys.argv)):
        if i > 0 and i % 2 == 0:
            if bot_data_args.get(sys.argv[(i - 1)], None) is None or override:
                bot_data_args[sys.argv[(i - 1)]] = sys.argv[i]
    return bot_data_args

if __name__ == '__main__':
    try:
        try:
            bot_data_array = BootData.get_bots()
            bots = []
            print('The arguments are %s ' % str(sys.argv))
            for bot_data in bot_data_array:
                try:
                    if bot_data.get('active', False):
                        bot_data.update(get_env_variables(override=False))
                        bots.append(HitParadeBotCreator.bot(**bot_data))
                    else:
                        print('bot data is not active - no action necessary %s ' % str(bot_data))
                    del bot_data
                except:
                    traceback.print_exc()
            del bot_data_array
        except:
            traceback.print_exc()
    except:
        traceback.print_exc()
