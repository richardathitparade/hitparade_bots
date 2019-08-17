from __future__ import generators
import traceback
from events_hitparade_co.boot.boot_data import BootData
from events_hitparade_co.boot.bot_creator import HitParadeBotCreator
import sys
if __name__ == '__main__':
    try:
        try:
            bot_data_array = BootData.get_bots()
            bots = []
            print('The arguments are %s ' % str(sys.argv))
            for bot_data in bot_data_array:
                try:
                    if bot_data.get('active', False):
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
