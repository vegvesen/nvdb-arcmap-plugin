import sys
from os.path import dirname, join

MODULES = ['hent_data_pyt', 'lag_metrert_vegnett_pyt']
for module in MODULES:
    if module in sys.modules:
        del sys.modules[module]

from hent_data_pyt import HentData
from lag_metrert_vegnett_pyt import LagMetrertVegnett

_cwd = dirname(__file__.split('#')[0])

HentData.set_config_meta(_cwd)
LagMetrertVegnett.set_config_meta(_cwd)

TOOLS = [HentData, LagMetrertVegnett]

class Toolbox(object):
    """
    NVDB-API Toolbox class. Modify the tools property as tools are added
    or removed.

    """
    def __init__(self):
        self.label = 'NVDB-API Toolbox'
        self.alias = 'NVDB-API'
        # self.alias = 'nvdbapi'        
        self.description = 'The NVDB-API Toolbox can get stuff from NVDB'

        self.tools = TOOLS
