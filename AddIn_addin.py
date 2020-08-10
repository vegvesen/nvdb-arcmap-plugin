from __future__ import (print_function, unicode_literals, division)

import arcpy
import pythonaddins
import sys
from os.path import dirname, join

# Added this to make relative imports possible
_cwd = unicode(dirname(__file__))
sys.path.append(_cwd)

class HentData(object):
    """Implementation for AddIn_addin.Nvdb_get_objects (Button)"""
    def __init__(self):
        self.enabled = True
        self.checked = False
    def onClick(self):
        tbx_path = join(_cwd, 'NVDB-API.pyt')
        pythonaddins.GPToolDialog(tbx_path, 'HentData')

class LagMetrertVegnett(object):
    """Implementation for AddIn_addin.Nvdb_get_objects (Button)"""
    def __init__(self):
        self.enabled = True
        self.checked = False
    def onClick(self):
        tbx_path = join(_cwd, 'NVDB-API.pyt')
        pythonaddins.GPToolDialog(tbx_path, 'LagMetrertVegnett')
