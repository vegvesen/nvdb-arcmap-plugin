"""
Custom logging module.

"""
from csv import writer, QUOTE_NONNUMERIC
from datetime import datetime
from logging import Handler
from os.path import join


class CSVLogHandler(Handler):
    """
    Custom logging handler. Writes logging information to a CSV file.

    """
    def __init__(self, out_workspace=r'.'):
        Handler.__init__(self)

        # Create table.
        datestr = datetime.now().strftime('%Y_%m_%d_%H%M%S')
        self._table = 'ERRLOG_{0}.csv'.format(datestr)
        self.log_table = join(out_workspace, self._table)
        self._fobj = open(self.log_table, 'wb')
        self._out = writer(self._fobj, quoting=QUOTE_NONNUMERIC)
        self._out.writerow(('OBJECTID_1', 'MESSAGE'))
        self.count = 0

    def __enter__(self):
        return self

    def emit(self, record):
        vals = self.format(record).split(' - ')
        self._out.writerow(vals)
        self.count += 1

    def __exit__(self, type, value, traceback):
        self._fobj.close()
