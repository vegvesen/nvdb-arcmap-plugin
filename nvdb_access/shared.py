# -*- coding: utf-8 -*-
import sys
from os.path import join
import json
if sys.version_info[0] < 3:
    from urllib2 import Request
    from urllib2 import HTTPError
    from urllib2 import quote
    from urllib2 import urlopen
    from urlparse import urljoin, urlparse, urlunparse
    from urllib import urlencode
else:
    from urllib.request import Request
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.request import urlopen
    from urllib.parse import urljoin, urlparse, urlunparse
    from urllib.parse import urlencode

import requests
from requests.packages.urllib3.util import Retry
from requests.adapters import HTTPAdapter
from requests import Session

import urllib
import pylab as pl
from collections import OrderedDict
from itertools import chain
from copy import deepcopy
import re
import datetime as dtm
import logging
from logging import Handler
import arcpy

logger = logging.getLogger(__name__)
SESSION = None

class ArcHandler(Handler):
    def __init__(self):
        """
        Initialize the handler.

        """
        Handler.__init__(self)

    def flush(self):
        """
        Flushes the stream.
        """
        pass

    def emit(self, record):
        """
        Emit a record.
        """
        try:
            msg = self.format(record)
            # fs = "%s\n"
            if record.levelno in [10, 20]:
                arcpy.AddMessage(msg)
            elif record.levelno == 30:
                arcpy.AddWarning(msg)
            elif record.levelno in [40, 50]:
                arcpy.AddError(msg)
        except:
            self.handleError(record)


def init_logging(log_file=None, use_arc=True, verbose=True):
    # Init logger
    root_logger = logging.getLogger()
    # Clear handlers, just in case.
    root_logger.handlers = []
    root_logger.setLevel(logging.DEBUG)

    if log_file:
        fh = logging.FileHandler(log_file)
        if verbose:
            fh.setLevel(logging.DEBUG)
        else:
            fh.setLevel(logging.WARNING)
        fformatter = logging.Formatter('%(asctime)s %(name)-25s: %(levelname)-8s %(message)s',
                                       datefmt='%d-%m-%y %H:%M:%S')
        fh.setFormatter(fformatter)
        root_logger.addHandler(fh)

    # define a Handler which writes INFO messages or higher to chosen stream
    if use_arc:
        console = ArcHandler()
        # set a format which is super simple for ArcMap use
        formatter = logging.Formatter('%(message)s')
    else:
        console = logging.StreamHandler()
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')

    if verbose:
        console.setLevel(logging.DEBUG)
    else:
        console.setLevel(logging.INFO)
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handlers to the root logger

    root_logger.addHandler(console)


def post_proc_extracted(cfg, extracted):
    lookup = {}
    # for k, v in cfg['extract_name_lut'].iteritems():
    for k, v in cfg['extract_name_lut'].items():
        key = cfg['extras_default'].get(k, k)
        lookup[key] = v
        # arcpy.AddMessage('(dbg) extract_name_lut: {}, {}'.format(k, v))

    # return {lookup.get(key, key): val for key, val in extracted.iteritems()}
    return {lookup.get(key, key): val for key, val in extracted.items()}


def extract_data(cfg, elem, attributes, prefix=None, gdb=None):
    '''
    Method to extract data from an element. Dict *attributes* is
    updated in place, so no return value. Config *cfg* contains keys
    that will be skipped.

    :param cfg: Config dictionary
    :param elem: VegObjekt-element, dictionary containing all data for
        VegObjekt as returned by NVDB-API
    :param attributes: Dictionary containing all data that will potentially
        be inserted in feature class
    :param prefix: String that will be prepended to NVDB attribute names
    :param gdb: Workspace used to validate field names
    '''
    attrs = {}
    _extract_data(cfg, elem, attrs, prefix=prefix, gdb=gdb)
    attributes.update(post_proc_extracted(cfg, attrs))
#    nop = None # NOP

def _extract_data(cfg, elem, attributes, prefix=None, gdb=None):
    if prefix is None:
        prefix = ''

    if isinstance(elem, dict):
        # for key, val in elem.iteritems():
        for key, val in elem.items():
            _extract_data_kv(cfg, key, val, attributes, prefix, gdb)
    elif isinstance(elem, list):
        # Because we don't know the length of any list we cannot treat them as proper fields.
        # Treat list as json str, alternativt opprett hjelpetabeller og relasjoner
        attributes[prefix] = json.dumps(elem, separators=(',', ':'))
    else:
        attributes[prefix] = elem


def _extract_data_kv(cfg, key, val, attributes, prefix, gdb):
    rkeys = cfg['response_keys']

    if key in cfg['vegobjekt_exclude']:
        # cfg har oversikt over data vi ikke vil ha med
        return
    elif key == rkeys['vegObj.egskap']:
        # Denne er lett, har schema i egrp
        # iterer over alle egenskaper
        for egenskap in val:
            navn = egenskap[rkeys['vegObj.egskap.navn']]
            # Behandle ENUM-verdier
            if egenskap[rkeys['vegObj.egskap.datatype']] in cfg['typer']['enum']:
                verdi = egenskap[rkeys['vegObj.egskap.enum.verdi']]
            else:
                verdi = egenskap[rkeys['vegObj.egskap.verdi']]

            attributes[arcyfy_name(navn)] = verdi
            # attributes[arcpy.ValidateFieldName(navn, gdb)] = verdi
    elif key == rkeys['vegObj.lok.utm33']:
        # Behandle geometrien for seg selv
        attributes[cfg['extras_default']['geometri']] = val
    elif isinstance(val, dict) and set(val.keys()) == {'navn', 'nummer'}:
        #TODO: Se om enums funker
        attr_name = '%s_%s' % (prefix, key)
        attributes[attr_name] = val['nummer']
    else:
        # iterer lenger ned i strukturen
        _extract_data(cfg, val, attributes, prefix='_'.join(x for x in [prefix, key] if x), gdb=gdb)


def arcyfy_name(field, use_arc=False, gdb=None):
    '''
    ArcGIS replaces some chars when fields are created.
    Deprecated function, use arcpy.ValidateFieldName instead
    '''
    if use_arc:
        return arcpy.ValidateFieldName(field, gdb)
#     replace = [(',', '_'),
#                ('.', '_'),
#                (' ', '_'),
#                ('-', '_'),
#                ('/', '_'),
#                ('(', '_'),
#                (')', '_'),
#                #('__', '_')
#                ]
#     for i, o in replace:
#         field = field.replace(i, o)
    # Replace everything except valid characters
    s = re.sub(u'[^0-9a-zA-Z_æøåÆØÅ]', '_', u'{}'.format(field))
    # Preceed leading integer with char F
    ret_field = re.sub('^[0-9]', 'F%s' % s[0], s)

    return ret_field


def update_cfg(cfg, cwd):
    '''
    Updates config dictionary after loading

    :param cfg:
    :param cwd:
    '''

    sr_gdb = join(cwd, r'nvdb_access\resources\Spatial_refs.gdb')
    cfg['spatial_refs']['utm33'] = join(sr_gdb, cfg['spatial_refs']['utm33'])


def sok_parse(objektTyper, lokasjon=None):
    '''
    Method to parse query to NVDB API

    :param objektTyper: List of element types that will be included
        in query, see NVDB API documentation for details
    :param lokasjon: Locational limitation of query, see NVDB API
        documentation for details
    '''

    kriterie = {'objektTyper': objektTyper
               }
    if lokasjon:
        kriterie['lokasjon'] = lokasjon
    # HUSK: json liker ikkje numpy datatyper!
    sok = json.dumps(kriterie)
    # return 'kriterie={0}'.format(urllib2.quote(sok))
    return 'kriterie={0}'.format(quote(sok))

def my_urljoin(basepath, uri):
    '''
    Docstring for my_urljoin

    :param basepath:
    :param uri:
    '''
    if not basepath.endswith('/'):
        basepath = '{0}/'.format(basepath)
    uri_tmp = uri.lstrip('/')
    return urljoin(basepath, uri_tmp)

def request(baseurl, uri, headers, params=None, query=None, mode='GET'):
    '''
    The way we access the REST API
    '''
    if not baseurl.endswith('/'):
        baseurl = '{0}/'.format(baseurl)
    # parse baseurl
    p = urlparse(baseurl)
    # strip uri of leading '/'
    uri_tmp = uri.lstrip('/')
    # join path of baseurl with uri
    urlpath = urljoin(p.path, uri_tmp)
    # Rejoin the pieces with query
    parse_tuple = tuple((p.scheme, p.netloc, urlpath,
                         p.params, query, p.fragment))
    url = urlunparse(parse_tuple)
    # logger.info('(dbg) request: url: {}'.format(url))
    logger.debug('(dbg) request: headers: {}'.format(headers))
    logger.debug('(dbg) request: url: {}'.format(url))
    # return _request(url, headers, params=params, mode=mode)
    return _request_new(url, headers, params=params, mode=mode)

def _request(url, headers, params=None, mode='GET'):
    r = Request(url, headers=headers)
    if params and mode.lower() == 'post':
        r.add_data(urlencode(params))

    responseObj = urlopen(r)
    # TODO: Legg inn sjekk for om det gjekk bra
    # unpack here for debug purpose
    response = next(responseObj)
    return json.loads(response)

def _request_new(url, headers, params=None, mode='GET'):
    global SESSION
    if not SESSION:
        SESSION = Session()
        SESSION.mount(url, HTTPAdapter(max_retries=Retry(total=5, status_forcelist=[500, 503])))
    if params and mode.lower() == 'post':
        # response = requests.post(url, params)
        response = SESSION.post(url, params)
    else:
        try:
            # response = requests.get(url)
            response = SESSION.get(url, headers=headers, verify=True)
            if response.status_code != 200:
                errors = response.json()
                logger.info('NVDB-API KALL FEILER:')
                for error in errors:
                    if 'code' in error:
                        logger.info('FEILKODE: {}'.format(error['code']))
                    if 'message' in error:
                        logger.info('FEILMELDING: {}'.format(error['message']))
                logger.info('URL: {}'.format(url))
                    # logger.info('FEIL json: {}'.format(error))
                return ''
        except Exception as e:
            raise

    return response.json()

def check_token(cfg, t_buffer=1800):
    ttoken = dtm.datetime.fromtimestamp(cfg['token_expires'] / 1e3)
    tn = dtm.datetime.now()
    delta = dtm.timedelta(0, t_buffer)
    if ttoken > tn + delta:
        return True
    return False

def stop_start_services(cfg, stopOrStart, do_something=False):

    # Ask for server config
    if 'server_url' in cfg:
        baseurl = cfg['server_url']
    else:
        serv_scheme = cfg['server_scheme']
        serv_name = cfg['server_name']
        serv_port = cfg['server_port']
        # Parse baseurl
        baseurl = '{0}://{1}:{2}'.format(serv_scheme, serv_name, serv_port)

    headers = cfg['service_headers']
    params = cfg['params']
    uri = cfg['service_folder']

    # Check to make sure stop/start parameter is a valid value
    if str.upper(stopOrStart) != "START" and str.upper(stopOrStart) != "STOP":
        msg = "Invalid STOP/START parameter entered"
        logger.error(msg)
#         arcpy.AddMessage(msg)
        return

#     folderURL = cfg['service_folder'] #"/arcgis/admin/services/" + folder

    # Connect to URL and post parameters
    try:
        data = request(baseurl, uri, headers, params, mode='POST')
    # except urllib2.HTTPError as e:
    except HTTPError as e:
        try:
            msg = '%s: %s' % (e.filename, e.reason)
        except:
            msg = '%s: %s' % (e.msg, e.reason)
        logger.error(e.reason)
#         arcpy.AddMessage(msg)

    # Check that data returned is not an error object
    if not assertJsonSuccess(cfg, data):
        msg = "Error when reading folder information. " + str(data)
        logger.error(msg)
#         arcpy.AddMessage(msg)
        return
    else:
        msg = "Processed folder information successfully. Now processing services..."
        logger.info(msg)
#         arcpy.AddMessage(msg)

        # Loop through each service in the folder and stop or start it
        # depending on if it's mentioned in the config
        services = deepcopy(cfg['services'])
        for item in data['services']:
            if item['serviceName'] in services:
                if not do_something:
                    continue
                fullSvcName = item['serviceName'] + "." + item['type']

                # Construct URL to stop or start service, then make the request
                stopOrStartURL = my_urljoin(uri, my_urljoin(fullSvcName, stopOrStart))
                try:
                    stopStartData = request(baseurl, stopOrStartURL, headers, params, mode="POST")
                # except urllib2.HTTPError as e:
                except HTTPError as e:
                    try:
                        msg = '%s: %s' % (e.filename, e.reason)
                    except:
                        msg = '%s: %s' % (e.msg, e.reason)
                    logger.error(e.reason)
#                     arcpy.AddMessage(msg)

                # Check that data returned is not an error object
                if not assertJsonSuccess(cfg, stopStartData):
                    if str.upper(stopOrStart) == "START":
                        msg = "Error returned when starting service " + fullSvcName + "."
                        logger.error(msg)
#                         arcpy.AddMessage(msg)
                    else:
                        msg = "Error returned when stopping service " + fullSvcName + "."
                        logger.error(msg)
#                         arcpy.AddMessage(msg)

                    logger.error(str(stopStartData))
#                     arcpy.AddMessage(str(stopStartData))

                else:
                    msg = "Service " + fullSvcName + " processed successfully."
                    logger.info(msg)
#                     arcpy.AddMessage(msg)
                    services.pop(services.index(item['serviceName']))

        return

# A function that checks that the input JSON object
#  is not an error object.
def assertJsonSuccess(cfg, obj):
    #logger = logging.getLogger(cfg['loggers']['ajs'])
    #logger.setLevel(logging.DEBUG)
#     obj = json.loads(data)
    if 'status' in obj and obj['status'] == "error":
        msg = "JSON object returns an error. %s" % str(obj)
        logger.error(msg)
        return False
    else:
        return True


class BaseDataset(object):

    def __init__(self, name, shape=None, dtype=pl.float64, data=None):
        '''
        h5py-like dataset

        :param name:
        :param shape:
        :param dtype:
        :param data:
        '''
        self.attrs = {}
        self.name = name
        if not any([shape, data]):
            raise ValueError
        self._set_data(shape, dtype, data)

    def __getitem__(self, name):
        return self._dset[name]

    def __setitem__(self, ind, val):
        self._dset[ind] = val

    def _set_data(self, shape, dtype, data):
        if not data:
            dset = pl.zeros(shape, dtype=dtype)
        else:
            if isinstance(dtype, list):
                data = [tuple(row) for row in data]
            dset = pl.asarray(data, dtype=dtype)
        self._dset = dset
        self.shape = dset.shape
        self.dtype = dset.dtype



class BaseGroup(object):

    def __init__(self, *args, **kwargs):
        self.attrs = {}
        self._groups = {}
        self._datasets = {}
        try:
            self.name = kwargs['name']
        except:
            self.name = '/'

    def __getitem__(self, uri):
        pparts = uri.strip('/').split('/')
        grp_name = pparts.pop(0)
        db_name = '/'.join([self.name, grp_name])
        db_name = '/%s' % db_name.lstrip('/')
        nparts = len(pparts)
        if not pparts:
            try:
                return self._groups[grp_name]
            except:
                pass
            try:
                return self._datasets[grp_name]
            except KeyError:
                msg = 'Name %s does not exist' % db_name
                raise KeyError(msg)
        else:
            new_uri = '/'.join([pparts.pop(0) for _ in range(nparts)])
            try:
                return self._groups[grp_name][new_uri]
            except KeyError:
                msg = 'Group %s does not exist' % db_name
                raise KeyError(msg)


    def __iter__(self):
        '''
        A group can be iterated over to yield names of children
        '''
        for name in chain(self._groups, self._datasets):
            yield name


    def create_group(self, uri, *args, **kwargs):
        pparts = uri.strip('/').split('/')
        grp_name = pparts.pop(0)
        db_name = '/'.join([self.name, grp_name])
        db_name = '/%s' % db_name.lstrip('/')
        nparts = len(pparts)
        if len(pparts) >= 1:
            new_uri = '/'.join([pparts.pop(0) for _ in range(nparts)])
            if grp_name in self._datasets:
                raise ValueError
                return
            try:
                grp = self._groups[grp_name]
            except KeyError:
                grp = BaseGroup(name=db_name)
                self._groups[grp_name] = grp
            return grp.create_group(new_uri)
        elif not pparts:
            # We've come to the bottom, so create a group and return it
            if grp_name in self._groups:
#                 raise Exception('Group already exists')
                raise ValueError
            elif grp_name in self._datasets:
#                 raise Exception('%s is an existing dataset' % db_name)
                raise ValueError
            else:
                self._groups[grp_name] = BaseGroup(name=db_name)
                return self._groups[grp_name]


    def create_dataset(self, uri, shape=pl.float64, dtype=None, data=None):
        pparts = uri.strip('/').split('/')
        dset_name = pparts.pop(0)
        db_name = '/'.join([self.name, dset_name])
        db_name = '/%s' % db_name.lstrip('/')
        nparts = len(pparts)
        if len(pparts) >= 1:
            new_uri = '/'.join([pparts.pop(0) for _ in range(nparts)])
            if dset_name in self._datasets:
                raise ValueError
                return
            try:
                grp = self._groups[dset_name]
            except KeyError:
                grp = BaseGroup(name=db_name)
                self._groups[dset_name] = grp
            return grp.create_dataset(new_uri, shape, dtype, data)
        elif not pparts:
            # We've come to the bottom, so create a group and return it
            if dset_name in self._groups: 
#                 raise Exception('Group already exists')
                raise ValueError
                return
            elif dset_name in self._datasets:
#                 raise Exception('%s is an existing dataset' % db_name)
                raise ValueError
                return
            else:
                self._datasets[dset_name] = BaseDataset(db_name, shape,
                                                        dtype, data)
                return self._datasets[dset_name]

# try:
#     from h5py import File
# except:
File = BaseGroup


class BaseFile(File):
    '''
    Customized h5py.File
    '''

    def __init__(self, name, *args, **kwds):
        '''
        A h5py.File object customized to store information about retrieved
        from a REST API, originally for the norwegian road database (nvdb)

        :param baseurl: Url to root of database
        :param name:
        '''
        # Initialize the file object
        File.__init__(self, name, *args, **kwds)
        self._dbid2name_map = {'/': 'root'}
        self._name2dbid_map = {'root': '/'}
        self._name2uri_map = {'root': '/'}
        self._dbid2uri_map = {'/': '/'}
        self._uri2name_map = {'/': 'root'}
        self._uri2dbid_map = {'/': '/'}
        self.attrs['dbid'] = '/'
        self.attrs['name'] = 'root'


    def add(self, dbid, name, uri):
        if dbid in self._dbid2name_map:
            # If we re-add old id check if this is the same object as
            # existing one
            assert self._dbid2uri_map[dbid] == uri
            assert self._dbid2name_map[dbid] == name
            return
        if name in self._name2dbid_map:
            # If we re-add old name check if this is the same object as
            # existing one
            assert self._name2dbid_map[name] == dbid
            assert self._name2uri_map[name] == uri
            return
        if uri in self._uri2name_map:
            # If we re-add old uri check if this is the same object as
            # existing one
            assert self._uri2name_map[uri] == name
            assert self._uri2dbid_map[uri] == dbid
            return
        self._dbid2name_map[dbid] = name
        self._name2dbid_map[name] = dbid
        self._name2uri_map[name] = uri
        self._dbid2uri_map[dbid] = uri
        self._uri2name_map[uri] = name
        self._uri2dbid_map[uri] = dbid

    def by_name(self, name):
        if not name in self._name2dbid_map:
            raise KeyError("Key %s not found, available keys are %s" % (name, sorted(self._name2dbid_map.keys())))
        dbid = self._name2dbid_map[name]
        uri = self._name2uri_map[name]
        assert self._dbid2name_map[dbid] == name
        assert self._uri2name_map[uri] == name
        return uri, dbid

    def by_uri(self, uri):
        dbid = self._uri2dbid_map[uri]
        name = self._uri2name_map[uri]
        assert self._dbid2uri_map[dbid] == uri
        assert self._name2uri_map[name] == uri
        return dbid, name

    def by_dbid(self, dbid):
        uri = self._dbid2uri_map[dbid]
        name = self._dbid2name_map[dbid]
        assert self._uri2dbid_map[uri] == dbid
        assert self._name2dbid_map[name] == dbid
        return uri, name


class BaseTool(object):
    """
    BaseTool is a custom parent class from which other 'tools' can inherit
    properties and methods.

    The result is fewer lines of code when creating tools that inherit from
    BaseTool. For example, it is not always necessary to override the
    'isLicensed', 'updateParameters', and 'updateMessages' methods.

    """
    __parameters = {}

    def __init__(self):
        """
        Initialization.

        """
        self.label = 'Basetool'
        self.description = ''
        self.canRunInBackground = False

    def __getitem__(self, key):
        """
        Returns a parameter object by its name.

        """
        if not self.__parameters:
            self.__parameters = OrderedDict([(p.name, p) for p in self.getParameterInfo()])
        if key not in self.__parameters:
            raise KeyError('Parameter name "{0}" does not exist.'.format(key))
        return self.__parameters[key]

    def __iter__(self):
        """
        A tool can be iterated over to yield parameters.

        """
        for item in self.__parameters:
            yield self.__parameters[item]

    @property
    def parameters(self):
        """
        The parameters property returns a list of parameters.

        """
        return self.__parameters.values()

    def getParameterInfo(self):
        """
        Define parameter definitions.

        """
        return []

    def isLicensed(self):
        """
        Set whether tool is licensed to execute.

        """
        return True

    def updateParameters(self, parameters):
        """
        Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed.

        """
        self.refresh(parameters)

    def updateMessages(self, parameters):
        """
        Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation.

        """
        pass

    def execute(self, parameters, messages):
        """
        The source code of the tool.

        """
        pass

    @property
    def parameterNames(self):
        """
        Docstring for parameterNames.

        """
        return {p.name: i for i, p in enumerate(self.getParameterInfo())}

    def refresh(self, parameters):
        """
        Refreshes internal parameters.

        """
        for param in parameters:
            self.__parameters[param.name] = param
