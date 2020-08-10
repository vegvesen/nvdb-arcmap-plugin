# -*- coding: utf-8 -*-
import sys
import pylab as pl
from datetime import datetime
from copy import deepcopy
import traceback
import posixpath as path
import logging

if sys.version_info[0] < 3:
    from urllib2 import HTTPError
    from urllib import urlencode
else:
    from urllib.error import HTTPError
    from urllib.parse import urlencode

from ..shared import request, BaseFile, my_urljoin, extract_data

from .geometry import check_empty_geometry

vlen_dt = pl.object_

logger = logging.getLogger(__name__)



def lokasjon_filter(loc_type, loc):
    '''
    Set up lokasjon, might move this to shared?
    '''
    if loc_type in ['fylke', 'region']:
        if isinstance(loc, list):
            return {loc_type: loc}
        elif isinstance(loc, (int, float)):
            return {loc_type: [int(loc)]}
        else:
            msg = 'Lokasjonstype %s for type %s ikke implementert' % (loc_type,
                                                                      str(type(loc)))
            raise NotImplementedError(msg)
    elif loc_type == 'bbox':
        extent_ls = [loc.XMin, loc.YMin, loc.XMax, loc.YMax]
        extent = ','.join([str(c) for c in extent_ls])
        loc_dict = {'srid': "UTM33",
                    'kartutsnitt': extent}
                    # 'bbox': extent}

        return loc_dict


def get_omrade_by_name(cfg, metafile, omrade_type, name):
    '''
    '''
    omr_dbname = cfg['names'][omrade_type]
    omr_loc, _ = metafile.by_name(omr_dbname)
    fn = metafile[omr_loc]['navn']
    fi = metafile[omr_loc]['id']
    omr_lut = {n: i for n, i in zip(fn, fi)}
    return int(omr_lut[name])


def _build_schema(cfg, grp, uri):
    # TODO: Sett inn nvdb_name og name (arcname)
    schema = {'uri': [],
              'navn': [],
              'id': [],
              'dt': [],
              'nvdb_dt': []}

    r = request(cfg['baseurl'], uri, cfg['headers'])

    rkeys = cfg['response_keys']

    # translate geometrytype
    key = rkeys['egenskapsTyper.geometriType']
    geometry = r.get(key, None)

    try:
        grp.attrs['geometry_type'] = cfg['geomTyperLut'][geometry]
    except:
        logger.warn("Fant ikke geometri type {}".format(geometry))

    # replace = []
    for elem in r[rkeys['egenskapsTyper']]:
        name = elem[rkeys['egenskapsTyper.name']]
        dbid = elem[rkeys['egenskapsTyper.dbid']]
        rel = path.join(uri, format(dbid))
        dt = None
        if elem.get(rkeys['egenskapsTyper.type']):
        # if rkeys['egenskapsTyper.type'] in elem:
            nvdb_dt = elem[rkeys['egenskapsTyper.type']]
            if nvdb_dt in cfg['typerLut']:
                dt = cfg['typerLut'][nvdb_dt]
            else:
                logger.debug("NVDB datatypen {} er ikke mappa.".format(nvdb_dt))
                continue

            schema['uri'].append(rel)
            schema['navn'].append(name)
            schema['id'].append(dbid)
            schema['dt'].append(dt)
            schema['nvdb_dt'].append(nvdb_dt)
        else:
            nvdb_dt = None
            dt = None

    # Once the schema is built, we can update the group
    _add_schema_grp(grp, schema, 'egenskaper')


def _add_schema_grp(grp, schema, schema_name):
    '''
    Add schema group under existing group
    '''
    grp_e = grp.create_group(schema_name)

    for key in ['navn', 'dt', 'nvdb_dt', 'uri']:
        dset = grp_e.create_dataset(key, shape=(len(schema[key]),),
                                    dtype=vlen_dt)
        if len(schema[key]):
            dset[:] = schema[key]
    dset = grp_e.create_dataset('id', shape=(len(schema['id']),),
                                dtype=pl.int32)
    if len(schema['id']):
        dset[:] = schema['id']
#     if replace:
#         dset = grp_e.create_dataset('replace', shape=(len(replace), 2),
#                                     dtype=vlen_dt)
#         dset[:] = replace


def _post_process_schema(cfg, h5file, schema_grp=None):
    '''
    Method to build schema for extra attributes not in egenskaper
    '''
    # Loop over alle schemagrupper eller kun den eine hvis schema_grp er
    # satt
    if schema_grp:
        _extend_schema(cfg, h5file, schema_grp)
    else:
        # Finn gruppa som held alle schema grupper
        vo_typer_uri, _ = h5file.by_name(cfg['names']['objekttyper'])
        parent_grp = h5file[vo_typer_uri]
        # loop over alle schema grupper
        for schema_uri in parent_grp:
            # Parent group has 'id' and 'navn' datasets listing all
            # available objekttyper in nvdb, exclude those
            if not schema_uri in ['id', 'navn']:
                schema_grp = parent_grp[schema_uri]
                _extend_schema(cfg, h5file, schema_grp)


def _extend_schema(def_cfg, h5file, schema_grp):
    '''
    Method to extend schema with extra attributes not in egenskaper
    '''
    # Need a temporary copy of cfg since we don't want to get
    # egenskaper here
    cfg = deepcopy(def_cfg)
    e_key = cfg['response_keys']['vegObj.egskap']
    cfg['vegobjekt_exclude'].append(e_key)
    # Hent eit vegobjekt gjennom /vegobjekter
    base_uri, _ = h5file.by_name(cfg['names']['vegobjekter'])
    # Finn id for schema_grp
    dbid = schema_grp.attrs['dbid']
    # Maa hardkode uri siden denne ikkje kjem fra meta
    obj_uri = my_urljoin(base_uri, str(dbid))
    params = dict(antall=1)
    params.update(cfg['sok_params'])
    r = request(cfg['baseurl'], obj_uri, cfg['headers'], query=urlencode(params))

    rkeys = cfg['response_keys']

    if(len(r[rkeys['vegObjektType.objekter']]) == 0):
        return
    # Hent ut første objekt
    elem = r[rkeys['vegObjektType.objekter']][0]

    # Hent alle aktuelle attributter
    ext_extras = {}
    extract_data(cfg, elem, ext_extras, prefix='nvdb')

    # trekk ut standard attributter
    def_extras = _filter_attributes(cfg, ext_extras)

    # Hvis objektet ikkje hadde geometri men burde hatt det legg med dette til
    check_empty_geometry(cfg, def_extras, schema_grp.attrs['geometry_type'])

    # opprett nye datasett på schemagruppa
    # TODO: opprett navn og nvdb_navn
    schema = {'uri': [],
              'navn': [],
              'id': [],
              'dt': [],
              'nvdb_dt': []}
    # først for default extras
    for key, val in def_extras.items():
        dt, nvdb_dt = _get_dtype(val)
        if dt and nvdb_dt:
            schema['uri'].append('')
            schema['navn'].append(key)
            schema['id'].append(-1)
            schema['dt'].append(dt)
            schema['nvdb_dt'].append(nvdb_dt)
    # Add schema group
    _add_schema_grp(schema_grp, schema, 'default_extras')
    # Så for utvida extras
    ext_schema = {'uri': [],
                  'navn': [],
                  'id': [],
                  'dt': [],
                  'nvdb_dt': []}
    # først for default extras
    for key, val in ext_extras.items():
        dt, nvdb_dt = _get_dtype(val)
        if dt and nvdb_dt:
            ext_schema['uri'].append('')
            ext_schema['navn'].append(key)
            ext_schema['id'].append(-1)
            ext_schema['dt'].append(dt)
            ext_schema['nvdb_dt'].append(nvdb_dt)
    # Add schema group
    _add_schema_grp(schema_grp, ext_schema, 'extended_extras')


def _get_dtype(val):
    '''
    Method to extract dtypes from value
    '''
    # Check if numeric
    try:
        float(val)
    except:
        pass
    else:
        if int(val) == float(val):
            return 'LONG', 'Tall'
        else:
            return 'DOUBLE', 'Tall'
    # If past this point, check if date
    try:
        str(val)
    except:
        pass
    else:
        try:
            datetime.strptime(val, '%Y-%m-%dT%H:%M:%S+02:00')
            return 'DATE', 'Dato'
        except:
            pass
        try:
            datetime.strptime(val, '%Y-%m-%d')
            return 'DATE', 'Dato'
        except:
            pass
# Vi lagrer klokkeslett som string, siden DATE inneholder år, mnd og dag men vi vet har ikke disse
#        try:
#            datetime.strptime(val, '%H:%M:%S')
#            return 'DATE', 'Klokkeslett'
#        except:
#            pass
#        try:
#            datetime.strptime(val, '%H:%M')
#            return 'DATE', 'Klokkeslett'
#        except:
#            pass
        # If past this point assume it's a string
        return 'TEXT', 'Tekst'
    # If past this point we're out of ideas
    return None, None
    # Done


def _filter_attributes(cfg, ext_attributes):
    '''
    Method to sort out which extra attributes to include as default
    '''
    def_attributes = {}
    for key in cfg['extras_default'].values():
        if key in ext_attributes:
            def_attributes[key] = ext_attributes.pop(key)
    return def_attributes


def _post_process(cfg, meta):
    _post_process_schema(cfg, meta)
    ## TODO: This is a hack, will be removed later
    ## NOTE(Erlend): Endepunktet /endringer fjernes, og endringer gjøres
    ##  tilgjengelig som en undertjeneste til hver vegobjekttype.
    ##     ver_uri = '/datakatalog/version'
    ##     meta.add(ver_uri, 'version', ver_uri)
    ##     _build_recur(cfg, meta, uri=ver_uri)
    # mod_uri = '/endringer'
    # meta.add(mod_uri, 'endringer', mod_uri)
    # _build_recur(cfg, meta, uri=mod_uri)


def build_meta(cfg, ofile=None, uri='/', verbose=False):
    if not ofile:
        h5file = BaseFile('tmp.h5', 'w', driver='core')
    else:
        h5file = BaseFile(ofile, 'w')

    msg = 'Henter feltnavn og datatyper'
    logger.info(msg)

    bldr = Builder(cfg, h5file)
    bldr._build_recur()

    # Post process skal søke opp eit vegobjekt og trekke ut ekstra
    # attributter
    msg = 'Etterprosesserer metadata'
    logger.info(msg)

    try:
        _post_process(cfg, h5file)
    except KeyError as e:
        # Dette betyr at vi mangler metadata, typisk at
        # internett-forbindelesen er brutt
        logger.warn("Mangler metadata")
        logger.debug(traceback.format_exc(10))
        return None

    # Legg inn sjekk her om alt som trengs er bygd
    # TODO: Dette utelukker ikkje feil!!!
    db_names = h5file._name2uri_map
    chklst = [False for n in cfg['names'].values() if not n in db_names]
    ##TODO: implement
    # if not all(chklst):
    #    logger.warn("Metadata bestod ikke testing")
    #    return None

    msg = 'Metadata ferdigbygd'
    logger.info(msg)

    return h5file


def uri_get_relative(uri, root):
    """"Returnerer en path relativt til root"""
    l = uri.split(root)
    return l[1]


class Builder(object):
    '''class Builder'''
    def __init__(self, cfg, h5file):
        self.cfg = cfg
        self.h5file = h5file
        self.rkeys = cfg['response_keys']

    def _build_recur(self, uri='/'):
        dbid = self.h5file._uri2dbid_map[uri]
        name = self.h5file._uri2name_map[uri]

        try:
            r = request(self.cfg['baseurl'], uri, self.cfg['headers'])

            grp = self.h5file.create_group(uri)
            grp.attrs['dbid'] = dbid
            grp.attrs['name'] = name
        except ValueError:
            # dette skal intreffe nar / blir lagt til
            pass
        except HTTPError as e:
            logger.warn("Request to {} failed: {}".format(uri, e))
            logger.debug("args: {}".format([self.cfg['baseurl'], uri, self.cfg['headers']].__repr__()))
            logger.debug(traceback.format_exc(10))
            return

        if name == self.cfg['names']['version']:
            datakatalog = r[self.rkeys['ver.datakatalog']]
            date = datakatalog[self.rkeys['ver.date']]
            vers = datakatalog[self.rkeys['ver.ver']]
            self.h5file.attrs['version'] = vers
            self.h5file.attrs['date'] = date
            grp.attrs['version'] = vers
            grp.attrs['date'] = date

        elif name in self.cfg['typer']['omrade']:
            self._build_omrade(grp, name, r)

        elif isinstance(r, list) and len(r) > 0:
            if name == self.cfg['names']['objekttyper']:
                obj_id = []
                obj_name = []
                for elem in r:
                    dbid = elem[self.rkeys['vegObjektTyper.dbid']]
                    name = elem[self.rkeys['vegObjektTyper.name']]
                    rel = path.join(uri, format(dbid))  ## TODO: Dette er en Hack.

                    self.h5file.add(dbid, name, rel)  # Add element to top lookup
                    obj_id.append(dbid)  # Add element to local lookup

                    obj_name.append(name)
                    checklist = [name in self.cfg['include_by_name']['vegObjektTyper'],
                                 dbid in self.cfg['include_by_dbid']['vegObjektTyper'],
                                 self.cfg['include_by_name']['vegObjektTyper'] == 'All']

                    if any(checklist):
                        self._build_recur(rel)

                id_dset = grp.create_dataset('id',
                                             (len(obj_id),),  ## NOTE(Erlend): Var len(obj_name)
                                             dtype=pl.int32)
                id_dset[:] = obj_id
                nm_dset = grp.create_dataset('navn',
                                             (len(obj_name),),  ## NOTE(Erlend): Var len(obj_id)
                                             dtype=vlen_dt)
                nm_dset[:] = obj_name
            # elif r[0].has_key(self.rkeys['ressurser.dbid']):
            elif self.rkeys['ressurser.dbid'] in r[0]:
                for elem in r:
                    dbid = elem[self.rkeys['ressurser.dbid']]
                    uri = elem[self.rkeys['ressurser.uri']]

                    # Bruk relative paths
                    dbid = path.basename(dbid)
                    uri = uri_get_relative(uri, self.cfg['baseurl'])

                    name = elem[self.rkeys['ressurser.name']].lower()
                    self.h5file.add(dbid, name, uri)

                    # Ikke recur hvis det ligner på ett objekt(tall) eller er ekskludert eksplisitt
                    if not dbid.isdigit() and not name in self.cfg['exclude_by_name']:
                        self._build_recur(uri)

    def _build_omrade(self, grp, omr_type, val_list):
        rkeys = self.cfg['response_keys']
        navn = []
        omrid = []
        is_ruter = omr_type == rkeys['omrade.ruter']
        if is_ruter:
            skildring = []
        else:
            skildring = None
        # print('Bygger omrade %s' % omr_type)
        for elem in val_list:
            elem_navn = format(elem[rkeys['omrade.navn']])
            if is_ruter:
                elem_navn = ' '.join([elem_navn, elem[rkeys['omrade.ruter.periode']]])

            navn.append(elem_navn)
            elem_id = elem[rkeys['omrade.nummer']]
            omrid.append(elem_id)
            # 'ruter' has 'beskrivelse'
            if isinstance(skildring, list):
                elem_skildring = format(elem[rkeys['omrade.beskriv']])
                skildring.append(elem_skildring)
        assert len(omrid) == len(navn)

        nm_dset = grp.create_dataset('navn', shape=(len(navn),),
                                     dtype=vlen_dt)
        if isinstance(omrid[0], int):
            id_dt = pl.int32
        else:
            id_dt = vlen_dt

        id_dset = grp.create_dataset('id', shape=(len(omrid),),
                                     dtype=id_dt)
        if skildring:
            assert len(skildring) == len(navn)
            skildring_dset = grp.create_dataset('beskrivelse', shape=(len(navn),),
                                                dtype=vlen_dt)

        try:
            nm_dset[:] = navn
            id_dset[:] = omrid
            if skildring:
                skildring_dset[:] = skildring
        except:
            return
