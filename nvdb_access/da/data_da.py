# -*- coding: utf-8 -*-
# from __future__ import (print_function, unicode_literals, absolute_import, division)
from __future__ import (print_function, unicode_literals, division)
import sys
from os.path import basename, dirname, join
import pylab as pl
from datetime import datetime
from datetime import timedelta
if sys.version_info[0] < 3:
    from urllib2 import HTTPError
    from urllib import urlencode
else:
    from urllib.error import HTTPError
    from urllib.parse import urlencode

import logging

import arcpy
from arcpy.da import (InsertCursor, ListDomains, UpdateCursor) # @UnresolvedImport

if not __name__ == '__main__':
    from ..shared import request, sok_parse, arcyfy_name, extract_data
    from .meta_da import _build_schema, _post_process_schema
    from .geometry import check_empty_geometry, repair_inconsistent_geometry, WKT

logger = logging.getLogger(__name__)

# GEOMETRIES = ['POINT', 'POLYLINE', 'POLYGON'] # , MULTIPATCH
GEOMETRY_POSTFIX = {'POINT': 'punktgeometri', 'POLYLINE': 'linjegeometri', 'POLYGON': 'flategeometri'}
GEOMETRIES = {'POINT': ['POINT', 'MULTIPOINT'],
              'POLYLINE': ['POLYLINE', 'LINESTRING'],
              'POLYGON': ['POLYGON', 'MULTIPOLYGON']
             }

def _create_fc(cfg, fc, schema_grp, version, overwrite=False, spatial_ref='utm33',
               extended_extras=False, verbose=False):
    opath = dirname(fc)
    oname = basename(fc)
    if overwrite and arcpy.Exists(fc):
        try:
            arcpy.Delete_management(fc)
        except arcpy.ExecuteError as e:
            if 'ERROR 000732' in e.message:
                pass

    if not arcpy.Exists(fc):
        tmp_path = 'in_memory'
        tmp_fc = join(tmp_path, oname)
        if schema_grp.attrs['geometry_type'] == 'none':
            if verbose:
                logger.info('Oppretter tabell uten geometri i %s' % (fc))

            arcpy.CreateTable_management(tmp_path, oname)
            _add_fields(tmp_fc, schema_grp, version, extended_extras,
                        verbose=verbose)

            arcpy.CreateTable_management(opath, oname, template=tmp_fc)
            _add_domain_to_fields(fc, schema_grp, version, cfg, extended_extras,
                                  verbose=verbose)

        else:
            spatial_reference = arcpy.Describe(cfg['spatial_refs'][spatial_ref]).spatialReference
            # TODO: Bruk arcpy func her?

            geometry = schema_grp.attrs['geometry_type']

            if verbose:
                msg = 'Oppretter featureklasse med %sgeometri i %s' % \
                      (cfg['invGeomTyperLut'][geometry].lower(), fc)
                logger.info(msg)
            arcpy.CreateFeatureclass_management(tmp_path, oname, geometry)
            _add_fields(tmp_fc, schema_grp, version, extended_extras,
                        verbose=verbose)

            arcpy.CreateFeatureclass_management(opath, oname, geometry,
                                                template=tmp_fc,
                                                spatial_reference=spatial_reference)
            _add_domain_to_fields(fc, schema_grp, version, cfg, extended_extras,
                                  verbose=verbose)

            # Hack: Ikke nødvendig for lag_metrert_vegnett_pyt.py
            if basename(fc) not in ['Vegreferanse_temp']:
                if geometry.upper() not in GEOMETRIES['POINT']:
                    _geometry = "POINT"
                    oname = basename(fc) + '_' + GEOMETRY_POSTFIX[_geometry]
                    new_fc = join(dirname(fc), oname)
                    if overwrite and arcpy.Exists(new_fc):
                        try:
                            arcpy.Delete_management(new_fc)
                        except arcpy.ExecuteError as e:
                            if 'ERROR 000732' in e.message:
                                pass
                    if verbose:
                        msg = u'Oppretter ekstra featureklasse med {}: {}'.format(GEOMETRY_POSTFIX[_geometry], new_fc)
                        logger.info(msg)
                    # has_z = 'ENABLED'
                    arcpy.CreateFeatureclass_management(tmp_path, oname, _geometry)
                    _add_fields(tmp_fc, schema_grp, version, extended_extras,
                                verbose=verbose)

                    arcpy.CreateFeatureclass_management(opath, oname, _geometry,
                                                        template=tmp_fc,
                                                        spatial_reference=spatial_reference)
                    _add_domain_to_fields(new_fc, schema_grp, version, cfg, extended_extras,
                                          verbose=verbose)

                if geometry.upper() not in GEOMETRIES['POLYLINE']:
                    _geometry = "POLYLINE"
                    oname = basename(fc) + '_' + GEOMETRY_POSTFIX[_geometry]
                    new_fc = join(dirname(fc), oname)
                    if overwrite and arcpy.Exists(new_fc):
                        try:
                            arcpy.Delete_management(new_fc)
                        except arcpy.ExecuteError as e:
                            if 'ERROR 000732' in e.message:
                                pass
                    if verbose:
                        msg = u'Oppretter ekstra featureklasse med {}: {}'.format(GEOMETRY_POSTFIX[_geometry], new_fc)
                        logger.info(msg)
                    arcpy.CreateFeatureclass_management(tmp_path, oname, _geometry)
                    _add_fields(tmp_fc, schema_grp, version, extended_extras,
                                verbose=verbose)

                    arcpy.CreateFeatureclass_management(opath, oname, _geometry,
                                                        template=tmp_fc,
                                                        spatial_reference=spatial_reference)
                    _add_domain_to_fields(new_fc, schema_grp, version, cfg, extended_extras,
                                          verbose=verbose)

                if geometry.upper() not in GEOMETRIES['POLYGON']:
                    _geometry = "POLYGON"
                    oname = basename(fc) + '_' + GEOMETRY_POSTFIX[_geometry]
                    new_fc = join(dirname(fc), oname)
                    if overwrite and arcpy.Exists(new_fc):
                        try:
                            arcpy.Delete_management(new_fc)
                        except arcpy.ExecuteError as e:
                            if 'ERROR 000732' in e.message:
                                pass
                    if verbose:
                        msg = u'Oppretter ekstra featureklasse med {}: {}'.format(GEOMETRY_POSTFIX[_geometry], new_fc)
                        logger.info(msg)
                    arcpy.CreateFeatureclass_management(tmp_path, oname, _geometry)
                    _add_fields(tmp_fc, schema_grp, version, extended_extras,
                                verbose=verbose)

                    arcpy.CreateFeatureclass_management(opath, oname, _geometry,
                                                        template=tmp_fc,
                                                        spatial_reference=spatial_reference)
                    _add_domain_to_fields(new_fc, schema_grp, version, cfg, extended_extras,
                                          verbose=verbose)

def _create_domains(cfg, fc, schema_grp, version, verbose=False):
    '''
    Create domains in gdb, checks if domain already exists
    '''
    rkeys = cfg['response_keys']
    # Use arcpy
    gdb = dirname(fc)
    # list domains in gdb
    domains = [d.name for d in ListDomains(gdb)]
    # Loop over egenskaper in schema_grp and create domain if not already in
    # domains
    egrp = schema_grp['egenskaper']
    nms = egrp['navn']
    nv_dt = egrp['nvdb_dt']
    arc_dt = egrp['dt']
    u = egrp['uri']
    dbids = egrp['id']
    f = lambda n, i, v: arcyfy_name('%s_%d_%s' % (n, i, v))
#     f = lambda n, i, v: arcpy.ValidateFieldName('%s_%d_%s' % (n, i, v), gdb)
    d_names = [f(n, i, version) for n, i, d in zip(nms, dbids, nv_dt) if d in cfg['typer']['enum']]

    n_nonexist = len(set(d_names).difference(domains))
    if verbose and n_nonexist:
        logger.info('Lager %d nye domener for ENUM-verdier' % n_nonexist)
    for name, dbid, nvdb_dt, dt, uri in zip(nms, dbids, nv_dt, arc_dt, u):
#         name = arcyfy_name(name)
        if nvdb_dt in cfg['typer']['enum']:
            # Get domain info from nvdb
            r = request(cfg['baseurl'], uri, cfg['headers'])
            try:
                desc = r[rkeys['egskap.beskriv']]
            except:
                desc = 'Ingen beskrivelse'
            # Create new domain
            dbid = r[rkeys['egskap.id']]
            # TODO: Kva skjer hvis domenenavnet eksisterer, men ikkje verdiene?
            dname = '%s_%d_%s' % (name, int(dbid), version)
#             domain_name = arcpy.ValidateFieldName(dname, gdb)
            domain_name = arcyfy_name(dname)
            if not domain_name in domains:
                arcpy.CreateDomain_management(gdb, domain_name, desc, dt)
                for verdi in r[rkeys['egskap.eVerdi']]:
                    mapped_from = verdi[rkeys['egskap.eVerdi.id']]
                    mapped_to = verdi[rkeys['egskap.eVerdi.verdi']]
                    arcpy.AddCodedValueToDomain_management(gdb, domain_name, mapped_from, mapped_to)

def _cat_schema(schema_grp, version=None, extended_extras=False,
                verbose=False):
    '''
    Concatenate schema

    :param schema_grp:
    :param version:
    :param extended_extras:
    :param verbose:
    '''
    extras = []
    if 'default_extras' in schema_grp:
        extras = [schema_grp['default_extras']]
    if extended_extras:
        extras.append(schema_grp['extended_extras'])
    name_ls = list(schema_grp['egenskaper']['navn'][:])
    dt_ls = list(schema_grp['egenskaper']['dt'][:])
    nvdb_dt_ls = list(schema_grp['egenskaper']['nvdb_dt'][:])
    id_ls = list(schema_grp['egenskaper']['id'][:])

    # Treat extras
    en = []
    edt = []
    envdb_dt = []
    eid = []
    # Create lists of fields including extras
    for g in extras:
        en.extend(g['navn'][:])
        edt.extend(g['dt'][:])
        envdb_dt.extend(g['nvdb_dt'][:])
        eid.extend(g['id'][:])
    extras_schema = sorted(zip(en, edt, envdb_dt, eid))
    # logger.debug('extras_schema: {})'.format(extras_schema))

    if extras_schema:
        # unziping and extending colnames and dtypes
        ext_cn, ext_dt, ext_nvdb_dt, ext_id = zip(*extras_schema)
        name_ls.extend(ext_cn)
        dt_ls.extend(ext_dt)
        nvdb_dt_ls.extend(ext_nvdb_dt)
        id_ls.extend(ext_id)
    return id_ls, name_ls, dt_ls, nvdb_dt_ls


def _add_fields(fc, schema_grp, version, extended_extras=False,
                verbose=False):
    '''
    Adding fields in order
    '''
    gdb = dirname(fc)
    id_ls, name_ls, dt_ls, nvdb_dt_ls = _cat_schema(schema_grp, version,
                                                    extended_extras=extended_extras,
                                                    verbose=verbose)
    if verbose:
        nfields = len(name_ls)
        msg_ls = ['Legger til %d felt.' % nfields]
        if nfields > 10:
            msg_ls.append('Dette kan ta litt tid...')
        logger.info(' '.join(msg_ls))
    # Iterate over fields
    for f in zip(name_ls, dt_ls, nvdb_dt_ls, id_ls):
        ft = f[1]
        if f[0] == 'SHAPE@WKT':
            continue
        elif ft == 'ENUM':
            ft = 'LONG'

        fn = arcyfy_name(f[0])
#        fn = arcpy.ValidateFieldName(f[0], gdb)
        # TODO: Alias?
        # logger.debug('AddField_management(fc, field_name="{}", field_type="{}")'.format(fn, ft))
        arcpy.AddField_management(fc, field_name=fn, field_type=ft)

def _add_domain_to_fields(fc, schema_grp, version, cfg, extended_extras=False,
                          verbose=False):
    gdb = dirname(fc)
    id_ls, name_ls, dt_ls, nvdb_dt_ls = _cat_schema(schema_grp, version,
                                                    extended_extras=extended_extras,
                                                    verbose=verbose)
    for f in zip(name_ls, dt_ls, nvdb_dt_ls, id_ls):
        if f[0] == 'SHAPE@WKT':
            continue
#         fn = arcpy.ValidateFieldName(f[0], gdb)
        fn = arcyfy_name(f[0])
        if f[2] in cfg['typer']['enum']:
            dname = '%s_%d_%s' % (fn, int(f[3]), version)
            # domain_name = arcpy.ValidateFieldName(dname, gdb)
            domain_name = arcyfy_name(dname)
            arcpy.AssignDomainToField_management(fc, fn, domain_name)


def _populate_row(cfg, attributes, colnames, data_types,
                  verbose=False):
    '''
    Populates row with data from *attributes*.

    :param cfg: Dictionary
    :param attributes:
    :param colnames:
    :param data_types:
    :param verbose:
    '''
    row = []

    for c, dt in zip(colnames, data_types):
        try:
            thing = attributes.pop(c)
        except KeyError:
            row.append(None)
        else:
            if dt.upper() == 'DOUBLE':
                try:
                    row.append(float(thing))
                except:
                    row.append(None)
            elif dt.upper() == 'LONG':
                try:
                    row.append(int(thing))
                except:
                    row.append(None)
            elif dt.upper() in ['TEXT', 'STRING']:
                try:
                    if not c == 'SHAPE@WKT':
                        thing = thing[:255]
                    row.append(format(thing))
                except:
                    row.append(None)
            elif dt.upper() == 'DATE':
                dtime = None
                for fmt in cfg['try_fmts'].values():
                    try:
                        # TODO: Klokkeslett felt blir nå satt til 1900-tallet, finnes det en Time felttype i ArcGIS DB?
                        dtime = datetime.strptime(thing, fmt)
                        break
                    except ValueError:
                        pass

                row.append(dtime)
            else:
                # TODO: Send denne til logger
                print(format(thing))
#                         row.append(thing)
    return row

def _dump_elements(cfg, r, fc, schema_grp, extended_extras=False,
                   store_failed=False, debug=False, total_get=None):
    gdb = dirname(fc)
    grps = [schema_grp['egenskaper'], schema_grp['default_extras']]
    if extended_extras:
        grps.append(schema_grp['extended_extras'])
    # Create lists of fields
    cn = []
    data_types = []
    for g in grps:
        cn.extend(g['navn'][:])
        data_types.extend(g['dt'][:])

    # Validate names, except SHAPE@WKT
    colnames = [arcyfy_name(n) if not 'SHAPE@' in n else n for n in cn]
    geom_type = schema_grp.attrs['geometry_type']

    # Build lookup for attributes in r
    rkeys = cfg['response_keys']
    nelem_appended = 0
    message_step = 1000000

    row_cache = {}
    for key in GEOMETRY_POSTFIX: #.keys():
        if key != geom_type:
            row_cache[key] = []

    cursor = InsertCursor(fc, colnames)
    cursor2 = None
    while r[rkeys['vegObjektType.objekter']]:
        total = total_get + nelem_appended
        if total > 0 and (total + 1) % message_step == 0:
            logger.info('Henta totalt {} objekter.'.format(total + 1))

        elem = r[rkeys['vegObjektType.objekter']].pop(0)
        # Start med tom attributt-dict
        attributes = {}
        extract_data(cfg, elem, attributes, prefix='nvdb', gdb=gdb)

        # Check for empty geometry, Trengs kanskje ikke lenger!!
        has_geom = check_empty_geometry(cfg, attributes, geom_type)
        if has_geom:
            try:
                repair_inconsistent_geometry(attributes)
            except:
                logger.debug('Forsøkte å reparere geometri, men det feila', exc_info=debug)

        row = _populate_row(cfg, attributes, colnames, data_types, True)

        try:
            # New 19.06.2018: Check geometry type before insertRow
            expected_wkt_geom_type = cfg['wktTyperLut'][geom_type]
            wkt = WKT(row[colnames.index('SHAPE@WKT')])
            wkt_geometry_type = wkt.get_geometry_type(failIfUnknown=False)
            if wkt.has_known_geometry_type() and not wkt_geometry_type == expected_wkt_geom_type:
                for geometry_type in cfg['wktTyperLut']:
                    if wkt_geometry_type in cfg['wktTyperLut'][geometry_type]:
                        row_cache[geometry_type].append(row)
                        break
            else:
                cursor.insertRow(row)

            # Original code
            # cursor.insertRow(row)
            nelem_appended += 1

        except Exception as e:
            # TODO: Send denne til logger
            # TODO: Kjor diagnostikk pa element?

            # Still fails: Try to save as other geometry type
            expected_wkt_geom_type = cfg['wktTyperLut'][geom_type]
            wkt = WKT(row[colnames.index('SHAPE@WKT')])
            still_failed = True
            wkt_geometry_type = wkt.get_geometry_type(failIfUnknown=False)
            if wkt.has_known_geometry_type() and not wkt_geometry_type == expected_wkt_geom_type:
                still_failed = False
                for geometry_type in cfg['wktTyperLut']:
                    if wkt_geometry_type in cfg['wktTyperLut'][geometry_type]:
                        try:
                            row_cache[geometry_type].append(row)
                            nelem_appended += 1
                        except Exception as e:
                            logger.info('error: {}'.format(e))
                            still_failed = True
                        finally:
                            break
            else:
                arcpy.AddMessage('No geometry found.')

            if store_failed and has_geom:
                wkt = WKT(row[colnames.index('SHAPE@WKT')])
                expected_wkt_geom_type = cfg['wktTyperLut'][geom_type]
                if not wkt.has_known_geometry_type() or not wkt.get_geometry_type(failIfUnknown=False) == expected_wkt_geom_type:
                    row[colnames.index('SHAPE@WKT')] = '%s %s' % (cfg['wktTyperLut'][geom_type],
                                                                  cfg['wktEmpty'])
                try:
                    if not cursor:
                        cursor = InsertCursor(fc, colnames)
                    cursor.insertRow(row)
                    nelem_appended += 1
                except:
                    still_failed = True
                else:
                    still_failed = False
            if debug and still_failed:
                #raise
                logger.error('Insert error')

    # for key in GEOMETRY_POSTFIX.iterkeys():
    for key in GEOMETRY_POSTFIX: #.keys():
        # if row_cache.has_key(key):
        if key in row_cache:
            fc2 = fc + '_' + GEOMETRY_POSTFIX[key]
            # logger.info(u'(dbg) fc2: {}'.format(fc2))
            if arcpy.Exists(fc2):
                try:
                    if cursor:
                        del cursor
                        cursor = None
                    with InsertCursor(fc2, colnames) as cursor2:
                        for row in row_cache[key]:
                            cursor2.insertRow(row)
                except Exception as e:
                    logger.info('(dbg) : {}'.format(e))

    return nelem_appended


def _parse_full_time(t, fmt):
    try:
        dt, tz_str = t.split('+')
    except ValueError:
        offset = timedelta(hours=1.0)
        dt = t
    else:
        if len(tz_str) == 4:
            tz_str = '%s:%s' % (tz_str[:2], tz_str[2:])
        tdelta = float(tz_str.replace(':', '.'))
        offset = timedelta(hours=tdelta)
    return datetime.strptime(dt, fmt), offset

def get_deleted(cfg, metafile, dbid, max_pr_request=10000,
                deleted_since=None,
                verbose=True):
    # List of deleted features
    # TODO: Mindre hardkoding, meir dynamisk
    # Hardkoder uri enn sa lenge
    del_uri = '/endringer/objekttype/%s/slettet?rows=%d' % (str(dbid),
                                                            max_pr_request)
    rkeys = cfg['response_keys']
    e_types = cfg['endreTyperLut']
    deleted = []
    while del_uri:
        r = request(cfg['baseurl'], del_uri, cfg['headers'])
        # Read returned objects
        for e in r[rkeys['endre.trans']]:
            if e[rkeys['endre.trans.type']] == e_types['delete']:
                if deleted_since:
                    deleted_this = e[rkeys['endre.trans.dato']]
                    dtime_lim, offset_lim = _parse_full_time(deleted_since,
                                                             cfg['time_fmt']['full'])
                    dtime_del, offset_del = _parse_full_time(deleted_this,
                                                             cfg['time_fmt']['full'])

                    if dtime_del - offset_del < dtime_lim - offset_lim:
                        continue
                deleted.append(e[rkeys['endre.trans.id']])
        del_uri = r['next']

    return deleted


def multi_update_fc(cfg, meta, fc_list=None, objektTyper=None,
                    lokasjon=None, max_pr_request=10000,
                    extended_extras=True, verbose=False):
    '''

    :param cfg:
    :param meta:
    :param fc_list:
    :param objektTyper:
    :param lokasjon:
    :param max_pr_request:
    :param extended_extras:
    :param verbose:
    '''
    if not objektTyper:
        dbid_ls = cfg['include_by_dbid']['vegObjektTyper']
        objektTyper = [[{'id': dbid}] for dbid in dbid_ls]
    else:
        dbid_ls = [o['id'] for o in objektTyper]

    if fc_list and not len(fc_list) == len(objektTyper):
        msg = 'Lengde av *fc_list* må være lik lengde av *objekttyper*'
        logger.critical(msg)
        raise Exception(msg)
    if not fc_list:
        gdb = cfg['gdb']
        fc_list = []
        for dbid in dbid_ls:
            try:
                _, fc = meta.by_dbid(dbid)
                fc_list.append(join(gdb, fc))
            except:
                msg = 'Objekttype med id %d finnes ikke i NVDB' % dbid
                logger.warning(msg)
                continue

    kwargs = {'lokasjon': lokasjon,
              'max_pr_request': max_pr_request,
              'extended_extras': extended_extras,
              'verbose': verbose}

    for fc, objTyp in zip(fc_list, objektTyper):
        base_fc = basename(fc)
        fc = join(dirname(fc), arcpy.ValidateTableName(base_fc))

        if not arcpy.Exists(fc):
            msg = 'Featureklasse %s finnes ikke i %s' % (base_fc, gdb)
            logger.warning(msg)
            continue
        msg = 'Oppdaterer %s' % fc
        logger.info(msg)
        update_fc(cfg, meta, fc, objTyp, **kwargs)


def update_fc(cfg, metafile, fc, objektTyper,
              lokasjon=None, max_pr_request=10000, modified_since=None,
              extended_extras=False, verbose=False):
    '''

    :param cfg:
    :param metafile:
    :param fc:
    :param objektTyper:
    :param lokasjon:
    :param max_pr_request:
    :param modified_since:
    :param extended_extras:
    :param verbose:
    '''

    if len(objektTyper) > 1:
        msg = u'For mange objekttyper i søk'
        logger.critical(msg)
        raise Exception(msg)
    elif not objektTyper:
        msg = u'En objekttype må spesifiseres'
        logger.critical(msg)
        raise Exception(msg)

    # Get dbid from objektTyper
    dbid = objektTyper[0]['id']

    sok_uri, _ = metafile.by_name(cfg['names']['sokgrens'])
    rkeys = cfg['response_keys']

    # get location of schema, create it if it doesn't exist
    # and (implicitly) make sure dbid is available in nvdb

    #
    schema_uri, name = metafile.by_dbid(dbid)
    try:
        schema_grp = metafile[schema_uri]
    except KeyError:
        # TODO: Move this to function
        msg = 'Henter feltnavn og datatyper'
        logger.info(msg)
        schema_grp = metafile.create_group(schema_uri)
        schema_grp.attrs['dbid'] = dbid
        schema_grp.attrs['name'] = name
        _build_schema(cfg, schema_grp, schema_uri)
        _post_process_schema(cfg, metafile, schema_grp)

    # Hent modifisert fra nvdb, vanlig sok
    # Forst: sett opp antall og startpunkt
    if not modified_since and not 'endretdato' in objektTyper[0]:
        tmp_t = cfg['last_mod']
        if isinstance(tmp_t, datetime):
            fmt = '%Y-%m-%dT%H:%M:%S+0000'
            modified_since = datetime.strftime(tmp_t, fmt)
        else:
            modified_since = tmp_t

    objektTyper[0]['endretdato'] = modified_since

    try:
        nobj = objektTyper[0]['antall']
    except:
        nobj = pl.inf
    if 'start' in objektTyper[0]:
        next_obj = objektTyper[0]['start']
    else:
        next_obj = 1

    ndeleted = 0
    nupdated = 0
    nappended = 0
    count = 0
    msg = u'Henter data fra NVDB for %s' % fc
    logger.info(msg)

    deleted = get_deleted(cfg, metafile, dbid,
                          max_pr_request=max_pr_request,
                          deleted_since=modified_since,
                          verbose=verbose)

    while next_obj <= nobj:
        # Update query dict
        objektTyper[0]['antall'] = min([nobj - (next_obj - 1), max_pr_request])
        objektTyper[0]['start'] = next_obj
        sok = sok_parse(objektTyper, lokasjon=lokasjon)
        r = request(cfg['baseurl'], sok_uri, cfg['headers'], query=sok)

        nelem = r[rkeys['sok.totAntRet']]
        ndel, nup, napp = update_elements(cfg, r[rkeys['sok.res']][0],
                                          fc, schema_grp,
                                          extended_extras=extended_extras,
                                          deleted=deleted, verbose=verbose)
        # Get rid of deleted features after first run
        deleted = None
        # Break if no features were returned, object type
        # is exhausted
        if nelem == 0:
            break

        next_obj += nelem
        nappended += napp
        ndeleted += ndel
        nupdated += nup
        # Break if we get less than the limit
        if nelem < max_pr_request:
            break
        count += 1

        # Print statement pr iteration
        msg = u'%d sletta, %d endra og %d lagt til i %s' % (ndel,
                                                            nup,
                                                            napp,
                                                            fc)
        logger.info(msg)
    # Print statement at end
    msg = u'%d sletta, %d endra og %d lagt til i %s' % (ndeleted,
                                                        nupdated,
                                                        nappended,
                                                        fc)
    logger.info(msg)


def update_elements(cfg, r, fc, schema_grp, extended_extras=False,
                    deleted=None, verbose=False):
    gdb = dirname(fc)
    #=====================================================================
    # Schema
    #=====================================================================
    _, cn, _, _ = _cat_schema(schema_grp,
                              extended_extras=extended_extras,
                              verbose=verbose)
    schemanames = [arcyfy_name(n) if not 'SHAPE@' in n else n for n in cn]
    # Validate names, except SHAPE@WKT
#     colnames = [arcyfy_name(n) for n in cn]
#     f = lambda n, gdb: arcpy.ValidateFieldName(n, gdb)
#     colnames = [f(n, gdb) if not 'SHAPE@' in n else n for n in cn]

    # Only get columns both in schema and in fc
    fc_fn = [f.name for f in arcpy.ListFields(fc)]
    columns = [(f.name, f.type) for f in arcpy.ListFields(fc) if f.name in schemanames]
    colnames, data_types = zip(*columns)
    replace = cfg['replaceTyperLut']
    data_types = [replace[d] for d in data_types]

    geom_type = schema_grp.attrs['geometry_type']

    # Build lookup for attributes in r
    rkeys = cfg['response_keys']
    r_lut = {}
    if rkeys['vegObj'] in r:
        for ielem, elem in enumerate(r[rkeys['vegObj']]):
            objid = elem[rkeys['vegObj.objektId']]
            #assert not objid in r_lut
            r_lut[objid] = ielem

    nelem_updated = 0
    nelem_deleted = 0
    nelem_appended = 0
    with UpdateCursor(fc, colnames) as cursor:
        for row in cursor:
            record = dict(zip(colnames, row))
            objid_field = 'nvdb_%s' % rkeys['vegObj.objektId']
            objid = record[objid_field]
            # First check if row should be deleted
            if deleted and objid in deleted:
                cursor.deleteRow()
                #deleted.pop(deleted.index(objid))
                nelem_deleted += 1
#                 if objid in r_lut:
#                     r_lut.pop(objid)
                continue
            # Then check if row should be updated
            if objid in r_lut:
                elem = r[rkeys['vegObj']][r_lut[objid]]
                r_lut.pop(objid)
                attributes = {}
                extract_data(cfg, elem, attributes, prefix='nvdb', gdb=gdb)
                # Check for programming error
                assert objid == attributes[objid_field]
                # Check for empty geometry
                check_empty_geometry(cfg, attributes, geom_type)
                uprow = _populate_row(cfg, attributes, colnames, data_types)
                try:
                    cursor.updateRow(uprow)
                    nelem_updated += 1
                except:
                    # TODO: logging
                    pass

    if r_lut:
        # Might have som new rows, dump elements to fc
        with InsertCursor(fc, colnames) as cursor:
            for objid, idx in r_lut.items():
                elem = r[rkeys['vegObj']][idx]
                # Start med tom attributt-dict
                attributes = {}
                extract_data(cfg, elem, attributes, prefix='nvdb', gdb=gdb)
                assert objid == attributes[objid_field]
    #             if 'GeomPunkt' in attributes or 'GeomFlate' in attributes:
    #                 print('Geom')
                # Check for empty geometry
                check_empty_geometry(cfg, attributes, geom_type)
                row = _populate_row(cfg, attributes, colnames, data_types)

                try:
                    cursor.insertRow(row)
                    nelem_appended += 1
                except:
                    # TODO: Send denne til logger
                    # TODO: Kjor diagnostikk pa element?
    #                 print('ObjektId {0} feilet'.format(elem[rkeys['vegObj.objektId']]))
    #                 raise
                    pass

    return nelem_deleted, nelem_updated, nelem_appended


def one_funk_to_build_them_all(cfg, meta, gdb, lokasjon=None,
                               antall=1,
                               max_pr_request=10000, overwrite=True,
                               extended_extras=False, verbose='top'):

    '''
    Don't use it if you don't mean it!
    '''
    # TODO: Hvis len(objectTyper) == 1 og objektTyper[0]['id] == 'All'
    #       skal alle feature klasser hentast. objektTyper[0]['antall']
    #       vil da gjelde alle. Bygg liste av objektTyper og fc_list fra
    #       metafil. Putte det i annan funksjon? Holde denne lett
    #       og ledig?
    if not verbose or verbose.lower() == 'none':
        this_verbose = False
        child_verbose = False
    elif verbose.lower() == 'top':
        this_verbose = True
        child_verbose = False
    elif verbose.lower() == 'all':
        this_verbose = True
        child_verbose = True
    names = cfg['names']
    obj_loc, _ = meta.by_name(names['objekttyper'])
    obj_ids = meta[obj_loc]['id']
    obj_names = meta[obj_loc]['navn']

    nelem = 0
    nappend = 0

    for dbid, fc_tmp in zip(obj_ids, obj_names):

        objekt_typer = [{'id': int(dbid),
                         'antall': int(antall)
                        }]
        ofc = arcpy.ValidateTableName(fc_tmp, gdb)
        fc = join(gdb, ofc)
        if overwrite == 'skip' and arcpy.Exists(fc):
            if this_verbose:
                msg = u'Featureklasse %s eksisterer allerede, går til neste' % fc
                logger.info(msg)
            continue

        child_overwrite = True
        try:
            if this_verbose:
                msg = 'Oppretter og populerer %s' % ofc
                logger.info(msg)
            nget, npost = populate_fc(cfg, meta, fc, objekt_typer,
                                      egenskapsfilter=None,
                                      lokasjon=lokasjon,
                                      max_pr_request=max_pr_request,
                                      overwrite=child_overwrite,
                                      extended_extras=extended_extras,
                                      verbose=child_verbose)
        except HTTPError as e:
            if e.code == 404:
                msg = 'ERROR: Objekttype %s eksisterer ikke i NVDB' % fc
                logger.error(msg)
            else:
                msg_list = ['ERROR: Uidentifisert problem ved request til',
                            'NVDB-APIet']
                logger.error(' '.join(msg_list))
#         except:
#             t = sys.exc_info()[2]
#             tbinfo = tb.format_tb(t)[0]
#             msglist = ["PYTHON ERRORS:\nTraceback Info:\n",
#                        "{0}\nError Info:\n".format(tbinfo),
#                        "    {0}: {1}".format(sys.exc_type,
#                                              sys.exc_info())]
#             logger.error(''.join(msglist))
#             if arcpy.GetMessages(2):
#                 msgs = "GP ERRORS:\n{0}\n".format(arcpy.GetMessages(2))
#                 logger.error(msgs)
        else:
            nelem += nget
            nappend += npost
            if this_verbose:
                msg = u'Henta %d objekter, %d lagt til i %s' % (nget,
                                                                npost,
                                                                fc)
                logger.info(msg)
    if this_verbose:
        msg = u'Henta %d objekter, %d lagt til i %s' % (nelem,
                                                        nappend,
                                                        gdb)
        logger.info(msg)



def multi_populate_fc(cfg, meta, fc_list=None, objektTyper=None,
                      egenskapsfilter=None,
                      lokasjon=None,
                      max_pr_request=10000, overwrite=True,
                      extended_extras=True, verbose=False):
    '''
    Function to populate several ArcGIS feature classes with data from
    NVDB REST API in one go.

    :param cfg: Config dictionary, higly specific for this context
    :param meta: File instance created by *build_meta()*
    :param fc_list: list of paths to output feature classes located in fgdb.
        *fc_list* must be of same length as *objektTyper*. The 0th feature
        class in *fc_list* gets data retrieved from the 0th entry in
        *objektTyper*, etc.
    :param objektTyper: Dictionary with object types for nvdb sok. This
        will be parsed internally. This method operates on multiple object
        types. *objektTyper* must be of same length as *fc_list*. Data
        retrived from the 0th entry in *objektTyper* will be put in the
        0th feature class in *fc_list*, etc.
    :param egenskapsfilter: (optional) Egenskapsfilter for objects.
    :param lokasjon: (optional) Location dictionary for requested objects.
        Could contain bounding box or location by region/fylke/kommune,
        defaults to *None* which means no restriction by location
    :param max_pr_request: (optional) Integer number of objects obtained pr
        request. The function will loop until all features in dataset are
        collecter or until number restriction in *objektTyper* has been
        reached. Defaults to 500
    :param overwrite: (optional) Boolean value decides if existing feature class
        should be overwritten. Defaults to *True*. If *False*, objects will be
        appended to *fc* if *fc* exists. Otherwise the feature class will be
        created. Defaults to *True*

    '''
    if not objektTyper:
        dbid_ls = cfg['include_by_dbid']['vegObjektTyper']
        objektTyper = [[{'id': dbid}] for dbid in dbid_ls]
    else:
        dbid_ls = [o['id'] for o in objektTyper]

    if fc_list and not len(fc_list) == len(objektTyper):
        raise Exception('Lengde av *fc_list* må være lik lengde av *objekttyper*')
    if not fc_list:
        gdb = cfg['gdb']
        fc_list = []
        for dbid in dbid_ls:
            _, fc = meta.by_dbid(dbid)
            fc_list.append(join(gdb, fc))

    kwargs = {'egenskapsfilter': egenskapsfilter,
              'lokasjon': lokasjon,
              'max_pr_request': max_pr_request,
              'overwrite': overwrite,
              'extended_extras': extended_extras,
              'verbose': verbose}

    for fc, objTyp in zip(fc_list, objektTyper):
        fc = join(dirname(fc), arcpy.ValidateTableName(basename(fc)))
        populate_fc(cfg, meta, fc, objTyp, **kwargs)


def parse_lok(lokasjon):
    def parse_val(val):
        if isinstance(val, list):
            return ",".join(format(x) for x in val)
        else:
            return val

    # return {k: parse_val(v) for k, v in lokasjon.iteritems()}
    return {k: parse_val(v) for k, v in lokasjon.items()}

def populate_fc(cfg, metafile, fc, objektTyper, egenskapsfilter=None,
                lokasjon=None, vegreferanse=None, max_pr_request=10000,
                overwrite=True, extended_extras=False, store_failed=False,
                verbose=False, debug=False):
    '''
    Function to populate a single ArcGIS feature class with data from
    NVDB REST API

    :param cfg: dict
        Config dictionary, higly specific for this context
    :param metafile: BaseFile
        File instance created by 'build_meta()'
    :param fc: str, format
        Path to output feature class located in a fgdb
    :param objektTyper: list
        List of dictionaries with object types for nvdb sok. This
        will be parsed internally. This method operates on a single object
        type only and will raise exception if several object types are
        requested
    :param egenskapsfilter: str,format (optional, default None)
        Egenskapsfilter for objects.
    :param lokasjon: dictionary (optional, default None)
        Location dictionary for requested objects.
        Could contain bounding box or location by region/fylke/kommune,
        defaults to *None* which means no restriction by location
    :param vegreferanse: (optional, default None)
    :param max_pr_request: int (optional, default 10000)
        Integer number of objects obtained pr request.
        The method will loop until all features in dataset are collecter or
        until number restriction in *objektTyper* has been reached.
    :param overwrite: bool (optional, default True)
        Boolean value decides if existing feature class
        should be overwritten. Defaults to *True*. If *False*, objects will be
        appended to *fc* if *fc* exists. Otherwise the feature class will be
        created. Defaults to *True*
    :param extended_extras: bool (optional default False)
        Use extended schema
    :param store_failed: bool (optional default False)
        Try to store failed inserts without geometry
    :param verbose: bool (optional default False)
        Print more messages
    :param debug: bool (optional default False)
        Run in debug mode, no telling what might happen

    '''

    rkeys = cfg['response_keys']
    basepath = cfg['names']['sok']
    version = metafile.attrs['version']

    if len(objektTyper) > 1:
        raise Exception(u'For mange objekttyper i søk')
    elif not objektTyper:
        raise Exception(u'En objekttype må spesifiseres')

    # Get dbid from objektTyper
    dbid = objektTyper[0]['id']

    # Get egenskaps_filter from egenskapsfilter
    if egenskapsfilter != None:
        egenskaps_filter = egenskapsfilter
    else:
        egenskaps_filter = None

    # get location of schema, create it if it doesn't exist
    # and (implicitly) make sure dbid is available in nvdb
    schema_uri, name = metafile.by_dbid(dbid)
    try:
        schema_grp = metafile[schema_uri]
    except KeyError:
        # TODO: Move this to function
        if verbose:
            logger.info('Henter feltnavn og datatyper')
        schema_grp = metafile.create_group(schema_uri)
        schema_grp.attrs['dbid'] = dbid
        schema_grp.attrs['name'] = name
        _build_schema(cfg, schema_grp, schema_uri)
        _post_process_schema(cfg, metafile, schema_grp)

    # Get data from nvdb
    sok_uri = basepath.format(vegObjektTypeId=dbid)

    # create empty feature class/table with required fields
    _create_domains(cfg, fc, schema_grp, version, verbose)
    _create_fc(cfg, fc, schema_grp, version, overwrite=overwrite,
               extended_extras=extended_extras, verbose=verbose)

    # params = {k: v for k, v in objektTyper[0].iteritems()
    params = {k: v for k, v in objektTyper[0].items()
              if k not in cfg['sok_exclude']}

    params.update(cfg['sok_params'])
    if lokasjon:
        params.update(parse_lok(lokasjon))

    if egenskaps_filter != None:
        params.update({'egenskap': egenskaps_filter})

    # Get vegreferanse
    if vegreferanse:
        params.update({'vegreferanse': vegreferanse})

    nobj = params.get('antall', pl.inf)
    next_obj = params.get('start', None)

    nappended = 0
    nelem_get = 0
    count = 0
    if verbose:
        # Get statistics
        try:
            statpath = cfg['names']['statistikk']
            stat_uri = statpath.format(vegObjektTypeId=dbid)
            # stat_params = {k: v for k, v in params.iteritems()
            stat_params = {k: v for k, v in params.items()
                           if k not in cfg['stat_exclude']}

            r = request(cfg['baseurl'], stat_uri, cfg['headers'], query=urlencode(stat_params))
            ntot_stat = r[rkeys['statistikk.antall']]
            logger.info(u'Henter {} objekter fra NVDB for {}'.format(ntot_stat, fc))
        except Exception as e:
            logger.info(u'Henter objekter fra NVDB for {}'.format(fc))

    while nelem_get < nobj:
        # Update query dict
        params['antall'] = int(min([nobj - int(nelem_get), max_pr_request]))
        if next_obj is not None:
            params['start'] = next_obj

        try:
            r = request(cfg['baseurl'], sok_uri, cfg['headers'], query=urlencode(params))
        except MemoryError:
            # max_pr_request /= 2
            max_pr_request = int(max_pr_request/2)
            continue

        if rkeys['vegObjektType.metadata'] in r:
            meta = r[rkeys['vegObjektType.metadata']]
            nelem = meta[rkeys['vegObjektType.metadata.antReturnert']]
        else:
            nelem = 0
        # Break if no features were returned, object type
        # is exhausted
        if nelem == 0:
            break

        neste = meta[rkeys['vegObjektType.metadata.neste']]
        next_obj = neste[rkeys['vegObjektType.metadata.neste.start']]

        nelem_appended = _dump_elements(cfg,
                                        r,
                                        fc, schema_grp,
                                        extended_extras=extended_extras,
                                        store_failed=store_failed,
                                        debug=debug, total_get=nelem_get)
        nelem_get += nelem
        nappended += nelem_appended

        # Break if we get less than the limit NO! DON'T! WRONG
        # if nelem < max_pr_request: # Page limit is DYNAMIC, decreases  with heavy server load
            # break					 # We need to wait untill nelem=0 

        count += 1
        if verbose:
            # logger.info(u'Henta {} objekter, {} lagt til i {}'.format(nelem_get, nappended, format(fc)))
            logger.info(u'Henta {} objekter, {} lagt til.'.format(nelem_get, nappended))

    nelem_tot = nelem_get
    if verbose:
        count = int(arcpy.GetCount_management(fc).getOutput(0))
        # logger.info(u'Henta {} objekter, {} lagt til i {}'.format(nelem_tot, count, fc))
        logger.info(u'Av {} objekter er {} lagt til i {}'.format(nelem_tot, count, fc))

    # Delete empty featureclasses
    other_fc = []
    for _geometry in GEOMETRIES:
        fc2 = fc + '_' + GEOMETRY_POSTFIX[_geometry]
        if arcpy.Exists(fc2):
            count = int(arcpy.GetCount_management(fc2).getOutput(0))
            if count < 1:
                logger.info(u'Sletter tom featureklasse: {}'.format(fc2))
                arcpy.Delete_management(fc2)
            else:
                # logger.info(u'Beholder featureklasse {}'.format(fc2))
                logger.info(u'Av {} objekter er {} lagt til i {}'.format(nelem_tot, count, fc2))
                other_fc.append(fc2)

    return [nelem_tot, nappended, other_fc]


def lag_metrert_vegnett(fc, fc_out):
    """
    Lag nye kolonner i featureklasse for vegreferanse
    """
    HPID = ['HPID_A', 'HPID_B', 'HPID_C']
    # Add new columns for HPID
    arcpy.AddField_management(fc, HPID[0], "TEXT")
    arcpy.AddField_management(fc, HPID[1], "TEXT")
    arcpy.AddField_management(fc, HPID[2], "TEXT")

    # Spesifiser hvilke kolonner som må hentes for å lage innhold i HPID feltene
    FYLKE = 'Fylkesnummer'
    # KOMMUNE = 'Kommunenummer'
    KOMMUNE = 'nvdb_lokasjon_kommuner' #Listeobjekt?
    VEGKATEGORI = 'Vegkategori'
    VEGSTATUS = 'Vegstatus'
    VEGNUMMER = 'Vegnummer'
    HOVEDPARSELL = 'Hovedparsell'

    # For feltet "Vegkategori":
    # Coded value domain: Vegkategori_4566_2_07
    # 5492	Europaveg
    # 5493	Riksveg
    # 5494	Fylkesveg
    # 5495	Kommunal veg
    # 5496	Privat veg
    # 5497	Skogsbilveg
    vegkategori_verdi = {'5492': 'E',
                         '5493': 'R',
                         '5494': 'F',
                         '5495': 'K',
                         '5496': 'P',
                         '5497': 'S'
                        }

    # For feltet "Vegstatus":
    # Coded value domain: Vegstatus_4567_2_07
    # 5499	V - Eksisterende veg
    # 5505	W - Midlertidig veg
    # 5502	T - Midlertidig status bilveg
    # 5504	S - Eksisterende ferjestrekning
    # 12159	G - Gang-/sykkelveg
    # 12983	U - Midlertidig status gang-/sykkelveg
    # 13707	B - Beredskapsveg
    # 5501	M - Serviceveg
    # 5500	X - Rømningstunnel
    # 7041	A - Anleggsveg
    # 12160	H - Gang-/sykkelveg anlegg
    # 7042	P - Planlagt veg
    # 7046	E - Planlagt ferjestrekning
    # 12986	Q - Planlagt gang-/sykkelveg
    vegstatus_verdi = {'5499':  'V',
                       '5505':  'W',
                       '5502':  'T',
                       '5504':  'S',
                       '12159': 'G',
                       '12983': 'U',
                       '13707': 'B',
                       '5501':  'M',
                       '5500':  'X',
                       '7041':  'A',
                       '12160': 'H',
                       '7042':  'P',
                       '7046':  'E',
                       '12986': 'Q'
                      }

    # Kolonner som skal leses fra featureklassen
    colnames = HPID + [FYLKE, KOMMUNE, VEGKATEGORI, VEGNUMMER, VEGSTATUS, HOVEDPARSELL]

    # Finn index for hvert felt. (Gjør koden mer generisk og mere robust.)
    # Brukes for å slå opp i raden som leses med UpdateCursor
    idx_fylke = colnames.index(FYLKE)
    idx_kommune = colnames.index(KOMMUNE)
    idx_vegkategori = colnames.index(VEGKATEGORI)
    idx_vegnummer = colnames.index(VEGNUMMER)
    idx_vegstatus = colnames.index(VEGSTATUS)
    idx_hovedparsell = colnames.index(HOVEDPARSELL)

    logger.info(u'Bygger HPID kolonner...')
    # print('(dbg) {}'.format(colnames))

    rad_liste = {}
    with UpdateCursor(fc, colnames) as cursor:
        count = 0
        for row in cursor:
            # Hent data
            fylke = row[idx_fylke]
            kommune = row[idx_kommune].strip().strip('[').strip(']').split(',')[0].strip()
            vegkategori = row[idx_vegkategori]
            vegnummer = row[idx_vegnummer]
            vegstatus = row[idx_vegstatus]
            hovedparsell = row[idx_hovedparsell]

            # Bygger HPID_A
            hpid_a = '{:02d} {}{} {} {}'.format(int(fylke),
                                                vegkategori_verdi[str(row[idx_vegkategori])],
                                                vegstatus_verdi[str(row[idx_vegstatus])],
                                                int(vegnummer),
                                                int(hovedparsell)
                                               )
            # Bygger HPID_B
            hpid_b = '{:02d} 00 {}{} {} {}'.format(int(fylke),
                                                   vegkategori_verdi[str(row[idx_vegkategori])],
                                                   vegstatus_verdi[str(row[idx_vegstatus])],
                                                   int(vegnummer),
                                                   int(hovedparsell)
                                                  )
            # Bygger HPID_C
            hpid_c = '{:02d} {:02d} {}{} {} {}'.format(int(fylke),
                                                       int(kommune),
                                                       vegkategori_verdi[str(row[idx_vegkategori])],
                                                       vegstatus_verdi[str(row[idx_vegstatus])],
                                                       int(vegnummer),
                                                       int(hovedparsell)
                                                      )
            # Oppdater cursor med verdier.
            row[0] = hpid_a
            row[1] = hpid_b
            row[2] = hpid_c

            cursor.updateRow(row)

            # Tar vare på verdier siden det tar for lang tid å slå opp i database etterpå
            rad_liste[hpid_c] = [hpid_a,
                                 hpid_b,
                                 vegkategori_verdi[str(row[idx_vegkategori])],
                                 vegstatus_verdi[str(row[idx_vegstatus])],
                                 vegnummer
                                ]

            # Skriver de 20 første for debug
            # if count < 20:
            if count < 0:
                print('len(rad_liste)= {}'.format(len(rad_liste)))
                print('rad_liste[{}]={}'.format(hpid_c, rad_liste[hpid_c]))
                print('(dbg) {}'.format(row))
            count += 1

    # Ferdig med å fylle inn kolonner, kjør CreateRoutes med HPID_C som referanse
    logger.info(u'Kjører CreateRoutes...')
    arcpy.CreateRoutes_lr(in_line_features=fc, route_id_field="HPID_C", out_feature_class=fc_out,
                          measure_source="TWO_FIELDS", from_measure_field="FraMeter", to_measure_field="TilMeter",
                          coordinate_priority="UPPER_LEFT", measure_factor="1", measure_offset="0",
                          ignore_gaps="IGNORE", build_index="INDEX")

    # Lag nye kolonner og fyll inn data
    # Add new columns for HPID
    arcpy.AddField_management(fc_out, HPID[0], "TEXT")
    arcpy.AddField_management(fc_out, HPID[1], "TEXT")
    # arcpy.AddField_management(fc_out, HPID[2], "TEXT")
    arcpy.AddField_management(fc_out, VEGKATEGORI, "TEXT")
    arcpy.AddField_management(fc_out, VEGSTATUS, "TEXT")
    arcpy.AddField_management(fc_out, VEGNUMMER, "DOUBLE")

    search_fields = HPID + [VEGKATEGORI, VEGSTATUS, VEGNUMMER]

    count = 0
    antall = arcpy.GetCount_management(fc_out).getOutput(0)
    logger.info(u'Oppdater featureklasse etter CreateRoutes...')
    with UpdateCursor(fc_out, search_fields) as cursor:
        for row in cursor:
            count += 1
            # Har tatt vare på verdier i liste siden det tar for lang tid å slå opp i database.
            hpid = format(row[2])
            rad = rad_liste[hpid]
            # HPID_A
            row[0] = rad[0]
            # HPID_B
            row[1] = rad[1]
            # # HPID_C
            # row[2] = rad[2]
            # Vegkategori (text)
            row[3] = rad[2]
            # Vegstatus (text)
            row[4] = rad[3]
            # Vegnummer (double)
            row[5] = rad[4]

            # Oppdater cursor i fc_out
            cursor.updateRow(row)
            if count > 0 and count % 5000 == 0:
                logger.info('Antall rader oppdatert: {} av {}'.format(count, antall))

    logger.info('Antall rader oppdatert: {}'.format(count))
