# -*- coding: utf-8 -*-
'''
Lag metrert vegnett

'''
import os
import sys
if sys.version_info[0] < 3:
    from urllib2 import HTTPError
else:
    from urllib.error import HTTPError
import traceback as tb
import logging
import json
from arcpy import Parameter
import arcpy

CWDNAME = os.path.dirname(__file__)
sys.path.append(CWDNAME)

from nvdb_access.shared import (BaseTool, update_cfg, init_logging, sok_parse, request)

if sys.version_info[0] < 3:
    from nvdb_access.yaml import load
else:
    from nvdb_access.yaml_3 import load
from nvdb_access.da.meta_da import (build_meta, get_omrade_by_name, lokasjon_filter)
from nvdb_access.da.data_da import (populate_fc, lag_metrert_vegnett)

class LagMetrertVegnett(BaseTool):

    def __init__(self, debug=False):
        """
        Initialization.

        """
        self.label = 'Lag metrert vegnett'
        self.description = ''
        self.canRunInBackground = False
        arcpy.env.overwriteOutput = True

        self.__debug = debug
        self._ws = arcpy.env.workspace  # @UndefinedVariable


    @classmethod
    def set_config_meta(cls, cwd):
        cls._cwd = cwd
        cls._cfg = load(open(os.path.join(cls._cwd, r'nvdb_access\config\config.yaml')))
        update_cfg(cls._cfg, cls._cwd)

        cls._meta = build_meta(cls._cfg)


    #---------------------------------------------------------------------------------------
    # getParameterInfo
    #---------------------------------------------------------------------------------------
    def getParameterInfo(self):
        """
        Define parameter definitions.

        """

        p_features = Parameter(displayName=u'Featureklasse for objekter',
                               name='out_features',
                               datatype='DEFeatureClass',
                               parameterType='Required',
                               direction='Output')

        p_features.value = 'LagMetrertVegnett'

        p_lokasjon = Parameter(displayName=u'Områdebegrensning',
                               name='lokasjon',
                               datatype='GPString',
                               parameterType='Required',
                               direction='Input')

        p_rute = Parameter(displayName=u'Begrensning på riksvegrute',
                           name='rute',
                           datatype='GPString',
                           parameterType='Optional',
                           direction='Input')

        p_lokasjon_prev = Parameter(displayName=u'Områdebegrensning_prev',
                                    name='lokasjon_prev',
                                    datatype='GPString',
                                    parameterType='Derived',
                                    direction='Output')

        p_rute_prev = Parameter(displayName=u'Områdebegrensning_prev',
                                name='rute_prev',
                                datatype='GPString',
                                parameterType='Derived',
                                direction='Output')

        p_r_code = Parameter(displayName=u'r_code',
                             name='r_code',
                             datatype='GPLong',
                             parameterType='Derived',
                             direction='Output')
        p_r_code.value = 200

        # Vegkategorier
        p_gang_sykkelveger = Parameter(displayName=u'Gang-/sykkelveger',
                                       name='gang_sykkelveger',
                                       datatype='GPBoolean',
                                       parameterType='Required',
                                       direction='Input')
        p_gang_sykkelveger.value = False

        p_bilveger = Parameter(displayName=u'Bilveger',
                               name='bilveger',
                               datatype='GPBoolean',
                               parameterType='Required',
                               direction='Input')
        p_bilveger.value = True

        p_vegkategorier = Parameter(displayName=u'Vegkategorier for Bilveger',
                                    name='vegkategorier',
                                    datatype='GPString',
                                    parameterType='Required',
                                    direction='Input',
                                    multiValue=True)
        p_vegkategorier.filter.type = 'ValueList'
        p_vegkategorier.filter.list = [u'Europaveg', u'Riksveg', u'Fylkesveg', u'Kommunal_veg', u'Privat_veg', u'Skogsbilveg']
        p_vegkategorier.value = [u'Europaveg', u'Riksveg', u'Fylkesveg']

        if self._meta:
            names = self._cfg['names']
            # Get regioner/fylker
            region_uri, _ = self._meta.by_name(names['regioner'])
            regioner = ['Region %s' % n for n in self._meta[region_uri]['navn']]
            fylke_uri, _ = self._meta.by_name(names['fylker'])
            fylker = sorted(self._meta[fylke_uri]['navn'])
            # Create filter list for lokasjoner
            if sys.version_info[0] < 3:
                lokasjoner = [self._cfg['lokasjonLut']['bbox'],
                              self._cfg['lokasjonLut']['alle']
                             ]
            else:
                lokasjoner = [self._cfg['lokasjonLut']['bbox_pro'],
                              self._cfg['lokasjonLut']['alle']
                             ]
            lokasjoner.extend(regioner)
            lokasjoner.extend(fylker)
            # Create filter list for riksvegruter
            ruter_uri, _ = self._meta.by_name(names['riksvegruter'])
            rr_tup = zip(self._meta[ruter_uri]['navn'],
                         self._meta[ruter_uri]['beskrivelse'])
            riksruter = ['%s: %s' % rr for rr in rr_tup]

        else:
            # obj_typer = ['Klarte ikke å hente objekttyper fra nvdb']
            #p_objtype.value = obj_typer[0]
            lokasjoner = ['Klarte ikke å hente områder fra nvdb']
            p_lokasjon.value = lokasjoner[0]
            riksruter = ['Klarte ikke å hente riksvegruter fra nvdb']
            p_rute.value = riksruter[0]

        p_lokasjon.filter.list = lokasjoner
        p_rute.filter.list = riksruter

        return [p_features, p_lokasjon, p_rute,
                p_gang_sykkelveger,
                p_bilveger,
                p_vegkategorier,
                p_lokasjon_prev, p_rute_prev,
                p_r_code]

    #---------------------------------------------------------------------------------------
    # updateParameters
    #---------------------------------------------------------------------------------------
    def updateParameters(self, parameters):
        """
        Modify the values and properties of parameters before internal
        validation is performed. This method is called whenever a parameter
        has been changed.

        """
        # Folg disse prinsippene:
        # 1 - Hvis ny objekttype: Bygg fc-navn på nytt
        # 2 - Hvis andre parametere er endret: Bytt gammel streng med ny streng
        self.refresh(parameters)
        altered_params = [p.altered for p in parameters]
        if any(altered_params):
            out_features = self['out_features'].valueAsText
            out_gdb = os.path.dirname(out_features)
            out_fc = os.path.basename(out_features)
            if not out_fc:
                out_fc = 'LagMetrertVegnett'
            lok_prev = self['lokasjon_prev'].valueAsText
            rute_prev = self['rute_prev'].valueAsText
            # Rebuild fc_name if rechosen obj_type
            lok_lut = self._cfg['lokasjonLut']

            lok_raw = self['lokasjon'].valueAsText
            if not lok_raw or lok_raw == lok_lut['alle']:
                lok = None
            elif lok_raw == lok_lut['bbox'] or lok_raw == lok_lut['bbox_pro']:
                lok = '_bbox'
            elif lok_raw.startswith('Region'):
                lok = u'_{}'.format(lok_raw.split(' ')[1].lower())
            else:
                # Parse fylke lokasjon
                lok_ls = lok_raw.split('-')
                lok = ''.join([s.capitalize() for s in lok_ls])
                lok = lok.replace(' ', '_')
                lok = u'_{}'.format(lok)

            # Replace og ta vare på brukerendringer
            # Strip endings if not user has changed it
            ending = ''
            if rute_prev and rute_prev in out_fc:
                out_fc = out_fc.replace(rute_prev, '')
                ending = u'{}{}'.format(rute_prev, ending)
            if lok_prev and not lok:
                out_fc = out_fc.replace(lok_prev, '')
            elif lok_prev and lok:
                out_fc = out_fc.replace(lok_prev, lok)
            elif lok and not lok_prev:
                out_fc = u'{}{}'.format(out_fc, lok)
            if ending:
                out_fc = u'{}{}'.format(out_fc, ending)
            self['lokasjon_prev'].value = lok

            rute_raw = self['rute'].valueAsText
            if not rute_raw:
                rute = None
            else:
                # Parse rute
                rute = u'_{}'.format(rute_raw.split(': ')[0].lower().replace(' ', '_').replace('-', '_').replace('/', '_'))
                while '__' in rute:
                    rute = rute.replace('__', '_')

            # Replace og ta vare på brukerendringer
            # Strip endings if not user has changed it
            ending = ''
            if rute_prev and not rute:
                out_fc = out_fc.replace(rute_prev, '')
            elif rute_prev and rute:
                out_fc = out_fc.replace(rute_prev, rute)
            elif rute and not rute_prev:
                out_fc = u'{}{}'.format(out_fc, rute)
            if ending:
                out_fc = u'{}{}'.format(out_fc, ending)
            self['rute_prev'].value = rute

            # Sett featureklassenavn
            self['out_features'].value = os.path.join(out_gdb, out_fc)

            # Er check-boks for Bilveger endret?
            if not self['bilveger'].hasBeenValidated:
                if self['bilveger'].value:
                    self['gang_sykkelveger'].value = False
                else:
                    self['gang_sykkelveger'].value = True

            # Er check-boks for Gang-/sykkelveger endret?
            if not self['gang_sykkelveger'].hasBeenValidated:
                if self['gang_sykkelveger'].value:
                    self['bilveger'].value = False
                else:
                    self['bilveger'].value = True

            # Er Vegkategorier for Bilveger endret?
            # Har verdi
            if self['vegkategorier'].valueAsText:
                vegkategorier = (self['vegkategorier'].valueAsText).split(';')
                bilveger = [u'Europaveg', u'Riksveg', u'Fylkesveg', u'Kommunal_veg', u'Privat_veg', u'Skogsbilveg']
                nye_vegkategorier = []
                if vegkategorier:
                    if len(vegkategorier) > 0:
                        for kategori in vegkategorier:
                            if kategori in bilveger:
                                nye_vegkategorier.append(kategori)
                self['vegkategorier'].value = nye_vegkategorier
            # Har ikke noen verdi, setter default.
            else:
                self['vegkategorier'].value = [u'Europaveg', u'Riksveg', u'Fylkesveg']

    #---------------------------------------------------------------------------------------
    # updateMessages
    #---------------------------------------------------------------------------------------
    def updateMessages(self, parameters):
        """
        Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation.

        """
        self.refresh(parameters)
        if not self._meta:
            msg_ls = ['Klarte ikke å hente informasjon fra nvdb,',
                      'sjekk internett-tilkoblingen!']
            for param in parameters:
                param.setErrorMessage(' '.join(msg_ls))

    #---------------------------------------------------------------------------------------
    # execute
    #---------------------------------------------------------------------------------------
    def execute(self, parameters, messages):
        """
        The source code of the tool.

        """
        obj_type = 'Vegreferanse'
        egenskapsfilter = None
        if self.__debug:
            lokasjon = format(self['lokasjon'].value)
            out_features = format(self['out_features'].value)
            riksrute = self['rute'].value
            if riksrute:
                riksrute = format(riksrute)
        else:
            lokasjon = self['lokasjon'].valueAsText
            out_features = self['out_features'].valueAsText
            riksrute = self['rute'].valueAsText

        self['out_features'].value = None

        init_logging(verbose=self.__debug)
        logger = logging.getLogger('lag_metrert_vegnett_pyt')
        # Skal alltid ha med utvida attributttabell for metrert vegnett
        extended_extras = True

        # Hent valgte vegkategorier
        # For bilveg legges det på status som ikke er gang-/sykkelveg
        vegreferanse = ''
        if self['vegkategorier'].valueAsText and self['bilveger'].value:
            vegkategorier = (self['vegkategorier'].valueAsText).split(';')
            veg_status = ['V', 'W', 'T', 'S', 'B', 'M', 'X', 'A', 'P', 'E']
        else:
            # 12159	G - Gang-/sykkelveg
            # 12983	U - Midlertidig status gang-/sykkelveg
            # 12160	H - Gang-/sykkelveg anlegg
            # 12986	Q - Planlagt gang-/sykkelveg
            vegkategorier = ['E', 'R', 'F', 'K', 'P', 'S']
            veg_status = ['G', 'U', 'H', 'Q']

        if vegkategorier:
            for kategori in vegkategorier:
                for status in veg_status:
                    if vegreferanse:
                        vegreferanse = vegreferanse + ','
                    vegreferanse = vegreferanse + kategori[:1] + status
        try:
            # Sett lokasjonsfilter
            if lokasjon == self._cfg['lokasjonLut']['alle']:
                loc_filter = {}
                omrade = lokasjon
            elif lokasjon == self._cfg['lokasjonLut']['bbox'] or lokasjon == self._cfg['lokasjonLut']['bbox_pro']:
                # TODO: Test om gjeldende extent er ok
                extent = None
                if sys.version_info[0] < 3:
                    # ArcMap
                    if self.__debug:
                        mxd = arcpy.mapping.MapDocument(r'C:\Users\eivindn\TestOutputs\svv\nvdb.mxd')
                    else:
                        mxd = arcpy.mapping.MapDocument("CURRENT")
                    df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
                    extent = df.extent
                    del mxd, df

                else:
                    # ArcGIS Pro
                    aprx = arcpy.mp.ArcGISProject("CURRENT")
                    map = aprx.listMaps()[0]
                    extent = map.defaultCamera.getExtent()
                    del aprx, map

                if extent:
                    loc_filter = lokasjon_filter('bbox', extent)
                    omrade = lokasjon
                else:
                    logging.warning('Ikke noe kartutsnitt er funnet.')
                    return

            elif lokasjon.startswith('Region'):
                region = lokasjon.split(' ')[1]
                omrade = get_omrade_by_name(self._cfg, self._meta,
                                            'regioner', region)
                loc_filter = lokasjon_filter('region', [omrade])
            else:
                omrade = get_omrade_by_name(self._cfg, self._meta,
                                            'fylker', lokasjon)
                loc_filter = lokasjon_filter('fylke', [omrade])
            # Legg til begrensing på riksvegrute
            if riksrute:
                rute = riksrute.split(': ')[0]
                loc_filter['riksvegrute'] = [rute]

            # Sett objekttypefilter
            _, dbid = self._meta.by_name(obj_type)
            objekt_filter = [{'id': dbid}]

            logger.info(u'\nObjekttype: {}'.format(obj_type))

            # Validerer output feature navn
            out_gdb = os.path.dirname(out_features)
            out_fc = os.path.basename(out_features)
            out_fc = arcpy.ValidateTableName(out_fc, out_gdb)
            out_features = os.path.join(out_gdb, out_fc)

            # Generer temporært feature navn
            out_fc_temp = 'Vegreferanse_temp'
            out_features_temp = os.path.join(out_gdb, out_fc_temp)

            # Sett segmentering til True ???
            # self._cfg['sok_params']['segmentering'] = True
            # logger.info('(dbg) sok_params: {}'.format(self._cfg['sok_params']))

            populate_fc(self._cfg, self._meta, out_features_temp,
                        objekt_filter,
                        egenskapsfilter=egenskapsfilter,
                        vegreferanse=vegreferanse,
                        lokasjon=loc_filter,
                        max_pr_request=10000, overwrite=True,
                        extended_extras=extended_extras, verbose=True)

            lag_metrert_vegnett(out_features_temp, out_features)

            # Slett evt temoporær fil
            # if arcpy.Exists(out_features_temp):
            #     try:
            #         # logger.info(u'Sletter: {}'.format(out_features_temp))
            #         arcpy.Delete_management(out_features_temp)
            #     except:
            #         # logger.info(u'Sletting feilet for: {}'.format(out_features_temp))
            #         pass

            # Kopier inn gruppe lyr-fil med Vegkategorier:
            if sys.version_info[0] < 3:
                # ArcMap
                mxd = arcpy.mapping.MapDocument("CURRENT")
                dataframe = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
                lyrfil_navn = os.path.join(os.path.dirname(__file__), 'lyrfiler', u'Vegreferanse.lyr')
                group_layer_name_old = u'{}_Vegkategorier'.format(out_fc)
                group_layer_name = format(out_fc)
                if os.path.exists(lyrfil_navn):
                    grouplayer = arcpy.mapping.Layer(lyrfil_navn)
                    grouplayer.name = group_layer_name
                    # Sjekk om layer allerede finnes i kartdokumentetet
                    for lyr in arcpy.mapping.ListLayers(mxd, "*", dataframe):
                        if lyr.name in [group_layer_name, group_layer_name_old]:
                            arcpy.mapping.RemoveLayer(dataframe, lyr)
                    arcpy.mapping.AddLayer(dataframe, grouplayer)

                    # Erstatt datakilde (replaceDataSource) for layer eller fjern layer dersom ikke i bruk
                    first_layer = True
                    for lyr in arcpy.mapping.ListLayers(mxd):
                        group_path = u'{}\\'.format(group_layer_name)
                        lyr_path = u'{}'.format(lyr)
                        if lyr_path.startswith(group_path):
                            layer = lyr.name.strip().replace(' ', '_')
                            if layer in vegkategorier:
                                lyr.replaceDataSource(out_gdb, "FILEGDB_WORKSPACE", u'{}'.format(out_fc))
                                if first_layer:
                                    lyr.visible = True
                                    first_layer = False
                                else:
                                    lyr.visible = False
                            else:
                                arcpy.mapping.RemoveLayer(dataframe, lyr)

                del mxd, dataframe

            else:
                # ArcGIS Pro
                aprx = arcpy.mp.ArcGISProject("CURRENT")
                map = aprx.listMaps()[0]
                lyrfil_navn = os.path.join(os.path.dirname(__file__), 'lyrfiler', u'Vegreferanse.lyr')
                group_layer_name_old = u'{}_Vegkategorier'.format(out_fc)
                group_layer_name = format(out_fc)
                if os.path.exists(lyrfil_navn):
                    grouplayer = arcpy.mp.LayerFile(lyrfil_navn)
                    # Sjekk om nytt layer allerede finnes i kartdokumentetet
                    for lyr in map.listLayers():
                        if lyr.name in [group_layer_name, group_layer_name_old]:
                            map.removeLayer(lyr)
                    # Add layer and set name
                    map.addLayer(grouplayer)
                    for lyr in map.listLayers():
                        if lyr.isGroupLayer and lyr.name == 'Vegreferanse':
                            lyr.name = group_layer_name

                    # Erstatt datakilde (replaceDataSource) for layer eller fjern layer dersom ikke i bruk
                    group_path = u'{}\\'.format(group_layer_name)
                    first_layer = True
                    for lyr in map.listLayers():
                        lyr_path = u'{}'.format(lyr)
                        if lyr.isGroupLayer:
                            if lyr.name in [group_layer_name]:
                                lyr.visible = True
                        elif lyr_path.startswith(group_path) or lyr_path.startswith('Vegreferanse\\'):
                            layer = lyr_path.strip().replace(' ', '_').split('\\')[1].strip()
                            if layer in vegkategorier:
                                # Repair data source
                                new_connection_info = {}
                                current_connection_info = lyr.connectionProperties
                                for key in current_connection_info:
                                    new_connection_info[key] = current_connection_info[key]
                                    if key == 'dataset':
                                        new_connection_info[key] = out_fc
                                    if key == 'connection_info':
                                        new_connection_info[key]['database'] = out_gdb
                                lyr.updateConnectionProperties(current_connection_info, new_connection_info)
                                if first_layer:
                                    lyr.visible = True
                                    first_layer = False
                                else:
                                    lyr.visible = False
                            else:
                                if layer not in ['Vegreferanse']:
                                    map.removeLayer(lyr)

                del map, aprx

        except HTTPError as e:
            if e.code == 404:
                logger.error('ERROR: Objekttype eksisterer ikke i NVDB')
            else:
                t_info = sys.exc_info()[2]
                tbinfo = tb.format_tb(t_info)[0]
                msglist = ["NVDB ERRORS:"]
                response = json.loads(e.read())
                error = response[0]
                if 'code' in error:
                    msglist.append('\n    NVDB Error code: {}'.format(error['code']))
                if 'message' in error:
                    msglist.append('\n    NVDB Error message: {}'.format(error['message']))
                msglist.append('\nTraceback Info:')
                msglist.append("\n{}\nError Info:".format(tbinfo))
                msglist.append("\n    {}: {}".format(sys.exc_info()[0], sys.exc_info()[1]))
                logger.error(''.join(msglist))

            if arcpy.Exists(out_features):
                try:
                    arcpy.Delete_management(out_features)
                except:
                    pass

        except:
            t = sys.exc_info()[2]
            tbinfo = tb.format_tb(t)[0]
            msglist = ["PYTHON ERRORS:\nTraceback Info:\n",
                       "{}\nError Info:\n".format(tbinfo),
                       "    {}: {}".format(sys.exc_info()[0], sys.exc_info()[1])
                      ]
            logger.error(''.join(msglist))
            if arcpy.GetMessages(2):
                msgs = "GP ERRORS:\n{0}\n".format(arcpy.GetMessages(2))
                logger.error(msgs)

#---------------------------------------------------------------------------------------
# main
#---------------------------------------------------------------------------------------
if __name__ == '__main__':
    THIS = os.path.abspath(__file__)
    CWD = os.path.dirname(THIS)
    # print(CWD)

    LagMetrertVegnett.set_config_meta(CWD)
    # getter = LagMetrertVegnett(debug=True)
    getter = LagMetrertVegnett()
    # getter['obj_type'].value = u'Vegreferanse'
    getter['lokasjon'].value = 'Region ØST'
    getter['out_features'].value = r'C:\\Users\\tores\\Documents\\ArcGIS\\Default.gdb\\Metrert_vegnett_øst_test'
    getter['rute'].value = None #u'RUTE6A 2014-2023: E6 Oslo - Trondheim'
    getter['bilveger'].value = True
    getter['vegkategorier'].value = [u'Europaveg', u'Riksveg', u'Fylkesveg']
    getter['gang_sykkelveger'].value = False
    # getter['vegkategorier'].value = u'Gang-/sykkelveg'
    getter.execute(getter.parameters, messages=None)
