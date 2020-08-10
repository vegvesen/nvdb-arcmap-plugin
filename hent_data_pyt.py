# -*- coding: utf-8 -*-
'''
Hent data

'''
from __future__ import (print_function, unicode_literals, division)

import os
import sys
if sys.version_info[0] < 3:
    from urllib2 import HTTPError
else:
    from urllib.error import HTTPError
import logging
import json
import re
import traceback as tb
from arcpy import Parameter
import arcpy

CWDNAME = os.path.dirname(__file__)
sys.path.append(CWDNAME)

from nvdb_access.shared import (BaseTool, update_cfg, init_logging, request)

if sys.version_info[0] < 3:
    from nvdb_access.yaml import load
else:
    from nvdb_access.yaml_3 import load
from nvdb_access.da.meta_da import (build_meta, get_omrade_by_name, lokasjon_filter)
from nvdb_access.da.data_da import populate_fc

class HentData(BaseTool):

    def __init__(self, debug=False):
        """
        Initialization.

        """
        self.label = 'Hent data'
        self.description = ''
        self.canRunInBackground = False
        arcpy.env.overwriteOutput = True

        self.__debug = debug
        self._ws = arcpy.env.workspace  # @UndefinedVariable

    @classmethod
    def set_config_meta(cls, cwd):
        cls._cwd = cwd
        # cls._cfg = load(file(os.path.join(cls._cwd, r'nvdb_access\config\config.yaml')))
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

        # Ny tabell for objekttype, featureklassenavn og egenskapsfilter
        p_objekttabell = Parameter(displayName=u'Objekttyper',
                                   name='objekttabell',
                                   datatype='GPValueTable',
                                   parameterType='Required',
                                   direction='Input')

        p_objekttabell.columns = [['GPString', u'Objekttype'],
                                  ['GPString', u'Featureklassenavn'],
                                  ['GPString', u'Egenskapsfilter'],
                                  ['GPString', u'Mulige egenskapstyper (info)']
                                 ]

        # Navn på filgeodatabase
        p_geodatabase = Parameter(displayName=u'Navn på filgeodatabase',
                                  name='out_geodatabase',
                                  datatype='DEWorkspace',
                                  parameterType='Required',
                                  direction='Input')
        p_geodatabase.value = self._ws

        # Områdebegrensning
        p_lokasjon = Parameter(displayName=u'Områdebegrensning',
                               name='lokasjon',
                               datatype='GPString',
                               parameterType='Required',
                               direction='Input')

        # Utvida attributt-tabell
        p_extended = Parameter(displayName=u'Utvida attributt-tabell',
                               name='extended',
                               datatype='GPBoolean',
                               parameterType='Required',
                               direction='Input')
        p_extended.value = False

        # Begrensning på riksvegrute
        p_rute = Parameter(displayName=u'Begrensning på riksvegrute',
                           name='rute',
                           datatype='GPString',
                           parameterType='Optional',
                           direction='Input')

        # Definisjoner for å spare på gamle variable
        p_objtype_prev = Parameter(displayName=u'Objekttype_prev',
                                   name='obj_type_prev',
                                   datatype='GPString',
                                   parameterType='Derived',
                                   direction='Output')

        p_egenskapsfilter_prev = Parameter(displayName=u'Egenskapsfilter_prev',
                                           name='egenskapsfilter_prev',
                                           datatype='GPString',
                                           parameterType='Derived',
                                           direction='Output',
                                           multiValue=True)
        egenskapsfilter_prev_values = []
        for n in range(20):
            egenskapsfilter_prev_values.append(None)
        p_egenskapsfilter_prev.values = egenskapsfilter_prev_values

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

        p_extended_prev = Parameter(displayName=u'Utvida attributt-tabell_prev',
                                    name='extended_prev',
                                    datatype='GPBoolean',
                                    parameterType='Derived',
                                    direction='Output')
        p_extended_prev.value = False

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

        # Lag output features. Har forberedt 20
        p_feature_0 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(0),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_1 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(1),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_2 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(2),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_3 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(3),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_4 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(4),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_5 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(5),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_6 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(6),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_7 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(7),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_8 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(8),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_9 = Parameter(displayName=u'Featureklasse',
                                name='out_feature_{}'.format(9),
                                datatype='DEFeatureClass',
                                parameterType='Derived',
                                direction='Output')
        p_feature_10 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(10),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_11 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(11),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_12 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(12),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_13 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(13),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_14 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(14),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_15 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(15),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_16 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(16),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_17 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(17),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_18 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(18),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')
        p_feature_19 = Parameter(displayName=u'Featureklasse',
                                 name='out_feature_{}'.format(19),
                                 datatype='DEFeatureClass',
                                 parameterType='Derived',
                                 direction='Output')

        # Lag other output features.
        p_other_featureclass = Parameter(displayName=u'Featureklasse',
                                         name='other_featureclass',
                                         datatype='DEFeatureClass',
                                         parameterType='Derived',
                                         direction='Output',
                                         multiValue=True)
        p_other_featureclass.values = []

        if self._meta:
            names = self._cfg['names']
            # Get all objekttyper
            obj_loc, _ = self._meta.by_name(names['objekttyper'])
            obj_typer = sorted(self._meta[obj_loc]['navn'])
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
            obj_typer = ['Klarte ikke å hente objekttyper fra nvdb']
            lokasjoner = ['Klarte ikke å hente områder fra nvdb']
            p_lokasjon.value = lokasjoner[0]
            riksruter = ['Klarte ikke å hente riksvegruter fra nvdb']
            p_rute.value = riksruter[0]

        p_lokasjon.filter.list = lokasjoner
        p_rute.filter.list = riksruter
        p_objekttabell.filters[0].list = obj_typer

        return [p_objekttabell, p_geodatabase,
                p_lokasjon, p_extended, p_rute,
                p_gang_sykkelveger,
                p_bilveger,
                p_vegkategorier,
                p_objtype_prev, p_egenskapsfilter_prev, p_lokasjon_prev, p_rute_prev,
                p_feature_0, p_feature_1, p_feature_2, p_feature_3, p_feature_4,
                p_feature_5, p_feature_6, p_feature_7, p_feature_8, p_feature_9,
                p_feature_10, p_feature_11, p_feature_12, p_feature_13, p_feature_14,
                p_feature_15, p_feature_16, p_feature_17, p_feature_18, p_feature_19,
                p_extended_prev, p_r_code, p_other_featureclass]

    #---------------------------------------------------------------------------------------
    # updateParameters
    #---------------------------------------------------------------------------------------
    def updateParameters(self, parameters):
        """
        Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed.

        """
        # global egenskapsfilter_prev_values
        # Følg disse prinsippene:
        # 1 - Hvis ny objekttype: Bygg fc-navn på nytt
        # 2 - Hvis andre parametere er endret: Bytt gammel streng med ny streng
        self.refresh(parameters)
        altered_params = [p.altered for p in parameters]
        if any(altered_params):
            # Output geodatabase
            out_geodatabase = self['out_geodatabase'].valueAsText
            if out_geodatabase is None:
                if self._ws:
                    if arcpy.Exists(self._ws):
                        out_geodatabase = self._ws
                        self['out_geodatabase'].value = self._ws
            else:
                if not arcpy.Exists(out_geodatabase):
                    self['out_geodatabase'].value = self._ws

            # Oppdater innholdet i objekttabell for hvert objekt
            objekttabell = self['objekttabell'].values
            egenskapsfilter_prev_values = self['egenskapsfilter_prev'].values

            # Sjekk om siste objekttype er lovlig (finnes i filter):
            objekttype_is_ok = True
            if objekttabell and len(objekttabell) > 0:
                objekttype = objekttabell[len(objekttabell)-1][0]
                if not objekttype in self['objekttabell'].filters[0].list:
                    del objekttabell[len(objekttabell)-1]
                    self['objekttabell'].values = objekttabell
                    objekttype_is_ok = False

            if objekttabell and objekttype_is_ok:
                lok_prev = self['lokasjon_prev'].valueAsText
                rute_prev = self['rute_prev'].valueAsText

                for n in range(len(objekttabell)):
                    if objekttabell[n][1] is None or objekttabell[n][1] == "":
                        objekttabell[n][1] = objekttabell[n][0].replace(r' ', '_').replace(r'/', '_')

                        if lok_prev:
                            objekttabell[n][1] = u'{}{}'.format(objekttabell[n][1], lok_prev)
                        if rute_prev:
                            objekttabell[n][1] = u'{}{}'.format(objekttabell[n][1], rute_prev)
                        # Fyll inn egenskapstyper
                        _, dbid = self._meta.by_name(objekttabell[n][0])
                        obj_uri = u'/vegobjekter/{}'.format(dbid)
                        query = 'inkluder=egenskaper&antall=1'
                        result = request(self._cfg['baseurl'], obj_uri, self._cfg['headers'], query=query)
                        objekter = result['objekter'][0]
                        egenskapstyper = ''
                        # if objekter.has_key('egenskaper'):
                        if 'egenskaper' in objekter:
                            for egenskaper in objekter['egenskaper']:
                                if egenskapstyper:
                                    egenskapstyper += ', '
                                egenskapstyper += u'{} ({})'.format(egenskaper['id'], egenskaper['navn'])
                        objekttabell[n][3] = egenskapstyper

                    egenskapsfilter_prev = egenskapsfilter_prev_values[n]

                    out_fc = objekttabell[n][1]
                    out_fc = out_fc.replace('_utvida', '') # Tar bort fordi den legges på

                    # Rebuild fc_name if rechosen obj_type
                    lok_lut = self._cfg['lokasjonLut']

                    # Lokasjon
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
                    if self['extended'].value and '_utvida' in out_fc:
                        out_fc = out_fc.replace('_utvida', '')
                        ending = u'{}{}'.format('_utvida', ending)
                    if rute_prev and rute_prev in out_fc:
                        out_fc = out_fc.replace(rute_prev, '')
                        ending = u'{}{}'.format(rute_prev, ending)
                    if egenskapsfilter_prev and egenskapsfilter_prev in out_fc:
                        out_fc = out_fc.replace(egenskapsfilter_prev, '')
                        ending = u'{}{}'.format(egenskapsfilter_prev, ending)
                    if lok_prev and not lok:
                        out_fc = out_fc.replace(lok_prev, '')
                    elif lok_prev and lok:
                        out_fc = out_fc.replace(lok_prev, lok)
                    elif lok and not lok_prev:
                        out_fc = u'{}{}'.format(out_fc, lok)
                    if ending:
                        out_fc = u'{}{}'.format(out_fc, ending)
                    self['lokasjon_prev'].value = lok

                    # Rute
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
                    if self['extended'].value and '_utvida' in out_fc:
                        out_fc = out_fc.replace('_utvida', '')
                        ending = u'{}{}'.format('_utvida', ending)

                    if rute_prev and not rute:
                        out_fc = out_fc.replace(rute_prev, '')
                    elif rute_prev and rute:
                        out_fc = out_fc.replace(rute_prev, rute)
                    elif rute and not rute_prev:
                        out_fc = u'{}{}'.format(out_fc, rute)
                    if ending:
                        out_fc = u'{}{}'.format(out_fc, ending)
                    self['rute_prev'].value = rute

                    # Utvida tabell
                    try:
                        if self['extended'].value:
                            out_fc = u'{}_utvida'.format(out_fc)
                        else:
                            out_fc = out_fc.replace('_utvida', '')
                    except Exception as e:
                        pass

                    # Egenskapsfilter
                    # Replace og ta vare på brukerendringer
                    # Strip endings if not user has changed it
                    ending = ''
                    # if egenskapsfilter_prev != None and egenskapsfilter_prev != "":
                    if egenskapsfilter_prev:
                        out_fc = out_fc.replace(egenskapsfilter_prev, '')
                    egenskapsfilter = objekttabell[n][2]
                    if egenskapsfilter:
                        # Valider egenskapsfilter
                        # Det er støtte for følgende operatorer:
                        # != ulik
                        # >= større enn, eller lik
                        # <= mindre enn, eller lik
                        # = lik
                        # < større enn
                        # > mindre enn
                        if '=!' in egenskapsfilter:
                            egenskapsfilter = egenskapsfilter.replace('=!', '!=')
                        if '=<' in egenskapsfilter:
                            egenskapsfilter = egenskapsfilter.replace('=<', '<=')
                        if '=>' in egenskapsfilter:
                            egenskapsfilter = egenskapsfilter.replace('=>', '>=')
                        if '==' in egenskapsfilter:
                            egenskapsfilter = egenskapsfilter.replace('==', '=')

                        objekttabell[n][2] = egenskapsfilter

                        egenskapsfilter = egenskapsfilter.strip().strip('"').strip("'")
                        egenskapsfilter.strip().strip('"').strip("'")
                        egenskapsfilter = egenskapsfilter.replace('!=', '_ne_')
                        egenskapsfilter = egenskapsfilter.replace('>=', '_ge_')
                        egenskapsfilter = egenskapsfilter.replace('<=', '_le_')
                        egenskapsfilter = egenskapsfilter.replace('=', '_eq_')
                        egenskapsfilter = egenskapsfilter.replace('>', '_gt_')
                        egenskapsfilter = egenskapsfilter.replace('<', '_lt_')
                        egenskapsfilter = re.sub(u'[^0-9a-zA-Z_æøåÆØÅ]', '_', format(egenskapsfilter))
                        egenskapsfilter = u'{}'.format(egenskapsfilter)
                        egenskapsfilter = egenskapsfilter.replace('""', '"')
                        ending = u'_{}'.format(egenskapsfilter)

                        egenskapsfilter_prev_values[n] = ending
                        self['egenskapsfilter_prev'].values = egenskapsfilter_prev_values

                    if ending:
                        out_fc = u'{}{}'.format(out_fc, ending)

                    # Ferdig med å generere featureklassenavn
                    # Oppdater objekttabellen
                    objekttabell[n][1] = out_fc
                    self['objekttabell'].values = objekttabell

                #-----------------------------------------------------
                # Oppdater output features. Har laget 20
                count = 0
                for rad in objekttabell:
                    lyrfil = None
                    # Sjekk om det er lyrfil for denne objekttypen
                    lyrfil_navn = os.path.join(os.path.dirname(__file__),
                                               'lyrfiler', u'{}.lyr'.format(rad[0].replace(r' ', '_').replace(r'/', '_')))
                    if os.path.exists(lyrfil_navn):
                        # Vegreferanse har group layer, vil feile når den skal symboliseres
                        if rad[0] not in ['Vegreferanse']:
                            lyrfil = lyrfil_navn

                    feature = rad[1]
                    # Generer output feature navn
                    out_fc = arcpy.ValidateTableName(feature, out_geodatabase)
                    out_feature = os.path.join(out_geodatabase, out_fc)
                    if count < 20:
                        name = 'out_feature_{}'.format(count)
                        self[name].value = out_feature
                        self[name].symbology = lyrfil
                    count += 1

            #-----------------------------------------------------
            # Bilveger eller Gang-/sykkelveger
            if not self['bilveger'].hasBeenValidated:
                if self['bilveger'].value:
                    self['gang_sykkelveger'].value = False
                else:
                    self['gang_sykkelveger'].value = True

            if not self['gang_sykkelveger'].hasBeenValidated:
                if self['gang_sykkelveger'].value:
                    self['bilveger'].value = False
                else:
                    self['bilveger'].value = True

            if self['vegkategorier'].valueAsText:
                vegkategorier = (self['vegkategorier'].valueAsText).split(';')
                bilveger = [u'Europaveg', u'Riksveg', u'Fylkesveg', u'Kommunal_veg', u'Privat_veg', u'Skogsbilveg']
                nye_vegkategorier = []
                if vegkategorier:
                    for kategori in vegkategorier:
                        if kategori in bilveger:
                            nye_vegkategorier.append(kategori)

                self['vegkategorier'].value = nye_vegkategorier

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
        if self.__debug:
            lokasjon = format(self['lokasjon'].value)
            riksrute = self['rute'].value
            if riksrute:
                riksrute = format(riksrute)
        else:
            lokasjon = self['lokasjon'].valueAsText
            riksrute = self['rute'].valueAsText

        # Initialize logging here, could use logging module instead of AddMessages and all that stuff
        init_logging(verbose=self.__debug)
        logger = logging.getLogger('hent_data_pyt')

        objekttabell = self['objekttabell'].values
        out_geodatabase = self['out_geodatabase'].valueAsText

        extended_extras = bool(self['extended'].value)

        # Definer vegkategorier (kopiert fra Lag Metrert vegnett)
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
                    if vegreferanse != '':
                        vegreferanse = vegreferanse + ','
                    vegreferanse = vegreferanse + kategori[:1] + status

        # Les objekter og hent fra databasen for hvert objekt
        for rad in objekttabell:
            obj_type = format(rad[0])
            out_fc = format(rad[1])
            egenskapsfilter = format(rad[2])
            # Generer output feature navn
            out_fc = arcpy.ValidateTableName(out_fc, out_geodatabase)
            out_features = os.path.join(out_geodatabase, out_fc)

            try:
            # PROCESS = True
            # if PROCESS:
                # Sett lokasjonsfilter
                if lokasjon == self._cfg['lokasjonLut']['alle']:
                    loc_filter = {}
                    omrade = lokasjon
                elif lokasjon == self._cfg['lokasjonLut']['bbox'] or lokasjon == self._cfg['lokasjonLut']['bbox_pro']:
                    if sys.version_info[0] < 3:
                        # ArcMap
                        if self.__debug:
                            mxd = arcpy.mapping.MapDocument(r'C:/Users/eivindn/TestOutputs/svv/nvdb.mxd')
                        else:
                            mxd = arcpy.mapping.MapDocument("CURRENT")
                        df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
                        extent = df.extent
                    else:
                        # ArcGIS Pro
                        aprx = arcpy.mp.ArcGISProject("CURRENT")
                        map = aprx.activeMap
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
                    omrade = get_omrade_by_name(self._cfg, self._meta, 'regioner', region)
                    loc_filter = lokasjon_filter('region', [omrade])
                else:
                    omrade = get_omrade_by_name(self._cfg, self._meta, 'fylker', lokasjon)
                    loc_filter = lokasjon_filter('fylke', [omrade])
                # Legg til begrensing på riksvegrute
                if riksrute:
                    rute = riksrute.split(': ')[0]
                    if "/_no_test" in rute:
                        splitt = rute.split('/')
                        for n in range(len(splitt)):
                            splitt[n] = splitt[n].strip()
                        prefiks = splitt[0].split(' ')[0]
                        # rute = '"{}", "{} {}"'.format(splitt[0], prefiks, splitt[1])
                        # logger.info('rute: ' + rute)
                        loc_filter['riksvegrute'] = ['"{}"'.format(splitt[0])]
                        loc_filter['riksvegrute'].append('"{} {}"'.format(prefiks, splitt[1]))
                    else:
                        loc_filter['riksvegrute'] = ['"{}"'.format(rute)]

                # Sett objekttypefilter
                _, dbid = self._meta.by_name(obj_type)
                objekt_filter = [{'id': dbid}]

                # Sett egenskapsfilter
                if egenskapsfilter and egenskapsfilter != "None" and egenskapsfilter != "":
                    egenskapsfilter.strip().strip('"')
                    egenskaps_filter = u'{}'.format(egenskapsfilter)
                else:
                    egenskaps_filter = None

                logger.info(u'\nObjekttype: {}'.format(obj_type))
                if egenskaps_filter:
                    logger.info(u'Egenskapsfilter: {}'.format(egenskaps_filter))

                # Sett segmentering til False
                self._cfg['sok_params']['segmentering'] = False

                # Her hentes data
                result = populate_fc(self._cfg, self._meta, out_features,
                                     objekt_filter,
                                     egenskapsfilter=egenskaps_filter,
                                     vegreferanse=vegreferanse,
                                     lokasjon=loc_filter,
                                     max_pr_request=10000, overwrite=True,
                                     extended_extras=extended_extras, verbose=True)

                if result[2]:
                    other_fcs = result[2]
                    values = self['other_featureclass'].values
                    if not values:
                        values = []
                    for other_fc in other_fcs:
                        values.append(other_fc)
                    self['other_featureclass'].values = values

                # # Kode hentet fra lag_metrert_vegnett:
                # # Kopier inn gruppe lyr-fil med Vegkategorier:
                # if obj_type == 'Vegreferanse':
                #     out_gdb = out_geodatabase
                #     if sys.version_info[0] < 3:
                #         # ArcMap
                #         mxd = arcpy.mapping.MapDocument("CURRENT")
                #         dataframe = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
                #         lyrfil_navn = os.path.join(os.path.dirname(__file__), 'lyrfiler', u'Vegreferanse.lyr')
                #         group_layer_name_old = u'{}_Vegkategorier'.format(out_fc)
                #         group_layer_name = format(out_fc)
                #         if os.path.exists(lyrfil_navn):
                #             grouplayer = arcpy.mapping.Layer(lyrfil_navn)
                #             grouplayer.name = group_layer_name
                #             # Sjekk om layer allerede finnes i kartdokumentetet
                #             for lyr in arcpy.mapping.ListLayers(mxd, "*", dataframe):
                #                 if lyr.name in [group_layer_name, group_layer_name_old]:
                #                     arcpy.mapping.RemoveLayer(dataframe, lyr)
                #             arcpy.mapping.AddLayer(dataframe, grouplayer)

                #             # Erstatt datakilde (replaceDataSource) for layer eller fjern layer dersom ikke i bruk
                #             first_layer = True
                #             for lyr in arcpy.mapping.ListLayers(mxd):
                #                 group_path = u'{}\\'.format(group_layer_name)
                #                 lyr_path = u'{}'.format(lyr)
                #                 if lyr_path.startswith(group_path):
                #                     layer = lyr.name.strip().replace(' ', '_')
                #                     if layer in vegkategorier:
                #                         lyr.replaceDataSource(out_gdb, "FILEGDB_WORKSPACE", u'{}'.format(out_fc))
                #                         if first_layer:
                #                             lyr.visible = True
                #                             first_layer = False
                #                         else:
                #                             lyr.visible = False
                #                     else:
                #                         arcpy.mapping.RemoveLayer(dataframe, lyr)

                #         del mxd, dataframe

                #     else:
                #         # ArcGIS Pro
                #         aprx = arcpy.mp.ArcGISProject("CURRENT")
                #         map = aprx.listMaps()[0]
                #         lyrfil_navn = os.path.join(os.path.dirname(__file__), 'lyrfiler', u'Vegreferanse.lyr')
                #         group_layer_name_old = u'{}_Vegkategorier'.format(out_fc)
                #         group_layer_name = format(out_fc)
                #         if os.path.exists(lyrfil_navn):
                #             grouplayer = arcpy.mp.LayerFile(lyrfil_navn)
                #             # Sjekk om nytt layer allerede finnes i kartdokumentetet
                #             for lyr in map.listLayers():
                #                 if lyr.name in [group_layer_name, group_layer_name_old]:
                #                     map.removeLayer(lyr)
                #             # Add layer and set name
                #             map.addLayer(grouplayer)
                #             for lyr in map.listLayers():
                #                 if lyr.isGroupLayer and lyr.name == 'Vegreferanse':
                #                     lyr.name = group_layer_name

                #             # Erstatt datakilde (replaceDataSource) for layer eller fjern layer dersom ikke i bruk
                #             group_path = u'{}\\'.format(group_layer_name)
                #             first_layer = True
                #             for lyr in map.listLayers():
                #                 lyr_path = u'{}'.format(lyr)
                #                 if lyr.isGroupLayer:
                #                     if lyr.name in [group_layer_name]:
                #                         lyr.visible = True
                #                 elif lyr_path.startswith(group_path) or lyr_path.startswith('Vegreferanse\\'):
                #                     layer = lyr_path.strip().replace(' ', '_').split('\\')[1].strip()
                #                     if layer in vegkategorier:
                #                         # Repair data source
                #                         new_connection_info = {}
                #                         current_connection_info = lyr.connectionProperties
                #                         for key in current_connection_info:
                #                             new_connection_info[key] = current_connection_info[key]
                #                             if key == 'dataset':
                #                                 new_connection_info[key] = out_fc
                #                             if key == 'connection_info':
                #                                 new_connection_info[key]['database'] = out_gdb
                #                         lyr.updateConnectionProperties(current_connection_info, new_connection_info)
                #                         if first_layer:
                #                             lyr.visible = True
                #                             first_layer = False
                #                         else:
                #                             lyr.visible = False
                #                     else:
                #                         if layer not in ['Vegreferanse']:
                #                             map.removeLayer(lyr)
                #         del map, aprx

            except HTTPError as e:
                if e.code == 404:
                    logger.error('ERROR: Objekttype eksisterer ikke i NVDB' + '\n' + e.read())
                else:
                    t = sys.exc_info()[2]
                    tbinfo = tb.format_tb(t)[0]
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
    import os
    path = os.path.abspath(__file__)
    cwd = os.path.dirname(path)
    print(cwd)

    HentData.set_config_meta(cwd)
    getter = HentData(debug=True)
    getter = HentData(debug=False)
    objekttabell = []
    #objekttabell.append(['Fartsgrense', 'Fartsgrense_test', None, None])
    # objekttabell.append(['Vegreferanse', 'Vegreferanse_test', None, None])
    # objekttabell.append(['Bomstasjon', 'Bomstasjon_test', None, None])
    # objekttabell.append(['Bomstasjon', 'Bomstasjon_test', '1820=20 OR 1820>=33', None]) # egenskapsfilter: #'1820=20 OR 1820=50' #'1820=20 OR 1820=50' # '1820>=33'
    # objekttabell.append(['Bru', 'Bru_test', None, None])
    # objekttabell.append(['Belysningspunkt', 'Belysningspunkt_kontraktsomrade_eq_0203_FOLLO_2015_2020','kontraktsomrade="0203 FOLLO 2015-2020"', None])
    objekttype = 'Trær'
    # objekttype = 'Trafikkmengde'
    # objekttype = 'Branndetektor'
    # objekttype = 'Vegreferanse'
    # objekttype = 'Grasdekker'
    # objekttype = 'Bomstasjon'
    # objekttabell.append([objekttype, objekttype + '_øst_ny', None, None])
    # objekttabell.append([objekttype, objekttype + '_test', None, None])
    objekttype = 'Værrelatert strekning'
    # objekttabell.append([objekttype, 'Værrelatert_strekning_utvida2', None, None])
    objekttype = 'Vegreferanse'
    objekttabell.append([objekttype, objekttype + '_test', None, None])
    getter['objekttabell'].value = objekttabell
    # getter['lokasjon'].value = u'Region ØST' #  getter._cfg['lokasjonLut']['alle']
    getter['lokasjon'].value = u'Hele Landet' #u'Hordaland' #  getter._cfg['lokasjonLut']['alle']
    getter['lokasjon'].value = u'Hordaland' #  getter._cfg['lokasjonLut']['alle']
    #getter['lokasjon'].value = getter._cfg['lokasjonLut']['bbox']
    # getter['out_geodatabase'].value = r'C:\\Users\\tores\\Documents\\ArcGIS\\Default.gdb'
    getter['out_geodatabase'].value = r'D:\\ArcGIS_Pro_Projects\\MyProject\\MyProject.gdb'
    getter['extended'].value = False
    # getter['extended'].value = True
    getter['rute'].value = None
    # getter['rute'].value = r"RUTE4A 2014-2023 / 2018-2029: E39 Stavanger - Bergen - Ålesund" #None # u'RUTE1 2014-2023: E6 Riksgrensen/Svinesund - Oslo'
    # getter['rute'].value = "RUTE6A 2014-2023: E6 Oslo - Trondheim" #None # u'RUTE1 2014-2023: E6 Riksgrensen/Svinesund - Oslo'

    # Execute tool
    getter.execute(getter.parameters, messages=None)
