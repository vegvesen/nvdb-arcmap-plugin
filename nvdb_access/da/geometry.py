import logging
import copy
import re


logger = logging.getLogger(__name__)

class CorruptGeometryError(Exception):
    pass

class WKT(object):
    logger = logging.getLogger('NVDB.data_da.WKT')
    empty_set = 'EMPTY'
    
    geometry_types = ['POINT', 'LINESTRING', 'POLYGON']
    
    def __init__(self, wkt):
        self.wkt = wkt

    @property
    def geometry_tag(self):
        gt, _ = self.wkt.split('(', 1)
        return gt.strip()

    @property
    def raw_points(self):
        _, rp = self.wkt.split('(', 1)
        return '({}'.format(rp)

    def is_empty(self):
        if self.wkt.endswith(self.empty_set):
            return True
        return False

    def has_known_geometry_type(self):
        geom_type_tag = self.get_tag_parts()[0]
        for gt in self.geometry_types:
            if gt in geom_type_tag:
                return True
        return False

    def get_geometry_type(self, failIfUnknown=True):
        geom_type_tag = self.get_tag_parts()[0]

        geom_type = None
        for gt in self.geometry_types:
            if gt in geom_type_tag:
                geom_type = gt

        if failIfUnknown and not geom_type:
            raise Exception('Could not decide which geometry type {} has.'.format(self.wkt))
        elif not geom_type:
            logger.warn('Could not decide which geometry type {} has.'.format(self.wkt))

        return geom_type

    def get_dimension(self):
        rest, sep, part = self.raw_points.rpartition(',')
        ncoords = set()
        while part:
            nums = re.findall(r'\d+(?:\.\d*)?', part)
            ncoords.add(len(nums))

            rest, sep, part = rest.rpartition(',')

        if len(ncoords) == 0:
            # No coordinates found
            return None
        elif len(ncoords) > 1:
            raise CorruptGeometryError('Geometry had points with different number of coordinates')

        return list(ncoords)[0]

    def get_tag_parts(self):
        return self.geometry_tag.split()

    def get_tag_dimension(self):
        tag_parts = self.get_tag_parts()
        if len(tag_parts) == 1:
            return 2
        elif len(tag_parts) == 2:
            return 2 + len(tag_parts[1].strip())
        else:
            raise CorruptGeometryError('Geometry tag {} does not parse correctly'.format(self.geometry_tag))

    def get_next_extra(self):
        if self.get_tag_dimension() == 2:
            return ' Z'
        elif self.get_tag_dimension() == 3:
            tag_parts = self.get_tag_parts()
            extra = tag_parts[1]
            if extra == 'Z':
                return 'M'
            else:
                return 'Z'
        else:
            raise Exception('List of extra tags is exhausted')

    def create_repaired_tag(self):
        success = False
        gt = copy.copy(self.geometry_tag)

        if self.get_dimension() > 4:
            logger.info('More than 4 dimensions observed in geometry, could not repair tag')
        elif self.get_dimension() < 2:
            logger.info('Less than 2 dimensions observed in geometry, could not repair tag')
        elif self.get_dimension() > self.get_tag_dimension():
            gt = '{}{}'.format(gt, self.get_next_extra())
            success = True
        else:
            tag_parts = self.get_tag_parts()
            if self.get_dimension() == 2:
                gt = tag_parts[0]
            else:
                gt = '{} {}'.format(tag_parts[0], tag_parts[1][0])

            success = True

        return success, gt

    def check_dimensions(self):
        if self.get_dimension() == self.get_tag_dimension():
            return True
        return False

    def check_and_repair_dimensions(self):
        # First check if everything is ok
        if self.check_dimensions():
            return True
        else:
            # This flag is used as preliminary success flag, might still be errors, so keep old wkt for later use
            old_wkt = self.wkt

            success, gt = self.create_repaired_tag()

            if success:
                self.wkt = self.wkt.replace(self.geometry_tag, gt)
                if not self.check_dimensions():
                    self.wkt = old_wkt
                    logger.debug('Could not repair geometry')
                else:
                    #logger.debug('Geometry repaired')
                    pass


def repair_inconsistent_geometry(attributes, force_geometry_type=None):
    ''' Check and repair inconsistent geometry definitions in place
    
    :param attributes: dict
        Dictionary of feature attributes. Only requires 'SHAPE@WKT'
    :param force_dimension: int (default None)
        Check if geometry is of this type, otherwise return empty geometry
        
    

    Examples
    --------

    >>> attributes = {'SHAPE@WKT': 'LINESTRING (-32206.59 6737377.3 31.6, -32207 6737378.9 31.8, -32207.41 6737380.9 32, -32207.7 6737381.9 32.1)'}
    >>> repair_inconsistent_geometry(attributes)
    >>> attributes['SHAPE@WKT']
    'LINESTRING Z (-32206.59 6737377.3 31.6, -32207 6737378.9 31.8, -32207.41 6737380.9 32, -32207.7 6737381.9 32.1)'

    >>> attributes = {'SHAPE@WKT': 'POINT (-35727.48824551452 6577178.654272747 11.1)'}
    >>> repair_inconsistent_geometry(attributes)
    >>> attributes['SHAPE@WKT']
    'POINT Z (-35727.48824551452 6577178.654272747 11.1)'

    >>> attributes = {'SHAPE@WKT': 'LINESTRING (-32206.59 6737377.3, -32207 6737378.9, -32207.41 6737380.9, -32207.7 6737381.9)'}
    >>> repair_inconsistent_geometry(attributes)
    >>> attributes['SHAPE@WKT']
    'LINESTRING (-32206.59 6737377.3, -32207 6737378.9, -32207.41 6737380.9, -32207.7 6737381.9)'

    >>> attributes = {'SHAPE@WKT': 'POINT (-35727.48824551452 6577178.654272747)'}
    >>> repair_inconsistent_geometry(attributes)
    >>> attributes['SHAPE@WKT']
    'POINT (-35727.48824551452 6577178.654272747)'

    >>> attributes = {'SHAPE@WKT': 'LINESTRING Z (-32206.59 6737377.3, -32207 6737378.9, -32207.41 6737380.9, -32207.7 6737381.9)'}
    >>> repair_inconsistent_geometry(attributes)
    >>> attributes['SHAPE@WKT']
    'LINESTRING (-32206.59 6737377.3, -32207 6737378.9, -32207.41 6737380.9, -32207.7 6737381.9)'

    >>> attributes = {'SHAPE@WKT': 'POINT Z (-35727.48824551452 6577178.654272747)'}
    >>> repair_inconsistent_geometry(attributes)
    >>> attributes['SHAPE@WKT']
    'POINT (-35727.48824551452 6577178.654272747)'

    '''
    wkt = WKT(attributes['SHAPE@WKT'])
    if wkt.is_empty():
        return

    # evaluate consistency of geometry and repair if necessary
    if not wkt.check_dimensions():
        wkt.check_and_repair_dimensions()
        attributes['SHAPE@WKT'] = wkt.wkt


def check_empty_geometry(cfg, attributes, geom_type):
    ex = cfg['extras_default']

    if not geom_type == 'none':
    # TODO: Add logging point here
        try:
            wkt = attributes[ex['geometri']]
        except KeyError:
            # Dette elementet burde hatt geometri, men det var ingen
            wkt = '%s %s' % (cfg['wktTyperLut'][geom_type],
                             cfg['wktEmpty'])
            # TODO: Skrive logging her om manglande geometri?
            attributes[ex['geometri']] = wkt
        finally:
            return True
    else:
        return False