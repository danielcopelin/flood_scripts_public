# -*- coding: utf-8 -*-

"""
***************************************************************************
*                                                                         *
*   This program is free software; you can redistribute it and/or modify  *
*   it under the terms of the GNU General Public License as published by  *
*   the Free Software Foundation; either version 2 of the License, or     *
*   (at your option) any later version.                                   *
*                                                                         *
***************************************************************************
"""

import os

from qgis.PyQt.QtCore import QCoreApplication,QFileInfo
from qgis.core import (QgsProcessing,
                       QgsProcessingException,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterRasterLayer,
                       QgsProcessingOutputVectorLayer,
                       QgsCoordinateReferenceSystem,
                       QgsProcessingParameterFeatureSink,
                       QgsRasterLayer,
                       QgsProcessingParameterFolderDestination,
                       QgsProcessingParameterMultipleLayers,
                       QgsProcessingParameterBoolean,
                       QgsProcessingParameterField,
                       QgsProcessingParameterNumber,
                       QgsMessageLog,
                       )
from qgis import processing

class FCRCRoadImmunity(QgsProcessingAlgorithm):
    INPUT_RASTERS = 'INPUT_RASTERS'
    ROADS = 'ROADS'
    ROADS_ID_FIELD = 'ROADS_ID_FIELD'
    OUTPUT_ROADS = 'OUTPUT_ROADS'
    NOISE_REDUCTION = 'NOISE_REDUCTION'
    HAZARD_THRESHOLD = 'HAZARD_THRESHOLD'

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return FCRCRoadImmunity()

    def name(self):
        return 'fcrcroadimmunity'

    def displayName(self):
        return self.tr('FCRC Road Immunity')

    def group(self):
        return self.tr('Flood scripts')

    def groupId(self):
        return 'Flood scripts'

    def shortHelpString(self):
        return self.tr("Calculates road flood immunity.")

    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterMultipleLayers(
                self.INPUT_RASTERS,
                self.tr('Input Hazard Rasters'),
                QgsProcessing.TypeRaster
            )
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.NOISE_REDUCTION,
                self.tr('Noise Reduction'),
            )
        )
        self.addParameter(
            QgsProcessingParameterNumber(
                self.HAZARD_THRESHOLD,
                self.tr('Road Closure Hazard Threshold'),
                QgsProcessingParameterNumber.Integer,
                defaultValue=2,
                minValue=1,
                maxValue=6,
            )
        )
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.ROADS,
                self.tr('Road Centrelines')
            )
        )
        self.addParameter(
            QgsProcessingParameterField(
                self.ROADS_ID_FIELD,
                self.tr('Road ID Field'),
                "",
                'ROADS'
            )
        )
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT_ROADS,
                self.tr('Output Road Flood Immunity Layer'),
                # [QgsProcessing.TypeVectorLine]
            )
        )

    def processAlgorithm(self, parameters, context, feedback):

        buffer = processing.run(
            "native:buffer", 
            {
                'INPUT':parameters['ROADS'],
                'DISTANCE':QgsRasterLayer(parameters['INPUT_RASTERS'][0]).rasterUnitsPerPixelX()/2.0,
                'SEGMENTS':5,
                'END_CAP_STYLE':0,
                'JOIN_STYLE':0,
                'MITER_LIMIT':2,
                'DISSOLVE':False,
                'OUTPUT':'TEMPORARY_OUTPUT'
            },
            is_child_algorithm=True,
            context=context,
            feedback=feedback
        )

        fields_to_copy = []
        for hazard_raster in parameters['INPUT_RASTERS']:
            noise_reduced = {'OUTPUT': None}
            if parameters['NOISE_REDUCTION']:
                noise_reduced = processing.run(
                    "gdal:warpreproject", 
                    {
                        'INPUT':hazard_raster,
                        'SOURCE_CRS':QgsCoordinateReferenceSystem('EPSG:28356'),
                        'TARGET_CRS':QgsCoordinateReferenceSystem('EPSG:28356'),
                        'RESAMPLING':6,
                        'NODATA':None,
                        'TARGET_RESOLUTION':None,
                        'OPTIONS':'',
                        'DATA_TYPE':0,
                        'TARGET_EXTENT':None,
                        'TARGET_EXTENT_CRS':None,
                        'MULTITHREADING':False,
                        'EXTRA':'-tr {0} {0}'.format(QgsRasterLayer(hazard_raster).rasterUnitsPerPixelX()*2),
                        'OUTPUT':'TEMPORARY_OUTPUT'
                    },
                    is_child_algorithm=True,
                    context=context,
                    feedback=feedback
                )
            
            field = os.path.splitext(os.path.basename(hazard_raster))[0]+"_max"
            fields_to_copy.append(field)
            processing.run(
                "native:zonalstatistics", 
                {
                    'INPUT_RASTER':[hazard_raster, noise_reduced['OUTPUT']][parameters['NOISE_REDUCTION']],
                    'RASTER_BAND':1,
                    'INPUT_VECTOR':buffer['OUTPUT'],
                    'COLUMN_PREFIX':field[:-3],
                    'STATISTICS':[6]
                },
                is_child_algorithm=True,
                context=context,
                feedback=feedback
            )

        joined = processing.run(
            "native:joinattributestable", 
            {
                'INPUT':parameters['ROADS'],
                'FIELD':parameters['ROADS_ID_FIELD'],
                'INPUT_2':buffer['OUTPUT'],
                'FIELD_2':parameters['ROADS_ID_FIELD'],
                'FIELDS_TO_COPY':fields_to_copy,
                'METHOD':1,
                'DISCARD_NONMATCHING':False,
                'PREFIX':'',
                'OUTPUT':'TEMPORARY_OUTPUT',
            },
            is_child_algorithm=True,
            context=context,
            feedback=feedback
        )

        formula = " array_get(array('None',{0}), array_find(array_foreach(array({1}),@element>={2}),1)+1) ".format(
                    ','.join(["'{0}'".format(f) for f in fields_to_copy]), 
                    ','.join(['\"{0}\"'.format(f) for f in fields_to_copy]),
                    parameters['HAZARD_THRESHOLD'],
        )
        QgsMessageLog.logMessage(formula, 'Debug')

        immunity = processing.run(
            "qgis:fieldcalculator", 
            {
                'INPUT':joined['OUTPUT'],
                'FIELD_NAME':'RoadFirstClosedEvent',
                'FIELD_TYPE':2,
                'FIELD_LENGTH':10,
                'FIELD_PRECISION':3,
                'NEW_FIELD':True,
                'FORMULA': formula,
                'OUTPUT':parameters['OUTPUT_ROADS']
            },
            is_child_algorithm=True,
            context=context,
            feedback=feedback
        )

        return {
            'OUTPUT_ROADS': immunity['OUTPUT'],
        }
