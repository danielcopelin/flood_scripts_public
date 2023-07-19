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
import math

from qgis.PyQt.QtCore import QCoreApplication,QFileInfo
from qgis.core import (QgsProcessing,
                       QgsProcessingException,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterRasterLayer,
                       QgsProcessingParameterNumber,
                       QgsProcessingParameterRasterDestination,
                       QgsCoordinateReferenceSystem,
                       QgsProcessingOutputLayerDefinition,
                       QgsRasterLayer,
                       QgsProcessingParameterFolderDestination
                       )
from qgis import processing

class FloodFilter(QgsProcessingAlgorithm):
    DEPTH_C1 = 'DEPTH_C1'
    DV_C1 = 'DV_C1'
    DEPTH_C2 = 'DEPTH_C2'
    DV_C2 = 'DV_C2'
    AREA = 'AREA'
    DEPTH = 'DEPTH'
    VELOCITY = 'VELOCITY'
    DV = 'DV'
    FILTER = 'FILTER'
    HAZARD = 'HAZARD'
    LEVEL = 'LEVEL'
    OUTPUT_FOLDER = 'OUTPUT_FOLDER'

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return FloodFilter()

    def name(self):
        return 'floodfilter'

    def displayName(self):
        return self.tr('Flood Filtering')

    def group(self):
        return self.tr('Flood scripts')

    def groupId(self):
        return 'Flood scripts'

    def shortHelpString(self):
        return self.tr("Filters raw direct rainfall grids based on specified criteria.")

    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterNumber(
                self.DEPTH_C1,
                self.tr('Criteria 1 - Depth cutoff'),
                QgsProcessingParameterNumber.Double,
                0.01,
            )
        )
        self.addParameter(
            QgsProcessingParameterNumber(
                self.DV_C1,
                self.tr('Criteria 1 - DV cutoff'),
                QgsProcessingParameterNumber.Double,
                0.125,
            )
        )
        self.addParameter(
            QgsProcessingParameterNumber(
                self.DEPTH_C2,
                self.tr('Criteria 2 - Depth cutoff'),
                QgsProcessingParameterNumber.Double,
                0.3,
            )
        )
        self.addParameter(
            QgsProcessingParameterNumber(
                self.DV_C2,
                self.tr('Criteria 2 - DV cutoff'),
                QgsProcessingParameterNumber.Double,
                0.02,
            )
        )
        self.addParameter(
            QgsProcessingParameterNumber(
                self.AREA,
                self.tr('Pond/island area threshold'),
                QgsProcessingParameterNumber.Double,
                500,
            )
        )
        self.addParameter(
            QgsProcessingParameterRasterLayer(
                self.DEPTH,
                self.tr('Raw depth grid'),
                [QgsProcessing.TypeRaster]
            )
        )
        self.addParameter(
            QgsProcessingParameterRasterLayer(
                self.VELOCITY,
                self.tr('Raw velocity grid'),
                [QgsProcessing.TypeRaster]
            )
        )
        self.addParameter(
            QgsProcessingParameterRasterLayer(
                self.DV,
                self.tr('Raw DV grid'),
                [QgsProcessing.TypeRaster]
            )
        )
        self.addParameter(
            QgsProcessingParameterRasterLayer(
                self.HAZARD,
                self.tr('Raw Hazard grid'),
                [QgsProcessing.TypeRaster]
            )
        )
        self.addParameter(
            QgsProcessingParameterRasterLayer(
                self.LEVEL,
                self.tr('Raw Level grid'),
                [QgsProcessing.TypeRaster]
            )
        )
        self.addParameter(
            QgsProcessingParameterRasterDestination(
                self.FILTER,
                self.tr('Filter Output Layer')
            )
        )
        self.addParameter(
            QgsProcessingParameterFolderDestination(
                self.OUTPUT_FOLDER,
                self.tr('Folder for Filtered Outputs')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):

        depth_raster = QgsRasterLayer(parameters['DEPTH'])#, os.path.basename(parameters['DEPTH'])[:-4])
        velocity_raster = QgsRasterLayer(parameters['VELOCITY'])#, os.path.basename(parameters['VELOCITY'])[:-4])
        dv_raster = QgsRasterLayer(parameters['DV'])#, os.path.basename(parameters['DV'])[:-4])
        
        filtered = processing.run(
            "gdal:rastercalculator",
            {
                'INPUT_A': depth_raster,
                'BAND_A': '1',
                'INPUT_B': dv_raster,
                'BAND_B': '1',
                'FORMULA': f"logical_or(logical_and((A < {parameters['DEPTH_C1']}),(B < {parameters['DV_C1']})),logical_and((A < {parameters['DEPTH_C2']}),(B < {parameters['DV_C2']})))",
                'RTYPE': '1',
                'OUTPUT': 'TEMPORARY_OUTPUT'
            },
            is_child_algorithm=True,
            context=context,
            feedback=feedback
        )

        sieve = processing.run(
            "gdal:sieve", 
            {
                'INPUT': filtered['OUTPUT'],
                'THRESHOLD': math.floor(parameters['AREA'] / depth_raster.rasterUnitsPerPixelX()**2),
                'EIGHT_CONNECTEDNESS': False,
                'NO_MASK': False,
                'MASK_LAYER': None,
                'EXTRA': '',
                'OUTPUT': 'TEMPORARY_OUTPUT'
            },
            is_child_algorithm=True,
            context=context,
            feedback=feedback
        )
        
        mask = processing.run(
            "gdal:rastercalculator",
            {
                'INPUT_A': sieve['OUTPUT'],
                'BAND_A': '1',
                'FORMULA': 'A==0',
                'RTYPE': '1',
                'NO_DATA': 0,
                'OUTPUT': 'TEMPORARY_OUTPUT'
            },
            is_child_algorithm=True,
            context=context,
            feedback=feedback
        )

        if feedback.isCanceled():
            return {}
            
        filter_final = processing.run(
            "native:rasterbooleanand", 
            {
                'INPUT': [mask['OUTPUT']],
                'REF_LAYER': mask['OUTPUT'],
                'NODATA_AS_FALSE': False,
                'NO_DATA': 0,
                'DATA_TYPE': 5,
                'OUTPUT': parameters['FILTER']
            },
            is_child_algorithm=True,
            context=context,
            feedback=feedback
        )

        if feedback.isCanceled():
            return {}

        orig_rasters = {
            parameters['DEPTH']: '5',
            parameters['VELOCITY']: '5',
            parameters['DV']: '5',
            parameters['LEVEL']: '5',
            parameters['HAZARD']: '1',
        }

        for orig_raster, rtype in orig_rasters.items():
            filtered_result = processing.run(
                    "gdal:rastercalculator",
                    {
                        'INPUT_A': filter_final['OUTPUT'],
                        'BAND_A': '1',
                        'INPUT_B': orig_raster,
                        'BAND_B': '1',
                        'FORMULA': 'A*B',
                        'RTYPE': rtype,
                        'OUTPUT': os.path.join(parameters['OUTPUT_FOLDER'], os.path.basename(orig_raster[:-4])+'_filtered'+orig_raster[-4:])
                    },
                    is_child_algorithm=True,
                    context=context,
                    feedback=feedback
                )

        return {
            'FILTER': filter_final['OUTPUT'],
        }
