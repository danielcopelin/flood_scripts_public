import os
import glob
import pandas as pd
import datetime

from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (
    QgsRasterLayer,
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterCrs,
    QgsProcessingParameterEnum,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterField,
    QgsProcessingParameterFile,
    QgsProcessingParameterFolderDestination,
    QgsProcessingParameterVectorDestination,
    QgsProcessingParameterDefinition,
    QgsCoordinateReferenceSystem,
)
from qgis import processing


AEPS = {
    'QGIS': [
        '1EY',             
        '50pc',            
        '2yARI',           
        '20pc',            
        '5yARI',           
        '10pc',            
        '5pc',             
        '2pc',             
        '1pc',             
        '1in200yr',        
        '1in500yr',        
        '1in1000yr',       
        '1in2000yr', 
    ],
    'LIMB': [
        '1EY',       
        '50pc',      
        '2yARI',     
        '20pc',      
        '5yARI',     
        '10pc',      
        '5pc',       
        '2pc',       
        '1pc',       
        '1in200yr',  
        '1in500yr',  
        '1in1000yr', 
        '1in2000yr', 
    ],
    'QRA SEQ': [
        '63pct',  
        '50pct',  
        '2y',     
        '20pct',  
        '5y',     
        '10pct',  
        '5pct',   
        '2pct',   
        '1pct',   
        '1in200', 
        '1in500', 
        '1in1000',
        '1in2000',
    ],
    'BOM': [
        '1EY',
        '50%',
        '0.5EY',
        '20%',
        '0.2EY',
        '10%',
        '5%',
        '2%',
        '1%',
        '1 in 200',
        '1 in 500',
        '1 in 1000',
        '1 in 2000',
    ],
    'URBS': [
        'ARI1',
        'ARI2',
        'ARI2',
        'ARI5',
        'ARI5',
        'ARI10',
        'ARI20',
        'ARI50',
        'ARI100',
        'ARI200',
        'ARI500',
        'ARI1e3',
        'ARI2e3',
    ],
}


DURATIONS = {
    'QGIS': [
        '5 minutes',              
        '10 minutes',             
        '15 minutes',             
        '20 minutes',             
        '25 minutes',             
        '30 minutes',             
        '45 minutes',             
        '60 minutes (1 hour)',    
        '90 minutes (1.5 hours)', 
        '120 minutes (2 hours)',  
        '180 minutes (3 hours)',  
        '270 minutes (4.5 hours)',
        '360 minutes (6 hours)',  
        '540 minutes (9 hours)',  
        '720 minutes (12 hours)', 
        '1080 minutes (18 hours)',
        '1440 minutes (24 hours)',
        '30 hours (1.25 days)',   
        '36 hours (1.5 days)',    
        '48 hours (2 days)',      
        '72 hours (3 days)',      
        '96 hours (4 days)',      
        '120 hours (5 days)',     
        '144 hours (6 days)',     
        '168 hours (7 days)',     
    ],
    'LIMB': [
        '00005',
        '00010',
        '00015',
        '00020',
        '00025',
        '00030',
        '00045',
        '00060',
        '00090',
        '00120',
        '00180',
        '00270',
        '00360',
        '00540',
        '00720',
        '01080',
        '01440',
        '01800',
        '02160',
        '02880',
        '04320',
        '05760',
        '07200',
        '08640',
        '10080',
    ],
    'QRA SEQ': [
        '5min',    
        '10min',   
        '15min',   
        '20min',   
        '25min',   
        '30min',   
        '45min',   
        '1hr',     
        '90min',   
        '2hr',     
        '3hr',     
        '270min',  
        '6hr',     
        '9hr',     
        '12hr',    
        '18hr',    
        '24hr',    
        '30hr',    
        '36hr',    
        '48hr',    
        '72hr',    
        '96hr',    
        '120hr',   
        '144hr',   
        '168hr',   
    ],
    'BOM': [
        '5 min',
        '10 min',
        '15 min',
        '20 min',
        '25 min',
        '30 min',
        '45 min',
        '1 hour',
        '1.5 hour',
        '2 hour',
        '3 hour',
        '4.5 hour',
        '6 hour',
        '9 hour',
        '12 hour',
        '18 hour',
        '24 hour',
        '30 hour',
        '36 hour',
        '48 hour',
        '72 hour',
        '96 hour',
        '120 hour',
        '144 hour',
        '168 hour',
    ],
    'URBS': [
        '5m',
        '10m',
        '15m',
        '20m',
        '25m',
        '30m',
        '45m',
        '1h',
        '90m',
        '2h',
        '3h',
        '270m',
        '6h',
        '9h',
        '12h',
        '18h',
        '24h',
        '30h',
        '36h',
        '48h',
        '72h',
        '96h',
        '120h',
        '144h',
        '168h',
    ],
}


def createBomCSVs(input_layer, results_dict, grid_set, depth_or_intensity, id_field, output_folder, feedback, context):

    aeps_naming = dict(zip(AEPS['QRA SEQ'], AEPS['BOM']))
    durations_naming = dict(zip(DURATIONS['QRA SEQ'], DURATIONS['BOM']))
    numerical_durations = dict(zip(DURATIONS['QRA SEQ'], DURATIONS['LIMB']))
    
    grid_sets = {
        '_IFD_data_HARC2024\HARC2024_IFDgrids': 'QRA_SEQ',
        '_IFD_data_Max_BoM_HARC2024\MaxBureau2016andHARC2024_IFDgrids_TIFF': 'QRA_SEQ_BOM_Envelope'
    }
    grid_set = grid_sets[grid_set]

    date = datetime.datetime.today().strftime('%d %B %Y')

    for feature in input_layer.getFeatures():
        feature_id = feature[id_field]
        if input_layer.crs().isGeographic():
            header_template = '\n'.join(
                [ 'Brisbane City Council',
                '',
                f'IFD Design Rainfall {depth_or_intensity} (mm{"/hr" if depth_or_intensity == "Intensity" else ""}) - {0}',
                'Issued:,{1}',
                'Location Label:,{2}',
                'Requested coordinate:,Latitude,{3:.4f},Longitude,{4:.4f}',
                'Nearest grid cell:,Latitude,{5},Longitude,{6}',
                '',
                ',,Annual Exceedance Probability (AEP)\n'
                ]
            )
            if feature.geometry().type() == 0: # points
                lat, lon = feature.geometry().get().y(), feature.geometry().get().x()
                near_lat, near_lon = 0, 0
            elif feature.geometry().type() == 2: # polygons
                lat, lon = feature.geometry().centroid().get().y(), feature.geometry().centroid().get().x()
                near_lat, near_lon = 0, 0
            header = header_template.format(grid_set, date, feature_id, lat, lon, near_lat, near_lon)

        else:
            header_template = '\n'.join(
                [ 'Brisbane City Council',
                '',
                f'IFD Design Rainfall {depth_or_intensity} (mm{"/hr" if depth_or_intensity == "Intensity" else ""}) - {0}',
                'Issued:,{1}',
                'Location Label:,{2}',
                'Requested coordinate:,Easting,{3:.1f},Northing,{4:.1f},Zone,{5}',
                'Nearest grid cell:,Latitude,{6},Longitude,{7}',
                '',
                ',,Annual Exceedance Probability (AEP)\n'
                ]
            )
            if feature.geometry().type() == 0: # points
                easting, northing = feature.geometry().get().x(), feature.geometry().get().y()
                near_lat, near_lon = 0, 0
            elif feature.geometry().type() == 2: # polygons
                easting, northing = feature.geometry().centroid().get().x(), feature.geometry().centroid().get().y()
                near_lat, near_lon = 0, 0
            try:
                zone = input_layer.crs().toProj().split()[1].split("=")[1]
            except:
                zone = 0
            header = header_template.format(grid_set, date, feature_id, easting, northing, zone, near_lat, near_lon)  

        feature_results_dict = results_dict[feature_id]
        aeps = feature_results_dict.keys()
        durations = feature_results_dict[list(aeps)[0]].keys()

        csv_file = os.path.join(output_folder, f'bcc_ifd_{grid_set}_{feature_id}.csv')
        df = pd.DataFrame.from_dict(feature_results_dict)
        df['Duration'] = df.index.map(durations_naming)
        df.rename(columns=aeps_naming, inplace=True)

        df.index = df.index.str.lstrip('0')
        df.index.name = 'Duration in min'
        df.reset_index(inplace=True)
        df.set_index('Duration', drop=True, inplace=True)

        if depth_or_intensity == 'Intensity':
            df[[c for c in df.columns if 'Duration'not in c]] = df[[c for c in df.columns if 'Duration'not in c]].divide([float(numerical_durations[d])/60.0 for d in durations], axis=0)

        with open(csv_file, 'w') as outfile:
            outfile.write(header)
        df.to_csv(csv_file, mode='a', float_format="%.1f")

    return None


def createURBS(input_layer, results_dict, grid_set, depth_or_intensity, id_field, output_folder, feedback, context):

    aeps_naming = dict(zip(AEPS['QRA SEQ'], AEPS['URBS']))
    durations_naming = dict(zip(DURATIONS['QRA SEQ'], DURATIONS['URBS']))
    numerical_durations = dict(zip(DURATIONS['QRA SEQ'], DURATIONS['LIMB']))
    
    grid_sets = {
        '_IFD_data_HARC2024\HARC2024_IFDgrids': 'QRA_SEQ',
        '_IFD_data_Max_BoM_HARC2024\MaxBureau2016andHARC2024_IFDgrids_TIFF': 'QRA_SEQ_BOM_Envelope'
    }
    grid_set = grid_sets[grid_set]

    for feature in input_layer.getFeatures():
        feature_id = feature[id_field]

        feature_results_dict = results_dict[feature_id]
        aeps = feature_results_dict.keys()
        durations = feature_results_dict[list(aeps)[0]].keys()

        # feedback.pushInfo(str(durations))
        # feedback.pushInfo(str(numerical_durations))
        # feedback.pushInfo(str([float(numerical_durations[d]) for d in durations]))
        
        df = pd.DataFrame.from_dict(feature_results_dict)
        df['Duration'] = df.index.map(durations_naming)
        df.rename(columns=aeps_naming, inplace=True)

        df.set_index('Duration', drop=True, inplace=True)

        if depth_or_intensity == 'Intensity':
            df = df.divide([float(numerical_durations[d])/60.0 for d in durations], axis=0)

        csv_file = os.path.join(output_folder, f'urbs_ifd_{grid_set}_{feature_id}.ifd')
        df.to_csv(csv_file, float_format="%.1f")

    return None


class IFDTool(QgsProcessingAlgorithm):
    """
    Extracts QRA SEQ 2024 IFD data.
    """

    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        return QCoreApplication.translate('Processing', string)
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        # Must return a new copy of your algorithm.
        return IFDTool()

    def name(self):
        """
        Returns the unique algorithm name.
        """
        return 'IFD extraction tool (QRA SEQ)'

    def displayName(self):
        """
        Returns the translated algorithm name.
        """
        return self.tr('IFD extraction tool (QRA SEQ)')

    def group(self):
        """
        Returns the name of the group this algorithm belongs to.
        """
        return self.tr('Custom scripts')

    def groupId(self):
        """
        Returns the unique ID of the group this algorithm belongs
        to.
        """
        return 'customscripts'

    def shortHelpString(self):
        """
        Returns a localised short help string for the algorithm.
        """
        return self.tr(
            '''
            Extracts QRA SEQ 2024 IFD data.

            Supply either a point or polygon input layer. If a points layer is supplied, the tool uses point inspection (Raster Sampling) to extract IFD values. If a polygon layer is supplied, the tool calculates an area-weighted mean IFD value (Zonal Statistics).

            The tool will create a GIS layer with IFD attributes, as well as IFD tables in the specified format for each point or polygon feature.
            '''
        )

    def initAlgorithm(self, config=None):
        """
        Here we define the inputs and outputs of the algorithm.
        """

        # point or polygon layer
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                "INPUT",
                self.tr('Input point or polygon layer'),
                types=[QgsProcessing.TypeVectorPoint, QgsProcessing.TypeVectorPolygon],
                optional=False
            )
        )

        # unique id field
        self.addParameter(
            QgsProcessingParameterField(
                "id_field",
                self.tr('Unique ID field'),
                '',
                "INPUT",
                optional=False
            )
        )

        # selection of grids (high res, bom scale, envelope)
        grid_set_parameter = QgsProcessingParameterEnum(
                "grid_set",
                self.tr('IFD grid set'),
                options = ['QRA SEQ', 'Maximum envelope QRA SEQ and BoM 2016'],
                allowMultiple = False,
                defaultValue = 'QRA SEQ',
                optional = False,
            )
        grid_set_parameter.setFlags(grid_set_parameter.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(grid_set_parameter)

        # selections for aep and duration
        self.addParameter(
            QgsProcessingParameterEnum(
                "aeps",
                self.tr('Select AEPs'),
                options = AEPS['QGIS'],
                allowMultiple = True,
                optional = False,
            )
        )

        self.addParameter(
            QgsProcessingParameterEnum(
                "durations",
                self.tr('Select durations'),
                options = DURATIONS['QGIS'],
                allowMultiple = True,
                optional = False,
            )
        )

        # optional - grid folder location (defaults to shared network location)
        grid_folder_parameter = QgsProcessingParameterFile(
            "grid_folder",
            self.tr('IFD grids input folder'),
            behavior = 1,
            defaultValue = r'H:\03_Work\03_Code\QRA_SEQ_IFD\QRA_SEQ_IFDUpdate\IFD_data',
            optional = False,
        )
        grid_folder_parameter.setFlags(grid_folder_parameter.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(grid_folder_parameter)

        # specify grid CRS if none
        crs_parameter = QgsProcessingParameterCrs(
            "CRS",
            self.tr('CRS of the grids (if not defined)'),
            defaultValue = QgsCoordinateReferenceSystem(4283, QgsCoordinateReferenceSystem.EpsgCrsId),
            optional=True,
        )
        crs_parameter.setFlags(crs_parameter.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(crs_parameter)

        # drop down to specify output format (defaults to BoM-format CSV)
        output_format_parameter = QgsProcessingParameterEnum(
                "output_format",
                self.tr('Output IFD table format'),
                options = ['BoM CSV', 'URBS'],
                allowMultiple = False,
                defaultValue = 'BoM CSV',
                optional = False,
            )
        output_format_parameter.setFlags(output_format_parameter.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(output_format_parameter)

        # drop down to specify output format (defaults to BoM-format CSV)
        depth_or_intensity_parameter = QgsProcessingParameterEnum(
                "depth_or_intensity",
                self.tr('Rainfall depths or intensities (for output IFD tables only)'),
                options = ['Depth', 'Intensity'],
                allowMultiple = False,
                defaultValue = 'Depth',
                optional = False,
            )
        depth_or_intensity_parameter.setFlags(depth_or_intensity_parameter.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(depth_or_intensity_parameter)

        # location for output folder
        self.addParameter(
            QgsProcessingParameterFolderDestination(
                "output_folder",
                self.tr('Output folder for IFD tables'),
                optional = False,
            )
        )

        # location for output file
        self.addParameter(
            QgsProcessingParameterVectorDestination(
                "OUTPUT",
                self.tr('Output GIS IFD results'),
                optional = False,
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """
        Here is where the processing itself takes place.
        """

        aeps_list = AEPS['QRA SEQ']

        durations_list = DURATIONS['QRA SEQ']

        aeps_enums = self.parameterAsEnums(
            parameters,
            'aeps',
            context
        )
        aeps = [aeps_list[i] for i in aeps_enums]
        # feedback.pushInfo(str(aeps))

        durations_enums = self.parameterAsEnums(
            parameters,
            'durations',
            context
        )
        durations = [durations_list[i] for i in durations_enums]
        # feedback.pushInfo(str(durations))

        # grid_templates = [
        #     'X{0}_{1}Envelope_2016_2020.asc' # Envelope
        #     'X{0}_{1}HighRes_LGA_extent.asc', # High resolution
        #     'X{0}_{1}Bom_scale_LGA_extent.asc', # Low resolution (BoM scale)
        # ]

        grid_sets = ['_IFD_data_HARC2024\HARC2024_IFDgrids', '_IFD_data_Max_BoM_HARC2024\MaxBureau2016andHARC2024_IFDgrids_TIFF']

        grid_set_enum = self.parameterAsEnum(
            parameters,
            'grid_set',
            context
        )
        grid_set = grid_sets[grid_set_enum]

        depths_or_intensity_options = [
            'Depth',
            'Intensity',
        ]

        depth_or_intensity_enum = self.parameterAsEnum(
            parameters,
            'depth_or_intensity',
            context
        )
        depth_or_intensity = depths_or_intensity_options[depth_or_intensity_enum]

        output_format_options = ['BoM CSV', 'URBS']

        output_format_enum = self.parameterAsEnum(
            parameters,
            'output_format',
            context
        )
        output_format = output_format_options[output_format_enum]

        base_grid_folder = self.parameterAsFile(
            parameters,
            'grid_folder',
            context
        )
        grid_folder = os.path.join(base_grid_folder, grid_set)

        output_folder = self.parameterAsFile(
            parameters,
            'output_folder',
            context
        )

        input_layer = self.parameterAsVectorLayer(
            parameters,
            'INPUT',
            context
        )

        id_field = self.parameterAsString(
            parameters,
            'id_field',
            context
        )

        grid_crs = self.parameterAsCrs(
            parameters,
            'CRS',
            context
        )

        if input_layer.featureCount() == 0:
            feedback.pushInfo("Input layer is blank. Nothing to process.") # Thanks for finding this bug, Tom.
            return {}

        if not input_layer.sourceCrs().isValid():
            input_crs = self.parameterAsCrs(
                parameters,
                'CRS',
                context
            )
            input_layer.setCrs(input_crs)

        grids = glob.glob(os.path.join(grid_folder, "*.tiff"))
        grid = QgsRasterLayer(grids[0])
        if not grid.crs().isValid():
            grid.setCrs(grid_crs)

        # reproject input layer if not in same CRS as grids
        if not grid.crs() == input_layer.sourceCrs():
            layer_to_process = processing.run(
                "native:reprojectlayer",
                {
                    'INPUT': input_layer,
                    'TARGET_CRS': grid.crs(),
                    'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                }
            )['OUTPUT']
        else:
            layer_to_process = input_layer

        if feedback.isCanceled():
                    return {}

        # process all AEPs and Durations
        ids = list(input_layer.uniqueValues(input_layer.fields().indexFromName(id_field)))
        # feedback.pushInfo(str(ids))
        results_dict = {i:{aep:{duration:'' for duration in durations} for aep in aeps} for i in ids}
        # feedback.pushInfo(str(results_dict))
        first = True
        for aep in aeps:
            for duration in durations:
                if first:
                    pass
                else:
                    layer_to_process = result['OUTPUT']
                # grid_name = grid_template.format(duration, aep)
                grid_match = [g for g in grids if f'IFD_{"ARI" if aep in ("2y", "5y") else "AEP"}_{aep}_{duration}' in g]
                if grid_match:
                    grid_name = grid_match[0]
                    grid_layer = QgsRasterLayer(grid_name)
                    if not grid_layer.crs().isValid():
                        grid_layer.setCrs(grid_crs)
                    # feedback.pushInfo(f"{duration} {aep} {grid_name} {grid_layer}")
                    feedback.pushInfo(f"Working on {aep} {duration} ({grid_name})...")

                    if input_layer.geometryType() == 0: # point
                        #   run "native:rastersampling" for points
                        result = processing.run(
                            'native:rastersampling',
                            {
                                'INPUT': layer_to_process,
                                'RASTERCOPY': grid_layer,
                                'COLUMN_PREFIX': '{0}_{1}_'.format(aep, duration),
                                'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
                            }
                        )
                    elif input_layer.geometryType() == 2: # polygon
                        #   run "native:zonalstatisticsfb" (mean) for polygons
                        result = processing.run(
                            'native:zonalstatisticsfb',
                            {
                                'INPUT': layer_to_process,
                                'INPUT_RASTER': grid_layer,
                                'RASTER_BAND': 1,
                                'COLUMN_PREFIX': '{0}_{1}_'.format(aep, duration),
                                'STATISTICS': 2,
                                'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
                            }
                        )
                    else:
                        # TODO raise error if geom type not point or polygon - do it somewhere else
                        pass

                    ifd_field = [f for f in result['OUTPUT'].fields().names() if (aep in f) and (duration in f)][0]
                    for feature in result['OUTPUT'].getFeatures():
                        feature_id = feature[id_field]
                        feature_ifd = feature[ifd_field]
                        results_dict[feature_id][aep][duration] = feature_ifd

                    first = False
                else:
                    feedback.pushInfo(f"No grid found for {aep} {duration}...")

                if feedback.isCanceled():
                    return {}

        if feedback.isCanceled():
            return {}

        output = self.parameterAsOutputLayer(
            parameters,
            'OUTPUT',
            context,
        )

        # reproject result layer to original CRS if not in same CRS as grids
        if not grid.crs() == input_layer.sourceCrs():
            save = processing.run(
                "native:reprojectlayer",
                {
                    'INPUT': result['OUTPUT'],
                    'TARGET_CRS': input_layer.crs(),
                    'OUTPUT': output
                }
            )
        else:
            save = processing.run(
                "native:savefeatures",
                {
                    'INPUT': result['OUTPUT'],
                    'OUTPUT': output
                }
            )

        if output_format == 'BoM CSV':
            createBomCSVs(input_layer, results_dict, grid_set, depth_or_intensity, id_field, output_folder, feedback, context)
        elif output_format == 'URBS':
            createURBS(input_layer, results_dict, grid_set, depth_or_intensity, id_field, output_folder, feedback, context)
        else:
            feedback.pushInfo(f"Output format {output_format} not supported or implemented.")

        return {
            'OUTPUT': save['OUTPUT'],
            'IFD table folder': output_folder
        }