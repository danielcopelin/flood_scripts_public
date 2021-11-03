from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (
    QgsVectorLayer,
    QgsExpression,
    QgsField,
    QgsFeature,
    QgsFeatureSink,
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterField,
    QgsProcessingParameterVectorDestination,
    QgsProcessingParameterFeatureSink,
    QgsProcessingParameterNumber,
    QgsProcessingParameterMatrix,
)
from qgis import processing
from PyQt5.QtCore import QVariant

from itertools import chain


class ImperviousFraction(QgsProcessingAlgorithm):
    """
    This is an example algorithm that takes a vector layer,
    creates some new layers and returns some results.
    """

    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        # Must return a new copy of your algorithm.
        return ImperviousFraction()

    def name(self):
        """
        Returns the unique algorithm name.
        """
        return 'Impervious Fraction'

    def displayName(self):
        """
        Returns the translated algorithm name.
        """
        return self.tr('Impervious Fraction')

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
        return self.tr('Calculates fraction impervious for catchments based on City Plan 2014 Zoning.')

    def initAlgorithm(self, config=None):
        """
        Here we define the inputs and outputs of the algorithm.
        """
        # 'INPUT' is the recommended name for the main input
        # parameter.
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                'Catchments',
                self.tr('Input catchment vector layer'),
                types=[QgsProcessing.TypeVectorAnyGeometry]
            )
        )
        self.addParameter(
            QgsProcessingParameterField(
                'ID_field',
                self.tr('Select catchment ID/name field'),
                '',
                'Catchments',
                optional=False
            )
        )

        self.addParameter(
            QgsProcessingParameterFeatureSource(
                'Zones',
                self.tr('Input zones vector layer'),
                types=[QgsProcessing.TypeVectorAnyGeometry]
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                'zone_field',
                self.tr('Select land use / zone field'),
                '',
                'Zones',
                optional=False
            )
        )


        self.addParameter(
            QgsProcessingParameterFeatureSource(
                'Roads',
                self.tr('Input road corridor vector layer'),
                types=[QgsProcessing.TypeVectorAnyGeometry],
                optional=True,
            )
        )

        self.addParameter(
            QgsProcessingParameterNumber(
                'roads_imp',
                self.tr('Roads impervious percentage'),
                type=1,
                defaultValue=50.0,
            )
        )

        self.addParameter(
            QgsProcessingParameterNumber(
                'default_imp',
                self.tr('Default impervious percentage (used for missing areas or zones)'),
                type=1,
                defaultValue=5.0,
            )
        )

        # fraction impervious values - reference https://www.brisbane.qld.gov.au/sites/default/files/documents/2021-05/27052021-L6-Extrinsic-Material-Stormwater-word.docx
        self.addParameter(
            QgsProcessingParameterMatrix(
                'imp_matrix',
                self.tr('Land use or zone-based fractions impervious'),
                numberRows=10,
                headers=['Land use / zone', 'Fraction impervious'],
                defaultValue=list(chain.from_iterable({
                    "EM": 0, #EM - Environmental management
                    "RU": 5.0, #RU - Rural
                    "LDR":  75.0, #LDR - Low density residential,
                    "SP": 10.0, #SP4 - Special purpose (Utility services)
                    "CN": 0, #CN - Conservation
                    "LMR": 85.0, #LMR3 - Low-medium density residential (Up to 3 storeys)
                    "OS": 0, #OS1 - Open space (Local)
                    "SR": 10.0, #SR2 - Sport and recreation (District)
                    "NC": 90.0, #NC - Neighbourhood centre
                    "CF": 60.0, #CF - Community facilities
                    # "GR": 85.0,
                    # "RR20": 20.0,
                    # "RR15": 15.0
                }.items())),
            )
        )

        # 'OUTPUT' is the recommended name for the main output
        # parameter.
        self.addParameter(
            QgsProcessingParameterVectorDestination(
                'OUTPUT',
                self.tr('Catchments with fraction impervious')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """
        Here is where the processing itself takes place.
        """
        catchment_id_field = self.parameterAsString(
            parameters,
            'ID_field',
            context
        )

        zone_field = self.parameterAsString(
            parameters,
            'zone_field',
            context
        )

        imp_matrix = self.parameterAsMatrix(parameters, 'imp_matrix', context)
        imp_dict = dict(zip(*[iter(imp_matrix)]*2))
        imp_dict[QVariant()] = self.parameterAsDouble(parameters, 'roads_imp', context)

        default_imp = self.parameterAsDouble(parameters, 'default_imp', context) # used any missing zone areas or any zones that are missing from the above dictionary, noting that features with blank land use attributes will be treated as roads.

        catchments_layer = self.parameterAsVectorLayer(
            parameters,
            'Catchments',
            context
        )

        output_fields = catchments_layer.fields()
        output_fields.append(QgsField("imp_percent",QVariant.Double))
        (sink, dest_id) = self.parameterAsSink(
            parameters,
            'OUTPUT',
            context,
            output_fields,
            catchments_layer.wkbType(), 
            catchments_layer.sourceCrs(),
        )

        if parameters['Roads']:
            merge = processing.run(
                "native:mergevectorlayers",
                {
                    'LAYERS': [parameters['Zones'], parameters['Roads']],
                    'CRS': parameters['Zones'],
                    'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
                },
                # is_child_algorithm=True,
                context=context,
                feedback=feedback
            )['OUTPUT']
        else:
            merge = parameters['Zones']


        if feedback.isCanceled():
            return {}

        intersection = processing.run(
            "native:intersection",
            {
                'INPUT': parameters['Catchments'],
                'OVERLAY': merge,
                'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
                # 'OUTPUT': output,
            },
            # is_child_algorithm=True,
            context=context,
            feedback=feedback
        )

        if feedback.isCanceled():
            return {}

        # add fraction impervious to each intersected polygon based on zones
        # intersection_layer = QgsVectorLayer(intersection['OUTPUT'])
        intersection_layer = intersection['OUTPUT']
        if not 'imp_percent' in intersection_layer.fields().names():
            layer_provider = intersection_layer.dataProvider()
            layer_provider.addAttributes([QgsField("imp_percent",QVariant.Double)])
            intersection_layer.updateFields()

        intersection_layer.startEditing()
        features = intersection_layer.getFeatures()
        for feature in features:
            feature['imp_percent'] = imp_dict.get(feature[zone_field], default_imp)
            intersection_layer.updateFeature(feature)
        intersection_layer.commitChanges()

        # calculate area weighted fi for each catchment
        catchments_features = catchments_layer.getFeatures()
        for catchment in catchments_features:
            feedback.pushInfo(str(catchment['id']))
            new_feature = QgsFeature()
            new_feature.setGeometry(catchment.geometry())
            name = catchment[catchment_id_field]
            intersection_features = [f for f in intersection_layer.getFeatures() if f[catchment_id_field] == name]
            intersection_area = sum([f.geometry().area() for f in intersection_features])
            balance_area = catchment.geometry().area() - intersection_area
            area_weighted_imp = sum([f['imp_percent']*f.geometry().area() for f in intersection_features]+[balance_area * default_imp]) / catchment.geometry().area()
            new_feature.setAttributes(catchment.attributes()+[area_weighted_imp])
            sink.addFeature(new_feature, QgsFeatureSink.FastInsert)

        if feedback.isCanceled():
            return {}

        # Return the results
        # return {'OUTPUT': intersection['OUTPUT']}
        return {'OUTPUT': dest_id}
