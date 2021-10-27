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
    QgsProcessingParameterFileDestination,
)
from qgis import processing
from PyQt5.QtCore import QVariant

from itertools import chain, zip_longest
import re
from typing import Optional
from dataclasses import dataclass

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


class RunfileBlock:

    start_line = 'STARTLINE'
    end_line = 'ENDLINE'

    def __init__(self, runfile_contents=None, gis_layer=None, subcatchment_id_field=None, feedback=None):
        
        self.runfile_contents = runfile_contents
        self.gis_layer = gis_layer
        self.subcatchment_id_field = subcatchment_id_field
        self.block_contents = None

        if runfile_contents:
            self.block_contents = self.read_runfile()

    def read_runfile(self):
        start = re.compile(re.escape(self.start_line))
        end = re.compile(re.escape(self.end_line))
        block_contents = []
        inside_block = False
        for line in self.runfile_contents:
            if inside_block:
                if re.match(end, line):
                    inside_block = False
                else:
                    block_contents.append(line)
            elif re.match(start, line):
                inside_block = True
        return block_contents

    # def read_gis_layer(self):
    #     features = self.gis_layer.getFeatures()
    #     return features

    
@dataclass
class CatchmentTopology:
    name: str
    cg_e: str
    cg_n: str
    outlet_e: str
    outlet_n: str
    downstream_sub_name: str


class TopologyBlock(RunfileBlock):

    start_line = '#####START_TOPOLOGY_BLOCK##########|###########|###########|###########|'
    end_line = '#####END_TOPOLOGY_BLOCK############|###########|###########|###########|'

    def __init__(self, runfile_contents=None, gis_layer=None, subcatchment_id_field=None, ds_id_field=None, feedback=None):
        super().__init__(runfile_contents, gis_layer, subcatchment_id_field)

        self.ds_id_field = ds_id_field

        self.num_subareas = None
        self.catchment_name = None
        self.topology = {}

        if self.block_contents:
            self.num_subareas, self.catchment_name = re.search(re.compile('([0-9]+)\s*(.*)'), self.block_contents[0]).groups()
            for catchment_topology_line in self.block_contents[1:]:
                name = catchment_topology_line.split()[0]
                self.topology[name] = CatchmentTopology(*catchment_topology_line.split())
        elif self.gis_layer:
            self.num_subareas = self.gis_layer.featureCount()
            self.catchment_name = self.gis_layer.name()
            for feature in self.gis_layer.getFeatures():
                self.topology[feature[self.subcatchment_id_field]] = CatchmentTopology(
                    name = feature[self.subcatchment_id_field],
                    cg_e = feature['centroid_x'],
                    cg_n = feature['centroid_y'],
                    outlet_e = feature['outlet_x'],
                    outlet_n = feature['outlet_y'],
                    downstream_sub_name = feature[self.ds_id_field],
                )
            self.topology = self.sort()
    
        if feedback:
            feedback.pushInfo(str(self.topology))

    def sort(self):
        topology_dict = {k:v.downstream_sub_name for k,v in self.topology.items()}
        return {k:self.topology[k] for k in wbnm_sort(topology_dict)}
    
    def write(self):
        s = self.start_line+'\n'
        s = s+f'{str(self.num_subareas):>12}'+'\n'
        for _, catchment in self.topology.items():
            s = s+'{0:<12}{1:>12.1f}{2:>12.1f}{3:>12.1f}{4:>12.1f} {5:<12}\n'.format(
                catchment.name, 
                catchment.cg_e, 
                catchment.cg_n, 
                catchment.outlet_e, 
                catchment.outlet_n, 
                catchment.downstream_sub_name
            )
        s = s+self.end_line+'\n'

        return s

@dataclass
class CatchmentSurface:
    name: str
    area: str
    imp: str
    lag: str
    imp_lag: str


class SurfacesBlock(RunfileBlock):

    start_line = '#####START_SURFACES_BLOCK##########|###########|###########|###########|'
    end_line = '#####END_SURFACES_BLOCK############|###########|###########|###########|'

    defaults = {
            'nonlinearity_exponent': 0.77,
            'discharge_when_routing_switches': -99.9,
            'lag': 1.6,
            'imp_lag': 0.1
        }

    def __init__(
        self, runfile_contents=None, gis_layer=None, subcatchment_id_field=None, 
        ds_id_field=None, imp_field=None, topology=None, feedback=None
    ):
        super().__init__(runfile_contents, gis_layer, subcatchment_id_field)

        self.ds_id_field = ds_id_field
        self.imp_field = imp_field
        self.topology = topology

        self.nonlinearity_exponent = None
        self.discharge_when_routing_switches = None
        self.surfaces = {}

        if self.block_contents:
            self.nonlinearity_exponent = self.block_contents[0]
            self.discharge_when_routing_switches = self.block_contents[0]
            self.surfaces = {}
            for catchment_surface_line in self.block_contents[2:]:
                name = catchment_surface_line.split()[0]
                self.surfaces[name] = CatchmentSurface(*catchment_surface_line.split())
        elif self.gis_layer:
            self.nonlinearity_exponent = self.defaults['nonlinearity_exponent']
            self.discharge_when_routing_switches = self.defaults['discharge_when_routing_switches']
            for feature in self.gis_layer.getFeatures():
                self.surfaces[feature[self.subcatchment_id_field]] = CatchmentSurface(
                    name = feature[self.subcatchment_id_field],
                    area = feature['area'],
                    imp = feature[self.imp_field],
                    lag = self.defaults['lag'],
                    imp_lag = self.defaults['imp_lag'],
                )
            self.surfaces = self.sort()
    
        if feedback:
            feedback.pushInfo(str(self.surfaces))

    def sort(self):
        topology_dict = {k:v.downstream_sub_name for k,v in self.topology.items()}
        return {k:self.surfaces[k] for k in topology_dict}
    
    def write(self):
        s = self.start_line+'\n'
        s = s+'{0:>12}{1:>12}{2:>12}\n'.format(
            self.nonlinearity_exponent,
            self.defaults['lag'],
            self.defaults['imp_lag'],
        )
        s = s+f'{str(self.discharge_when_routing_switches):>12}'+'\n'
        for _, surface in self.surfaces.items():
            s = s+'{0:<12}{1:>12.2f}{2:>12.2f}\n'.format(
                surface.name, 
                surface.area, 
                surface.imp, 
                # surface.lag, 
                # surface.imp_lag, 
            )
        s = s+self.end_line+'\n'

        return s

@dataclass
class CatchmentFlowpath:
    name: str
    routing_type: str
    stream_lag: Optional[str] = None
    delay: Optional[str] = None
    musk_k: Optional[str] = None
    musk_x: Optional[str] = None


class FlowpathsBlock(RunfileBlock):

    start_line = '#####START_FLOWPATHS_BLOCK#########|###########|###########|###########|'
    end_line = '#####END_FLOWPATHS_BLOCK###########|###########|###########|###########|'

    routing_types = {
        '#####ROUTING': 'routing',
        '#####DELAY': 'delay',
        '#####MUSK': 'musk',
    }

    defaults = {
        'routing': 'routing',
        'stream_lag': 1.0,
    }

    def __init__(self, runfile_contents=None, gis_layer=None, subcatchment_id_field=None, topology=None, feedback=None):
        super().__init__(runfile_contents, gis_layer, subcatchment_id_field)

        self.topology = topology

        self.num_subareas_with_stream = None
        self.flowpaths = {}

        if self.block_contents:
            self.num_subareas_with_stream = self.block_contents[0]
            for name, routing_line, value in grouper(3, self.block_contents[1:]):
                name = name.strip()
                routing_type = self.routing_types[routing_line.strip()]
                if routing_type == 'routing':
                    stream_lag = value.strip()
                    self.flowpaths[name] = CatchmentFlowpath(name, routing_type, stream_lag=stream_lag)
                elif routing_type == 'delay':
                    delay = value.strip()
                    self.flowpaths[name] = CatchmentFlowpath(name, routing_type, delay=delay)
                elif routing_type == 'musk':
                    musk_k, musk_x = value.strip().split()
                    self.flowpaths[name] = CatchmentFlowpath(name, routing_type, musk_k=musk_k, musk_x=musk_x)
        elif self.gis_layer:
            self.num_subareas_with_stream = self.gis_layer.featureCount()
            for feature in self.gis_layer.getFeatures():
                self.flowpaths[feature[self.subcatchment_id_field]] = CatchmentFlowpath(
                    name = feature[self.subcatchment_id_field],
                    routing_type = self.defaults['routing'],
                    stream_lag = self.defaults['stream_lag'],
                )
            self.flowpaths = self.sort()

    def sort(self):
        topology_dict = {k:v.downstream_sub_name for k,v in self.topology.items()}
        return {k:self.flowpaths[k] for k in topology_dict}
    
    def write(self):
        s = self.start_line+'\n'
        s = s+f'{str(self.num_subareas_with_stream):>12}'+'\n'
        for _, flowpath in self.flowpaths.items():
            s = s+'{0:<12}\n{1:>12}\n{2:>12.2f}\n'.format(
                flowpath.name, 
                {v:k for k, v in self.routing_types.items()}[flowpath.routing_type], 
                flowpath.stream_lag, 
            )
        s = s+self.end_line+'\n'

        return s

def ds_ranker(topology: dict) -> list:
    r = []
    for s, ds in topology.items():
        if ds in topology.keys():
            r.append(list(topology.keys()).index(ds))
        else:
            r.append(len(topology)+1)

    return r

def shuffle(topology: dict) -> dict:
    import random
    keys = list(topology.keys())
    random.shuffle(keys)
    shuffled = {}
    for k in keys:
        shuffled[k] = topology[k]

    return shuffled

def trace(s, topology, _seen=None, _path=None):
    if _seen is None:
        _seen = set()

    if _path is None:
        _path = []

    if s in _seen:
        return ' -> '.join(_path[_path.index(s):])+' -> '+str(s)
        
    _seen.add(s)
    _path.append(s)

    try:
        topology[s]
    except KeyError:
        return False

    return trace(topology[s], topology, _seen, _path)

def detect_circular_ref(topology: dict) -> bool:
    for s in topology.keys():
        trace_result = trace(s, topology)
        if trace_result:
            return trace_result
    return False

def integrity_check(topology):
    if not list(topology.values()).count('SINK') == 1:
        raise ValueError('Exactly one subcatchment must be connected to SINK.')
    for k, v in topology.items():
        if k in [QVariant(), None, 'None', 'NULL']:
            raise ValueError('Subcatchment ID missing or blank.')
        if v in [QVariant(), None, 'None', 'NULL']:
            raise ValueError('Subcatchment {k}\'s downstream node is blank.')
        if k == v:
            raise ValueError(f'Subcatchment {k} is connected to itself.')
        if v not in topology.keys() and v != 'SINK':
            raise ValueError(f'Subcatchment {k}\'s downstream node {v} does not exist.')
        if len(k) > 12:
            raise ValueError(f'Subcatchment {k}s name exceeds 12 characters in length.')

    circular_ref = detect_circular_ref(topology)
    if circular_ref:
        raise ValueError(f'Subcatchment routing contains a circular reference: {circular_ref}')

def wbnm_sort(topology: dict) -> dict:
    integrity_check(topology)

    wbnm_sorted = list(topology.keys())
    ds_rank = ds_ranker(topology)

    while any([x > ds_rank[x] for x in range(len(wbnm_sorted))]):
        new_topology = {k:topology[k] for k in wbnm_sorted}
        ds_rank = ds_ranker(new_topology)

        for n, s in enumerate(wbnm_sorted):
            if ds_rank[n] == len(topology)+1:
                wbnm_sorted.insert(len(wbnm_sorted)-1, wbnm_sorted.pop(n))
                break
            else:
                if n > ds_rank[n]:
                    wbnm_sorted.insert(ds_rank[n], wbnm_sorted.pop(n))
                    break
                else:
                    continue
    
    return {k:topology[k] for k in wbnm_sorted}

class WBNMHelper(QgsProcessingAlgorithm):
    """
    Helper script for creating WBNM runfiles.
    """

    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        # Must return a new copy of your algorithm.
        return WBNMHelper()

    def name(self):
        """
        Returns the unique algorithm name.
        """
        return 'WBNM Helper'

    def displayName(self):
        """
        Returns the translated algorithm name.
        """
        return self.tr('WBNM Helper')

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
        return self.tr('Helps with creating WBNM runfiles.')

    def initAlgorithm(self, config=None):
        """
        Here we define the inputs and outputs of the algorithm.
        """
        # 'INPUT' is the recommended name for the main input
        # parameter.
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                'Subcatchments',
                self.tr('Input subcatchment vector layer'),
                types=[QgsProcessing.TypeVectorAnyGeometry]
            )
        )
        self.addParameter(
            QgsProcessingParameterField(
                'ID_field',
                self.tr('Select subcatchment ID/name field'),
                '',
                'Subcatchments',
                optional=False
            )
        )
        self.addParameter(
            QgsProcessingParameterField(
                'DS_ID_field',
                self.tr('Select downstream subcatchment ID/name field'),
                '',
                'Subcatchments',
                optional=False
            )
        )
        self.addParameter(
            QgsProcessingParameterField(
                'imp_field',
                self.tr('Select impervious percentage field'),
                '',
                'Subcatchments',
                optional=False
            )
        )
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                'Outlets',
                self.tr('Subcatchment outlets points layer'),
                types=[QgsProcessing.TypeVectorAnyGeometry]
            )
        )
        # 'OUTPUT' is the recommended name for the main output
        # parameter.
        self.addParameter(
            QgsProcessingParameterVectorDestination(
                'OUTPUT',
                self.tr('Processed WBNM subcatchment vector file')
            )
        )

        self.addParameter(
            QgsProcessingParameterFileDestination(
                'TEXT',
                self.tr('Processed WBNM subcatchment text file'),
                defaultValue=r"C:\00_Projects\99_Misc\crendon\Crendon\text.txt"
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """
        Here is where the processing itself takes place.
        """
        subcatchments_layer = self.parameterAsVectorLayer(
            parameters,
            'Subcatchments',
            context
        )

        subcatchment_id_field = self.parameterAsString(
            parameters,
            'ID_field',
            context
        )

        ds_id_field = self.parameterAsString(
            parameters,
            'DS_ID_field',
            context
        )

        imp_field = self.parameterAsString(
            parameters,
            'imp_field',
            context
        )

        outlets_layer = self.parameterAsVectorLayer(
            parameters,
            'Outlets',
            context
        )

        area = processing.run(
            "native:fieldcalculator", 
            {
                'INPUT':subcatchments_layer,
                'FIELD_NAME':'area',
                'FIELD_TYPE':0,
                'FIELD_LENGTH':0,
                'FIELD_PRECISION':0,
                'FORMULA':'$area / 10000',
                'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
            },
            context=context, 
            feedback=feedback,
            is_child_algorithm=True,
        )

        centroid_x = processing.run(
            "native:fieldcalculator", 
            {
                'INPUT':area['OUTPUT'],
                'FIELD_NAME':'centroid_x',
                'FIELD_TYPE':0,
                'FIELD_LENGTH':0,
                'FIELD_PRECISION':0,
                'FORMULA':'x(centroid($geometry))',
                'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
            },
            context=context, 
            feedback=feedback,
            is_child_algorithm=True,
        )

        if feedback.isCanceled():
            return {}

        centroid_y = processing.run(
            "native:fieldcalculator", 
            {
                'INPUT':centroid_x['OUTPUT'],
                'FIELD_NAME':'centroid_y',
                'FIELD_TYPE':0,
                'FIELD_LENGTH':0,
                'FIELD_PRECISION':0,
                'FORMULA':'y(centroid($geometry))',
                'OUTPUT':QgsProcessing.TEMPORARY_OUTPUT
            },
            context=context, 
            feedback=feedback,
            is_child_algorithm=True,
        )

        if feedback.isCanceled():
            return {}

        outlet_x = processing.run(
            "native:fieldcalculator", 
            {
                'INPUT':centroid_y['OUTPUT'],
                'FIELD_NAME':'outlet_x',
                'FIELD_TYPE':0,
                'FIELD_LENGTH':0,
                'FIELD_PRECISION':0,
                'FORMULA':f"aggregate('{outlets_layer.name()}','max',$x,\"{subcatchment_id_field}\"=attribute(@parent,'{subcatchment_id_field}'))",
                'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            },
            context=context, 
            feedback=feedback,
            is_child_algorithm=True,
        )

        if feedback.isCanceled():
            return {}

        outlet_y = processing.run(
            "native:fieldcalculator", 
            {
                'INPUT':outlet_x['OUTPUT'],
                'FIELD_NAME':'outlet_y',
                'FIELD_TYPE':0,
                'FIELD_LENGTH':0,
                'FIELD_PRECISION':0,
                'FORMULA':f"aggregate('{outlets_layer.name()}','max',$y,\"{subcatchment_id_field}\"=attribute(@parent,'{subcatchment_id_field}'))",
                'OUTPUT':parameters['OUTPUT']
            },
            context=context, 
            feedback=feedback,
            is_child_algorithm=True,
        )

        if feedback.isCanceled():
            return {}

        topo_block = TopologyBlock(
            gis_layer=context.getMapLayer(outlet_y['OUTPUT']), 
            subcatchment_id_field=subcatchment_id_field, 
            ds_id_field=ds_id_field,
            feedback=feedback
        )

        surfaces_block = SurfacesBlock(
            gis_layer=context.getMapLayer(outlet_y['OUTPUT']), 
            subcatchment_id_field=subcatchment_id_field, 
            ds_id_field=ds_id_field,
            fi_field=imp_field,
            topology=topo_block.topology,
            # feedback=feedback
        )

        flowpaths_block = FlowpathsBlock(
            gis_layer=context.getMapLayer(outlet_y['OUTPUT']), 
            subcatchment_id_field=subcatchment_id_field, 
            topology=topo_block.topology,
            # feedback=feedback
        )

        with open(parameters['TEXT'], 'w') as outfile:
            outfile.write(topo_block.write())
            outfile.write(surfaces_block.write())
            outfile.write(flowpaths_block.write())


        # Return the results
        return {'OUTPUT': outlet_y['OUTPUT']}
        # return {'OUTPUT': dest_id}
