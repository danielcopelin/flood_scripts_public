"""
Script to create URBS vector and catchment data files. 
Created by Dan Copelin
danielcopelin@gmail.com

Area: Sub-catchment area in km2
UL: Low density Urban fraction
UM: Medium density Urban Fraction
UH: High density Urban Fraction
UD: Fraction of sub-catchment disturbed for urban development
UR: Fraction of sub-catchment with rural land (cleared but not forested)
UF: Fraction of sub-catchment forested.
CS: Catchment slope (m/m)
CS: Channel slope (m/m) when URBS_AKEN=TRUE
SSR: Site Storage Requirement (m3/hectare) for sub-catchment
PSD: Permissible Site Storage (L/s/hectare) for sub-catchment
TBO: Fraction of sub-catchment to be OSD'd
Q: Maximum capacity of sub-catchment minor drainage system (m3/s)
I: Fraction of catchment impervious
IF: Maximum Infiltration capacity of sub-catchment (mm)
N: sub-catchment roughness (used when URBS_AKEN=TRUE)
BF: Sub catchment Beta scaling factor

QUANTITY                                UNITS
Rainfall                                mm
Initial Loss                            mm
Catchment Slope (CS)                    m/m
Channel Slope (SC)                      m/m
Continuing Loss Rate                    mm/h
Proportion of Runoff                    fraction
Capillary Suction Head                  mm
Saturated Loss Rate                     mm/h
Discharge                               m3/s
Forestation                             fraction
Flow Height                             m
Area (sub-catchment)                    km2
Area (storages)                         ha
Reach Length                            km
Average Distance                        km
Urbanisation                            fraction
Volume, Storage                         ML
Storm Duration                          H
Run Duration                            H
Time Increment (Rainfall File)          H
Time Increment (Pluviograph Data File)  S
Sediment                                tonnes
Export Rates                            kg/km2/yr
Site Storage Requirement                m3/ha
Permissible Site Discharge              l/s/ha
Sewered Channel Capacity                m3/s
Max Infiltration Capacity               mm
Impervious fraction                     fraction
"""

from qgis.PyQt.QtCore import *
from qgis.core import (
    QgsProcessing,
    QgsFeatureSink,
    QgsProcessingException,
    QgsProcessingAlgorithm,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterFeatureSink,
    QgsProcessingParameterField,
    QgsProcessingParameterFolderDestination,
)
from qgis import processing

from threading import local
from typing import Union
from pathlib import Path
import csv


class NodeError(KeyError):
    pass

class Base:
    def __str__(cls):
        return f"{type(cls)}: {cls.name}"
    def __repr__(cls):
        return f"{type(cls)}: {cls.name}"

class Model(Base):
    def __init__(
        self,
        name: str,
        uses: list,
    ):
        self.name = name
        self.uses = uses

        self.outlet = ''
        self.nodes = {}

        self.branch_counter = 0
        self.processed_nodes = []
        self.urbs_vector = self._init_vector()
        self.branch_stack = []

    # def plot_tree(self):
    #     import networkx as nx
    #     from networkx.drawing.nx_agraph import graphviz_layout

    #     paths = [
    #         [name, node.downstream_node.name] for name, node in self.nodes.items()
    #     ]
    #     G = nx.DiGraph()
    #     for path in paths:
    #         nx.add_path(G, path)

    #     pos=graphviz_layout(G, prog='dot')
    #     nx.draw(
    #         G, pos=pos,
    #         node_color='lightgreen', 
    #         node_size=1500,
    #         with_labels=True, 
    #         arrows=True
    #     )

    def _init_vector(self):
        return f"{self.name}\nMODEL: SPLIT\nUSES: L, CS , SC, U, F\nDEFAULT PARAMTERS: ALPHA = 0.25 M = 0.8 BETA = 2.5 N = 1 X = 0 UHI = 0.75 UMI = 0.35 ULI = 0.1\nCATCHMENT DATA FILE = {self.name}.csv\n"

    def _store(self, downstream_node):
        self.branch_stack.append(downstream_node.name)
        self.urbs_vector+='STORE.\n'

    def _get(self):
        self.branch_stack.pop()
        self.urbs_vector+='GET.\n'

    def _rain(self, node):
        rain_line = f"RAIN #{node.name}\n"
        if 'L_ds' in node.parameters.keys(): # add subcatchment specific downstream channel routing parameters
            rain_line = rain_line.rstrip() + f" L = {node.parameters['L_ds'] / 2 :.4f}\n"  
        if 'SC_ds' in node.parameters.keys():
            rain_line = rain_line.rstrip() + f" SC = {node.parameters['SC_ds'] :.4f}\n"  
        if 'CS_ds' in node.parameters.keys():
            rain_line = rain_line.rstrip() + f" CS = {node.parameters['CS']}\n"  
        if 'n_ds' in node.parameters.keys():
            rain_line = rain_line.rstrip() + f" n = {node.parameters['n_ds']}\n"  

        self.urbs_vector+=rain_line

    def _add_rain(self, node):
        rain_line = f"ADD RAIN #{node.name}\n"
        if 'L_ds' in node.parameters.keys(): # add subcatchment specific downstream channel routing parameters
            rain_line = rain_line.rstrip() + f" L = {node.parameters['L_ds'] / 2 :.4f}\n"  
        if 'SC_ds' in node.parameters.keys():
            rain_line = rain_line.rstrip() + f" SC = {node.parameters['SC_ds'] :.4f}\n"  
        if 'CS_ds' in node.parameters.keys():
            rain_line = rain_line.rstrip() + f" CS = {node.parameters['CS']}\n"  
        if 'n_ds' in node.parameters.keys():
            rain_line = rain_line.rstrip() + f" n = {node.parameters['n_ds']}\n"  
        
        self.urbs_vector+=rain_line

    def _route_thru(self, node):
        route_line = f"ROUTE THRU #{node.name}\n"
        if 'L_ds' in node.parameters.keys(): # add subcatchment specific downstream channel routing parameters
            route_line = route_line.rstrip() + f" L = {node.parameters['L_ds'] /2 :.4f}\n"  
        if 'SC_ds' in node.parameters.keys():
            route_line = route_line.rstrip() + f" SC = {node.parameters['SC_ds'] :.4f}\n"  
        if 'n_ds' in node.parameters.keys():
            route_line = route_line.rstrip() + f" n = {node.parameters['n_ds']}\n"  

        self.urbs_vector+=route_line

    def _print_local(self, node):
        self.urbs_vector+=f"PRINT.{node.name}_Local\n"

    def _print_total(self, node):
        self.urbs_vector+=f"PRINT.{node.name}_Total*\n"

    def validate_subcatchments(self):
        for name, node in self.nodes.items():
            node.validate()

    def validate_subcatchments(self):
        for name, node in self.nodes.items():
            node.validate()

    def urbs_route(self, node):

        # don't route if the subcatchment dischargeas to the outlet
        if node.name == self.outlet.name:
            return

        # find all the branch nodes connecting to the downstream node that haven't already been processed
        branch_nodes = {name: node for name, node in node.branch_nodes().items() if name not in self.processed_nodes}
        
        # rain if a source node, otherwise addrain
        if node.name in self.source_nodes.keys():
            self._rain(node)
            self.processed_nodes.append(node.name)
            if node.local:
                self._print_local(node)
        else:
            if node.local:
                self._store(node)
                self._rain(node)
                if node.local:
                    self._print_local(node)
                self._get()
            else:
                self._add_rain(node)
            self.processed_nodes.append(node.name)

        if node.total:
            self._print_total(node)

        # process branch nodes
        if branch_nodes:
            self._store(node.downstream_node)
            branch_sources = {}
            for _, node in branch_nodes.items():
                branch_sources.update(node.source_nodes())
            branch_sources = [s for s in sorted(branch_sources.items(), key=lambda x: x[1].steps_to_outlet(), reverse=True) if s[0] not in self.processed_nodes]
            first_branch_source_node = branch_sources[0][1]
            self.urbs_route(first_branch_source_node)
        else:
            if len(self.branch_stack) > 0:
                try:
                    while node.downstream_node.name == self.branch_stack[-1]:
                        self._get() # collect all the completed branches
                except IndexError:
                    pass
            if not node.downstream_node.name == self.outlet.name:
                self._route_thru(node.downstream_node)
            self.urbs_route(node.downstream_node)
            self.processed_nodes.append(node.name)

    def create_urbs_vector(self):
        self.source_nodes = self.outlet.source_nodes()
        first_node = list(self.source_nodes.items())[0][1]

        self.urbs_route(first_node)

        self.urbs_vector += "END OF CATCHMENT DATA."
        return self.urbs_vector


class Node(Base):
    def __init__():
        pass

    def next_upstream_nodes(self) -> dict:
        "returns the immediately neighbouring upstream nodes"
        return {name: node for name, node in self.model.nodes.items() if self.name == node.downstream_node.name}

    def all_upstream_nodes(self, nodes=None) -> dict:
        "returns all upstream nodes, via recursive search"
        if nodes is None:
            nodes = {}
        if not self.next_upstream_nodes():
            return nodes
        else:
            nodes.update(self.next_upstream_nodes())
            for name, node in self.next_upstream_nodes().items():
                nodes.update(node.all_upstream_nodes(nodes))
        return nodes

    def source_nodes(self) -> dict:
        "returns all head/source nodes upstream of current node"
        all_upstream_nodes = self.all_upstream_nodes()
        if not all_upstream_nodes:
            return {self.name: self}
        else:
            source_nodes = {name: node for name, node in all_upstream_nodes.items() if not node.next_upstream_nodes()}
            return dict(sorted(source_nodes.items(), key=lambda x: x[1].steps_to_outlet(), reverse=True))


class Outlet(Node):
    def __init__(
        self,
        model: Model,
        name: str='OUTLET',
    ):
        self.name = name
        self.model = model

        self.model.outlet = self


class Subcatchment(Node):
    def __init__(
        self,
        model: Model,
        name: str,
        downstream_node: Union[str, "Subcatchment", Outlet],
        parameters: dict,
        geometry: Union[None, str]=None,
        local: bool=False,
        total: bool=False,
        overwrite: bool=False,
    ):
        self.model = model
        self.name = name
        self.parameters = parameters
        self.geometry = geometry
        self.local = local
        self.total = total
        self.downstream_node = downstream_node

        self.validated = False

        # if type(downstream_node) == str:
        #     try:
        #         self.downstream_node = self.model.nodes[downstream_node]
        #     except KeyError as e:
        #         if downstream_node == self.model.outlet.name:
        #             self.downstream_node = self.model.outlet
        #         else:
        #             raise NodeError(f"Downstream node {downstream_node} does not exist in model.")
        # else:
        #     self.downstream_node = downstream_node

        if overwrite:
            self.model.nodes[self.name] = self
        else:
            if self.name in self.model.nodes.keys():
                raise NodeError(f"Node {self.name} already exists and overwrite is set to False.")
            else:
                self.model.nodes[self.name] = self

    def validate(self):
        if not type(self.downstream_node) in [Subcatchment, Outlet]:
            try:
                if self.downstream_node == self.model.outlet.name:
                    self.downstream_node = self.model.outlet
                else:
                    self.downstream_node = self.model.nodes[self.downstream_node]
                self.validated = True
            except KeyError as e:
                raise NodeError(f"Node {self.name}'s downstream node {self.downstream_node} does not exist in model.")
        

    def steps_to_outlet(self, count=None) -> int:
        "returns the number of steps between current node and outlet"
        if count is None:
            count = 0
        count += 1
        if self.downstream_node == self.model.outlet:
            return count
        else:
            return self.downstream_node.steps_to_outlet(count)

    def branch_nodes(self) -> dict:
        "returns the other nodes that also connect to the next downstream node"
        return {name: node for name, node in self.downstream_node.next_upstream_nodes().items() if name != self.name}


def route(name, catchment_layer, id_field, ds_id_field, l_ds_field, sc_ds_field, cs_field, local_field, total_field):
    MODEL = Model(name, uses=[])
    OUTLET = Outlet(model=MODEL, name='Outlet')


    catchment_features = catchment_layer.getFeatures()

    for catchment in catchment_features:
        Subcatchment(
            model=MODEL, 
            name=catchment[id_field], 
            downstream_node=catchment[ds_id_field], 
            parameters={
                'L_ds': catchment[l_ds_field], 
                'SC_ds': catchment[sc_ds_field], 
                # 'n_ds': catchment[n_ds_field],
                # 'CS': catchment[cs_field],
            },
            local=bool(catchment[local_field]),
            total=bool(catchment[total_field]),
        )

    MODEL.validate_subcatchments()
    MODEL.create_urbs_vector()

    return MODEL.urbs_vector


class CreateURBS(QgsProcessingAlgorithm):
    """
    Creates URBS vector and catchment files from a GIS file input.
    """

    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return CreateURBS()

    def name(self):
        return 'createurbs'

    def displayName(self):
        return self.tr('Create URBS model')

    def group(self):
        return self.tr('URBS scripts')

    def groupId(self):
        return 'urbsscripts'

    def shortHelpString(self):
        return self.tr("Creates URBS vector and catchment files from a GIS file input.")

    def initAlgorithm(self, config=None):

        self.addParameter(
            QgsProcessingParameterFeatureSource(
                'INPUT',
                self.tr('Catchment layer'),
                [QgsProcessing.TypeVectorPolygon]
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                name ='id_field',
                description = self.tr('Catchment ID field'),
                defaultValue = 'id',
                parentLayerParameterName = 'INPUT',
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                name ='ds_id_field',
                description = self.tr('Downstream catchment ID field'),
                defaultValue = 'ds_id',
                parentLayerParameterName = 'INPUT'
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                name ='area_field',
                description = self.tr('Catchment area field (sq. km)'),
                defaultValue = 'area_sqkm',
                parentLayerParameterName = 'INPUT'
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                name ='l_ds_field',
                description = self.tr('Downstream channel length field'),
                defaultValue = 'L',
                parentLayerParameterName = 'INPUT'
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                name ='sc_ds_field',
                description = self.tr('Downstream channel slope field'),
                defaultValue = 'SC',
                parentLayerParameterName = 'INPUT'
            )
        )

        # self.addParameter(
        #     QgsProcessingParameterField(
        #         name ='n_ds_field',
        #         description = self.tr('Downstream channel Manning\'s n field'),
        #         defaultValue = 'n',
        #         parentLayerParameterName = 'INPUT'
        #     )
        # )

        self.addParameter(
            QgsProcessingParameterField(
                name ='cs_field',
                description = self.tr('Catchment slope field'),
                defaultValue = 'CS',
                parentLayerParameterName = 'INPUT'
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                name ='local_field',
                description = self.tr('Local hydrograph field'),
                defaultValue = 'local',
                parentLayerParameterName = 'INPUT'
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                name ='total_field',
                description = self.tr('Total hydrograph field'),
                defaultValue = 'total',
                parentLayerParameterName = 'INPUT'
            )
        )

        self.addParameter(
            QgsProcessingParameterFolderDestination(
                name = 'OUTPUT',
                description = self.tr('Output folder')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """
        Here is where the processing itself takes place.
        """

        # Retrieve the feature source and sink. The 'dest_id' variable is used
        # to uniquely identify the feature sink, and must be included in the
        # dictionary returned by the processAlgorithm function.
        catchment_layer = self.parameterAsSource(
            parameters,
            'INPUT',
            context
        )

        id_field = self.parameterAsString(
            parameters,
            'id_field',
            context
        )

        ds_id_field = self.parameterAsString(
            parameters,
            'ds_id_field',
            context
        )

        area_field = self.parameterAsString(
            parameters,
            'area_field',
            context
        )

        l_ds_field = self.parameterAsString(
            parameters,
            'l_ds_field',
            context
        )

        sc_ds_field = self.parameterAsString(
            parameters,
            'sc_ds_field',
            context
        )

        # n_ds_field = self.parameterAsString(
        #     parameters,
        #     'n_ds_field',
        #     context
        # )

        cs_field = self.parameterAsString(
            parameters,
            'cs_field',
            context
        )

        local_field = self.parameterAsString(
            parameters,
            'local_field',
            context
        )

        total_field = self.parameterAsString(
            parameters,
            'total_field',
            context
        )

        OUTPUT = self.parameterAsString(
            parameters,
            'OUTPUT',
            context
        )

        name = catchment_layer.sourceName()
        urbs_vector = route(name, catchment_layer, id_field, ds_id_field, l_ds_field, sc_ds_field, cs_field, local_field, total_field)
        

        vector_file = Path(OUTPUT) / f'{name}.vec'
        with vector_file.open('w') as outfile:
            outfile.write(urbs_vector)

        catchment_file = Path(OUTPUT) / f'{name}.csv'
        fields = [field.name() for field in catchment_layer.fields()]
        with catchment_file.open('w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow('Index,Area,CS,UH,UM,UL,UD,UR,UF,I,X,Y'.split(','))
            for feature in catchment_layer.getFeatures():
                attributes = dict(zip([f.name() for f in feature.fields()], feature.attributes()))
                new_row = [
                    attributes[id_field], 
                    attributes[area_field], 
                    attributes[cs_field], 
                    attributes.get('UH', ''),
                    attributes.get('UM', ''),
                    attributes.get('UL', ''),
                    attributes.get('UD', ''),
                    attributes.get('UR', ''),
                    attributes.get('UF', ''),
                    attributes.get('I', ''),
                    feature.geometry().centroid().asPoint().x(),
                    feature.geometry().centroid().asPoint().y(),
                ]
                writer.writerow(new_row)

        return {'OUTPUT': OUTPUT}
