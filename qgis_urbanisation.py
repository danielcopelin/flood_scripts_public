from PyQt5.QtCore import *

def urbanisation(catchments_layer, landuse_layer, i=True):
    
    intersection_layer = processing.run(
            "native:intersection",
            {
                'INPUT': catchments_layer,
                'OVERLAY': landuse_layer,
                'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
                # 'OUTPUT': output,
            }
        )['OUTPUT']
    print([f.name() for f in intersection_layer.fields()])
    
    landuse_categories = ['UH', 'UM', 'UL', 'UD', 'UR', 'UF'] + (['I'] if i else [])
    for landuse in landuse_categories:
        if not landuse in catchments_layer.fields().names():
                layer_provider = catchments_layer.dataProvider()
                layer_provider.addAttributes([QgsField(landuse,QVariant.Double)])
                catchments_layer.updateFields()

    
    with edit(catchments_layer):
        catchments_features = catchments_layer.getFeatures()
        for catchment in catchments_features:
            print(catchment['id'])
            landuse_areas = {}
            for landuse in landuse_categories:
                if landuse == 'I':
                    name = catchment['id']
                    intersection_features = [f for f in intersection_layer.getFeatures() if (f['id'] == name)]
                    intersection_area = sum([f.geometry().area()*f['I'] for f in intersection_features])
                    landuse_area = intersection_area / catchment.geometry().area()
                    catchment[landuse] = landuse_area
                    catchments_layer.updateFeature(catchment)
                else:
                    name = catchment['id']
                    intersection_features = [f for f in intersection_layer.getFeatures() if (f['id'] == name) and (f['URBS'] == landuse)]
                    intersection_area = sum([f.geometry().area() for f in intersection_features])
                    landuse_area = intersection_area / catchment.geometry().area()
                    catchment[landuse] = landuse_area
                    catchments_layer.updateFeature(catchment)    
    
                