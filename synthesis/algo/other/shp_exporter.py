
import geopandas as gpd
from shapely.geometry import LineString, Point

class SHPExporter:
    def __init__(self, df_source):
        self.df_source = df_source

        

    def __call__(self, df_source):
        pass

    def export_activity(self, output_shp):
        travels = gpd.GeoDataFrame()

        travels["geometry"] = self.df_source.geometry.apply(lambda point: Point(-point.x, -point.y))
        travels["activities"] = self.df_source.count.values

        travels[travels.travels].to_file(output_shp)
        print("Saved to:", output_shp)
        return
        


def export_activity(df_source, output_shp):
    exporter = SHPExporter(df_source)
    exporter.export_activity(output_shp)


exporter = None

def initializer(_exporter):
    global exporter
    exporter = _exporter
