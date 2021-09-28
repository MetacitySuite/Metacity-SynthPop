import osmium
import json
import shapely.wkb as wkblib
import pandas

pandas.options.mode.chained_assignment = None  # default='warn'
wkbfab = osmium.geom.WKBFactory()

def configure(context):
    context.config("data_path")
    context.config("osm_file")

class OSMHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.osm_data = []

    def tag_inventory(self, elem, elem_type):
        for tag in elem.tags:
            self.osm_data.append([elem_type, elem.id, tag.k,tag.v])

    def way(self, way):
        try:
            wkb = wkbfab.create_linestring(way)
            line = wkblib.loads(wkb, hex=True)
            for tag in way.tags:        	
                self.osm_data.append(['way', way.id, tag.k, tag.v, line.centroid.x, line.centroid.y])
        except Exception:
            pass
    def node(self, node):
        try:
            for tag in node.tags:
                if ((tag.k=='public_transport') | (tag.k=='amenity')):
                    self.osm_data.append(['node', node.id,tag.k, tag.v, node.location.lon, node.location.lat])
        except Exception:
            pass


def execute(context):
    handler = OSMHandler()

    # scan the input file and fills the handler list accordingly
    handler.apply_file(context.config("data_path")+context.config("osm_file"), locations=True)


    # transform the list into a pandas DataFrame
    data_colnames = ['type', 'id', 'tagkey', 'tagvalue', 'x', 'y']
    df_osm = pandas.DataFrame(handler.osm_data, columns=data_colnames)

    amenity_work_file = open(context.config("data_path") + "/zonetags/amenity-work.json")
    amenity_work = json.load(amenity_work_file)
    building_work_file = open(context.config("data_path") + "/zonetags/building-work.json")
    building_work = json.load(building_work_file)

    amenity_leisure_file = open(context.config("data_path") + "/zonetags/amenity-leisure.json")
    amenity_leisure = json.load(amenity_leisure_file)

    building_shop_file = open(context.config("data_path") + "/zonetags/building-shop.json")
    building_shop = json.load(building_shop_file)

    df_facilities = df_osm[ (df_osm["tagkey"]=='amenity') & (df_osm["tagvalue"].isin(amenity_work)) ]
    df_facilities["purpose"] = "work"

    df_work = df_osm[ (df_osm["tagkey"]=='building') & (df_osm["tagvalue"].isin(building_work)) ]
    df_work["purpose"] = "work"
    df_facilities = pandas.concat([df_facilities, df_work])

    df_leisure = df_osm[ ((df_osm["tagkey"]=='amenity') & (df_osm["tagvalue"].isin(amenity_leisure))) | (df_osm["tagkey"]=='leisure') ]
    df_leisure["purpose"] = "leisure work"
    df_facilities = pandas.concat([df_facilities, df_leisure])

    df_shop = df_osm[ ((df_osm["tagkey"]=='building') & (df_osm["tagvalue"].isin(building_shop))) | (df_osm["tagkey"]=='shop') ]
    df_shop["purpose"] = "shop work"
    df_facilities = pandas.concat([df_facilities, df_shop])

    df_home = df_osm[df_osm['type']=='way']
    df_home = df_home[df_home['tagkey']=='highway']
    df_home = df_home[(df_home['tagvalue']=='residential') | (df_home['tagvalue']=='living_street')]
    df_home["purpose"] = "home"
    df_facilities = pandas.concat([df_facilities, df_home])

    df_facilities.to_csv("facilities.csv")
    return df_facilities