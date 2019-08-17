"""
Extract destinations from an OSM file using supplied tags
"""
import osmium
import re
import geojson
from shapely.geometry import shape, mapping
import shapely.wkb as wkblib
wkbfab = osmium.geom.WKBFactory()

class DestinationOSMHandler(osmium.SimpleHandler):

    def __init__(self,tag_list,bbox=None):
        """
        args
        tag_list -- list of tags to compare features against
        bbox -- bounding box to filter features
        """
        self.bbox = bbox
        self.nodes_json = list()
        self.ways_json = list()
        self.tag_list = list()
        # parse the tag list and save into tuples
        p = re.compile(r"""
            \[?     # an opening bracket
            ['"]    # either single or double quote
            (\w+)   # tag name
            ['"]    # either single or double quote
            \s*=\s* # equals (with optional whitespace)
            ['"]    # either single or double quote
            (\w+)   # tag value
            ['"]    # either single or double quote
            \]?     # a closing bracket
        """,re.VERBOSE)
        for tag_expression in tag_list:
            m = p.match(tag_expression)
            self.tag_list.append((m.group(1),m.group(2)))

        osmium.SimpleHandler.__init__(self)


    def node(self,n):
        if self._tag_matches(n.tags):
            wkb = wkbfab.create_point(n)
            pt = wkblib.loads(wkb,hex=True)
            if self.bbox is not None:
                if not pt.intersects(self.bbox):
                    return
            gj = mapping(pt)
            properties = dict()
            for k,v in n.tags.items():
                if k == "id":
                    pass
                else:
                    properties[k] = v
            gj["id"] = n.tags["osmid"]
            gj["properties"] = properties
            nodes_json.append(gj)


    def way(self,w):
        if self._tag_matches(w.tags):
            wkb = wkbfab.create_linestring(n)
            ln = wkblib.loads(wkb,hex=True)
            if self.bbox is not None:
                if not ln.intersects(self.bbox):
                    return
            gj = mapping(ln)
            properties = dict()
            for k,v in n.tags.items():
                if k == "id":
                    pass
                else:
                    properties[k] = v
            gj["id"] = n.tags["osmid"]
            gj["properties"] = properties
            ways_json.append(gj)


    def _tag_matches(self,tags):
        for tag in self.tag_list:
            if tag[0] in tags:
                if tags.get(tag[0]) == tag[1]:
                    return True
        return False
