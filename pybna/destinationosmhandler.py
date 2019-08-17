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

    def __init__(self,tag_list):
        """
        args
        tag_list -- list of tags to compare features against
        """
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
            gj = mapping(pt)
            properties = dict()
            for pair in n.tags:
                if pair.k == "id":
                    pass
                else:
                    properties[pair.k] = pair.v
            gj["id"] = n.id
            gj["properties"] = properties
            self.nodes_json.append(gj)


    def way(self,w):
        if self._tag_matches(w.tags):
            try:
                wkb = wkbfab.create_linestring(w)
                ln = wkblib.loads(wkb,hex=True)
            except:
                return
            gj = mapping(ln)
            properties = dict()
            for pair in w.tags:
                if pair.k == "id":
                    pass
                else:
                    properties[pair.k] = pair.v
            gj["id"] = w.id
            gj["properties"] = properties
            self.ways_json.append(gj)


    def _tag_matches(self,tags):
        for tag in self.tag_list:
            if tag[0] in tags:
                if tags.get(tag[0]) == tag[1]:
                    return True
        return False
