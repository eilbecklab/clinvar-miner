#!/usr/bin/python3

import re
from xml.etree import ElementTree

class Mondo:
    xref_to_mondo_xref = {}
    name_to_mondo_xref = {}
    #mondo_xref_to_name = {}

    def __init__(self, path_to_mondo_owl = 'mondo.owl'):
        ns = {
            'oboInOwl': 'http://www.geneontology.org/formats/oboInOwl#',
            'owl': 'http://www.w3.org/2002/07/owl#',
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        }

        root = ElementTree.parse(path_to_mondo_owl)
        for class_el in root.findall('./owl:Class', ns):
            if '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about' not in class_el.attrib:
                continue
            mondo_iri = class_el.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
            if not mondo_iri or not mondo_iri.startswith('http://purl.obolibrary.org/obo/MONDO_'):
                continue
            mondo_xref = 'MONDO:' + mondo_iri[len('http://purl.obolibrary.org/obo/MONDO_'):]

            label_el = class_el.find('./rdfs:label', ns)
            if label_el == None:
                continue
            condition_name = label_el.text

            self.name_to_mondo_xref[condition_name.lower()] = mondo_xref
            #self.mondo_xref_to_name[mondo_xref] = condition_name

            for xref_el in class_el.findall('./oboInOwl:hasDbXref', ns):
                if not xref_el.text:
                    continue
                self.xref_to_mondo_xref[xref_el.text.upper()] = mondo_xref

            for synonym_el in class_el.findall('./oboInOwl:hasExactSynonym', ns):
                if synonym_el.text:
                    self.name_to_mondo_xref[synonym_el.text.lower()] = mondo_xref

            del class_el #conserve memory

    def exact_matches(self, condition_name, xrefs):
        ret = []

        for xref in xrefs:
            if xref in self.xref_to_mondo_xref:
                ret.append(self.xref_to_mondo_xref[xref])

        condition_name = condition_name.lower()
        if condition_name in self.name_to_mondo_xref:
            ret.append(self.name_to_mondo_xref[condition_name])

        return ret
