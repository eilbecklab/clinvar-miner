#!/usr/bin/python3

from xml.etree import ElementTree

def iri_to_mondo_xref(iri):
    if not iri or not iri.startswith('http://purl.obolibrary.org/obo/MONDO_'):
        return None
    return 'MONDO:' + iri[len('http://purl.obolibrary.org/obo/MONDO_'):]

class Mondo:
    xref_to_mondo_xref = {}
    name_to_mondo_xref = {}
    mondo_xref_to_name = {}
    parents_by_mondo_xref = {}

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
            mondo_xref = iri_to_mondo_xref(class_el.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about'])
            if not mondo_xref:
                continue

            label_el = class_el.find('./rdfs:label', ns)
            if label_el == None:
                continue
            condition_name = label_el.text

            self.name_to_mondo_xref[condition_name.lower()] = mondo_xref
            self.mondo_xref_to_name[mondo_xref] = condition_name

            for xref_el in class_el.findall('./oboInOwl:hasDbXref', ns):
                if not xref_el.text:
                    continue
                self.xref_to_mondo_xref[xref_el.text.upper()] = mondo_xref

            for synonym_el in class_el.findall('./oboInOwl:hasExactSynonym', ns):
                if synonym_el.text:
                    self.name_to_mondo_xref[synonym_el.text.lower()] = mondo_xref

            for subclassof_el in class_el.findall('./rdfs:subClassOf', ns):
                if '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource' in subclassof_el.attrib:
                    parent_xref = iri_to_mondo_xref(
                        subclassof_el.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']
                    )
                    if not parent_xref:
                        continue
                    if not mondo_xref in self.parents_by_mondo_xref:
                        self.parents_by_mondo_xref[mondo_xref] = []
                    self.parents_by_mondo_xref[mondo_xref].append(parent_xref)

            del class_el #conserve memory

    def ancestors(self, xref):
        parents = self.parents_by_mondo_xref.get(xref, [])
        ret = set(parents)
        for parent in parents:
            ret |= self.ancestors(parent)
        return ret

    def matches(self, condition_name, xrefs):
        ret = set()

        for xref in xrefs:
            if xref in self.xref_to_mondo_xref:
                ret.add(self.xref_to_mondo_xref[xref])

        condition_name = condition_name.lower()
        if condition_name in self.name_to_mondo_xref:
            ret.add(self.name_to_mondo_xref[condition_name])

        return ret

    def is_descendent_of(self, descendent_xref, ancestor_xref):
        if descendent_xref not in self.parents_by_mondo_xref:
            return False #term has no parents
        for parent_xref in self.parents_by_mondo_xref[descendent_xref]:
            if parent_xref == ancestor_xref or self.is_descendent_of(parent_xref, ancestor_xref):
                return True
        return False

    def most_specific_matches(self, condition_name, xrefs):
        matches = list(self.matches(condition_name, xrefs))
        i = 0
        while i < len(matches):
            j = i + 1
            while j < len(matches):
                if self.is_descendent_of(matches[j], matches[i]):
                    del matches[i]
                    i -= 1
                    break
                if self.is_descendent_of(matches[i], matches[j]):
                    del matches[j]
                    continue
                j += 1
            i += 1
        return set(matches)
