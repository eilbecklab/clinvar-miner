#!/usr/bin/env python3

# Fibromyalgia - MONDO:0005546 (Disease involving pain)
# cannabinoid hyperemesis syndrome - MONDO:0100094 (Disease involving pain)
# Disease involving Pain - MONDO:0021668
# Achoo syndrome - MONDO:0007038
# Antipyrine metabolism- MONDO:0007139
# chorea gravidarum - MONDO:0001976
# choreatic disease - MONDO:0001595

import unittest
from mondo import Mondo

mon = Mondo()

mondo_xref_to_xref = {}
for key, value in mon.xref_to_mondo_xref.items():
    mondo_xref_to_xref[value] = key

class MondoTestCases(unittest.TestCase):
    def test_common_ancestor(self):
        result = mon.lowest_common_ancestor(['MONDO:0005546', 'MONDO:0100094'])
        self.assertEqual(result, 'MONDO:0002254')

    def test_is_descendent_of(self):
        #DIRECT CHILD. should be true: disease involving pain > fibromyalgia
        self.assertTrue(mon.is_descendent_of('MONDO:0100094', 'MONDO:0021668'))
        #2 GENS DOWN CHILD. should be true: disease involving pain > chronic pain > fibromyalgia
        self.assertTrue(mon.is_descendent_of('MONDO:0005546', 'MONDO:0021668'))
        #this is the direct child but reversed, should be false
        self.assertFalse(mon.is_descendent_of('MONDO:0021668','MONDO:0100094'))
        #these are offet siblings, should be false
        self.assertFalse(mon.is_descendent_of('MONDO:0005546','MONDO:0100094'))

    def test_matches(self):
        list1 = list(map(
            lambda mondo_xref: mondo_xref_to_xref[mondo_xref],
            ['MONDO:0007038','MONDO:0007139', 'MONDO:0001976', 'MONDO:0001595']
        ))
        self.assertEqual(
            mon.matches('blank', list1),
            set(['MONDO:0007038','MONDO:0007139', 'MONDO:0001976', 'MONDO:0001595'])
        )
        list2 = list(map(
            lambda mondo_xref: mondo_xref_to_xref[mondo_xref],
            ['MONDO:0007038','MONDO:0007139', 'MONDO:0001595']
        ))
        self.assertEqual(
            mon.matches('chorea gravidarum', list2),
            set(['MONDO:0007038','MONDO:0007139', 'MONDO:0001976', 'MONDO:0001595', 'MONDO:0001976'])
        )
        self.assertEqual(mon.matches('acute cervicitis', []), set(['MONDO:0001081']))
        self.assertEqual(mon.matches('nosocomial infection', []), set(['MONDO:0043544']))
        self.assertEqual(mon.matches('infectious otitis interna', []), set(['MONDO:0002812']))
        self.assertEqual(mon.matches('inflammatory disease', []), set(['MONDO:0021166']))

    def test_ancestors(self):
        self.assertTrue(set(['MONDO:0002254', 'MONDO:0005252']).issubset(mon.ancestors('MONDO:0044079')))
        self.assertTrue(set(['MONDO:0001214', 'MONDO:0024618', 'MONDO:0043541']).issubset(mon.ancestors('MONDO:0005634')))
        self.assertTrue(set(['MONDO:0021166', 'MONDO:0002356', 'MONDO:0043541']), mon.ancestors('MONDO:0004982'))

    # BLANK CONDITION NAMES

    def test_most_specific_matches_blank_name_single_ancestor(self):
        #test 1, 1 ancestor
        xrefs = list(map(
            lambda mondo_xref: mondo_xref_to_xref[mondo_xref],
            ['MONDO:0007038','MONDO:0007139', 'MONDO:0001976', 'MONDO:0001595']
        ))
        mons = mon.most_specific_matches('blank', xrefs)
        self.assertEqual(mons, set(['MONDO:0007038','MONDO:0007139', 'MONDO:0001976']))

    def test_most_specific_matches_blank_name_all_ancestors(self):
        #test 2, all ancestors (1 to 2 levels up)
        #Descendent: MONDO:0007325
        xrefs = list(map(
            lambda mondo_xref: mondo_xref_to_xref[mondo_xref],
            ['MONDO:0001595', 'MONDO:0007325', 'MONDO:0005395', 'MONDO:0003847']
        ))
        mons = mon.most_specific_matches('blank', xrefs)
        self.assertEqual(mons, set(['MONDO:0007325']))

    # NON-BLANK CONDITION NAMES

    def test_most_specific_matches_single_ancestor(self):
        #test 1, 1 ancestor
        xrefs = list(map(
            lambda mondo_xref: mondo_xref_to_xref[mondo_xref],
            ['MONDO:0007038','MONDO:0007139', 'MONDO:0001595']
        ))
        mons = mon.most_specific_matches('chorea gravidarum', xrefs)
        self.assertEqual(mons, set(['MONDO:0007038','MONDO:0007139', 'MONDO:0001976']))

    def test_most_specific_matches_all_ancestors(self):
        #test 2, all ancestors (1 to 2 levels up)
        #Descendent: MONDO:0007325
        xrefs = list(map(
            lambda mondo_xref: mondo_xref_to_xref[mondo_xref],
            ['MONDO:0001595', 'MONDO:0005395', 'MONDO:0003847']
        ))
        mons = mon.most_specific_matches('choreoathetosis, familial inverted', xrefs)
        self.assertEqual(mons, set(['MONDO:0007325']))

    def test_lowest_common_ancestor(self):
        #direct children
        self.assertEqual(
            'MONDO:0001214',
            mon.lowest_common_ancestor(['MONDO:0001224', 'MONDO:0001226', 'MONDO:0005634'])
        )
        #multiple children and indirect ancestors
        self.assertEqual(
            'MONDO:0020683',
            mon.lowest_common_ancestor(['MONDO:0001224', 'MONDO:0001226', 'MONDO:0005634', 'MONDO:0001817'])
        )
        #common ancestor in list
        self.assertEqual(
            'MONDO:0001214',
            mon.lowest_common_ancestor(['MONDO:0001224','MONDO:0001214'])
        )
        #offset deep children
        self.assertEqual(
            'MONDO:0000001',
            mon.lowest_common_ancestor(['MONDO:0043079','MONDO:0027751'])
        )

    def test_lowest_common_ancestor_extras(self):
        xrefs = [
            'HP:0001263',
            'HP:0001270',
            'HP:0001344',
            'HP:0002126',
            'HP:0002194',
            'HP:0002273',
            'HP:0002518',
            'HP:0007204',
            'HP:0008936',
            'HP:0009062',
            'HP:0100021',
            'MONDO:0000087',
            'MONDO:0006497',
            'UMLS:C0007789',
            'UMLS:C0266464',
            'UMLS:C0270790',
            'UMLS:C0557874',
            'UMLS:C1837658',
            'UMLS:C1853743',
            'UMLS:C1854301',
            'UMLS:C1854882',
            'UMLS:C2673431',
            'UMLS:C3806604',
            'UMLS:C4024923',
        ]
        self.assertEqual('MONDO:0002602', mon.lowest_common_ancestor(xrefs))

    def test_lowest_common_ancestor_extras2(self):
        xrefs = [
            'MONDO:0008810',
            'OMIM:207750',
            'OMIM:608083.0002',
            'OMIM:608083.0003',
            'OMIM:608083.0004',
            'OMIM:608083.0005',
            'OMIM:608083.0006',
            'OMIM:608083.0007',
            'OMIM:608083.0008',
            'OMIM:608083.0011',
            'OMIM:608083.0012',
            'ORPHANET:444490',
            'UMLS:C1720779',
        ]
        self.assertEqual('MONDO:0008810', mon.lowest_common_ancestor(xrefs))

unittest.main()
