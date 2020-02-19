#!/usr/bin/env python3

import json
import gzip
import re
import sqlite3
from asynchelper import promise, render_template_async
from cachelib import FileSystemCache
from cachelib import NullCache
from datetime import datetime
from db import DB
from flask import Flask
from flask import Response
from flask import abort
from flask import redirect
from flask import render_template
from flask import request
from json import JSONEncoder
from hashlib import sha256
from os import environ
from urllib.parse import urlparse, quote
from werkzeug.routing import BaseConverter

app = Flask(__name__)
ttl = float(environ.get('TTL', 0)) #zero means infinity to the FileSystemCache
cache = FileSystemCache('/tmp/clinvar-miner', threshold=1000000) if ttl >= 0 else NullCache()
cache.clear() #delete the cache when the webserver is restarted

app.jinja_env.trim_blocks = True
app.jinja_env.lstrip_blocks = True

clinvar_versions = DB().dates()

#it's necessary to double-escape slashes because WSGI decodes them before passing the URL to Flask
class SuperEscapedConverter(BaseConverter):
    @staticmethod
    def to_python(value):
        return value.replace('%2F', '/')

    @staticmethod
    def to_url(value):
        return quote(value).replace('/', '%252F')

app.url_map.converters['superescaped'] = SuperEscapedConverter

nonstandard_significance_term_map = dict(map(
    lambda line: line[0:-1].split('\t'),
    open('nonstandard_significance_terms.tsv')
))

@promise
def get_breakdown_by_condition_and_significance(total_variants_by_condition,
                                                total_variants_by_condition_and_significance):
    breakdown = {'data': {}, 'significances': set()}

    for row in total_variants_by_condition.result():
        condition_name = row['condition_name']
        count = row['count']

        breakdown['data'][condition_name] = {'total': count}

    for row in total_variants_by_condition_and_significance.result():
        condition_name = row['condition_name']
        significance = row['significance']
        count = row['count']

        breakdown['data'][condition_name][significance] = count
        breakdown['significances'].add(significance)

    breakdown['significances'] = sorted(breakdown['significances'], key=significance_rank)

    return breakdown

@promise
def get_breakdown_by_gene_and_significance(total_variants_by_gene,
                                           total_variants_by_gene_and_significance):
    breakdown = {'data': {}, 'significances': set()}

    for row in total_variants_by_gene.result():
        gene = row['gene']
        count = row['count']

        breakdown['data'][gene] = {'total': count}

    for row in total_variants_by_gene_and_significance.result():
        gene = row['gene']
        significance = row['significance']
        count = row['count']

        breakdown['data'][gene][significance] = count
        breakdown['significances'].add(significance)

    breakdown['significances'] = sorted(breakdown['significances'], key=significance_rank)

    return breakdown

@promise
def get_breakdown_by_submitter_and_significance(total_variants_by_submitter,
                                                total_variants_by_submitter_and_significance):
    breakdown = {'data': {}, 'significances': set()}

    for row in total_variants_by_submitter.result():
        submitter_id = row['submitter_id']
        submitter_name = row['submitter_name']
        count = row['count']

        breakdown['data'][submitter_id] = {
            'name': submitter_name,
            'counts': {'total': count}
        }

    for row in total_variants_by_submitter_and_significance.result():
        submitter_id = row['submitter_id']
        significance = row['significance']
        count = row['count']

        breakdown['data'][submitter_id]['counts'][significance] = count
        breakdown['significances'].add(significance)

    breakdown['significances'] = sorted(breakdown['significances'], key=significance_rank)

    return breakdown

@promise
def get_conflict_breakdown(total_variants_in_conflict_by_significance_and_significance):
    breakdown = {'data': {}, 'submitter1_significances': set(), 'submitter2_significances': set()}

    for row in total_variants_in_conflict_by_significance_and_significance.result():
        significance1 = row['significance1']
        significance2 = row['significance2']
        conflict_level = row['conflict_level']
        count = row['count']

        if not significance1 in breakdown['data']:
            breakdown['data'][significance1] = {}
        breakdown['data'][significance1][significance2] = {'level': conflict_level, 'count': count}

        breakdown['submitter1_significances'].add(significance1)
        breakdown['submitter2_significances'].add(significance2)

    breakdown['submitter1_significances'] = sorted(breakdown['submitter1_significances'], key=significance_rank)
    breakdown['submitter2_significances'] = sorted(breakdown['submitter2_significances'], key=significance_rank)

    return breakdown

@promise
def get_conflict_summary_by_condition(total_variants_by_condition, total_variants_potentially_in_conflict_by_condition,
                                      total_variants_in_conflict_by_condition,
                                      total_variants_in_conflict_by_condition_and_conflict_level):
    summary = {}

    for row in total_variants_in_conflict_by_condition.result():
        condition_name = row['condition_name']
        count = row['count']
        summary[condition_name] = {'any_conflict': count}

    for row in total_variants_potentially_in_conflict_by_condition.result():
        condition_name = row['condition_name']
        if condition_name in summary: #some conditions have no conflicts at all
            count = row['count']
            summary[condition_name][0] = count - summary[condition_name]['any_conflict']

    for row in total_variants_by_condition.result():
        condition_name = row['condition_name']
        if condition_name in summary: #some conditions have no conflicts at all
            count = row['count']
            summary[condition_name][-1] = count - summary[condition_name][0] - summary[condition_name]['any_conflict']

    for row in total_variants_in_conflict_by_condition_and_conflict_level.result():
        condition_name = row['condition_name']
        conflict_level = row['conflict_level']
        count = row['count']
        summary[condition_name][conflict_level] = count

    return summary

@promise
def get_conflict_summary_by_gene(total_variants_by_gene, total_variants_potentially_in_conflict_by_gene,
                                 total_variants_in_conflict_by_gene,
                                 total_variants_in_conflict_by_gene_and_conflict_level):
    summary = {}

    for row in total_variants_in_conflict_by_gene.result():
        gene = row['gene']
        count = row['count']
        summary[gene] = {'any_conflict': count}

    for row in total_variants_potentially_in_conflict_by_gene.result():
        gene = row['gene']
        if gene in summary: #some genes have no conflicts at all
            count = row['count']
            summary[gene][0] = count - summary[gene]['any_conflict']

    for row in total_variants_by_gene.result():
        gene = row['gene']
        if gene in summary: #some genes have no conflicts at all
            count = row['count']
            summary[gene][-1] = count - summary[gene][0] - summary[gene]['any_conflict']

    for row in total_variants_in_conflict_by_gene_and_conflict_level.result():
        gene = row['gene']
        conflict_level = row['conflict_level']
        count = row['count']
        summary[gene][conflict_level] = count

    return summary

@promise
def get_conflict_summary_by_submitter(total_variants_by_submitter, total_variants_potentially_in_conflict_by_submitter,
                                      total_variants_in_conflict_by_submitter,
                                      total_variants_in_conflict_by_submitter_and_conflict_level):
    summary = {}

    for row in total_variants_in_conflict_by_submitter.result():
        submitter_id = row['submitter_id']
        submitter_name = row['submitter_name']
        count = row['count']
        summary[submitter_id] = {'name': submitter_name, 'any_conflict': count}

    for row in total_variants_potentially_in_conflict_by_submitter.result():
        submitter_id = row['submitter_id']
        if submitter_id in summary: #some submitters have no conflicts with anyone
            count = row['count']
            summary[submitter_id][0] = count - summary[submitter_id]['any_conflict']

    for row in total_variants_by_submitter.result():
        submitter_id = row['submitter_id']
        if submitter_id in summary: #some submitters have no conflicts with anyone
            count = row['count']
            summary[submitter_id][-1] = count - summary[submitter_id][0] - summary[submitter_id]['any_conflict']

    for row in total_variants_in_conflict_by_submitter_and_conflict_level.result():
        submitter_id = row['submitter_id']
        conflict_level = row['conflict_level']
        count = row['count']
        summary[submitter_id][conflict_level] = count

    return summary

@promise
def get_conflict_overview(total_variants_in_conflict_by_conflict_level):
    overview = {}

    for row in total_variants_in_conflict_by_conflict_level.result():
        conflict_level = row['conflict_level']
        count = row['count']
        overview[conflict_level] = count

    return overview

@promise
def get_graph_data_for_submissions_by_normalized_method(total_submissions_by_normalized_method_over_time):
    rows = total_submissions_by_normalized_method_over_time.result()

    dates = set()
    methods = set()
    date_method_pairs = set()
    for row in rows:
        date = row['date']
        method = row['normalized_method']
        count = row['count']

        dates.add(date)
        methods.add(method)
        date_method_pairs.add((date, method))

    for date in dates:
        for method in methods:
            if not (date, method) in date_method_pairs:
                rows.append({'date': date, 'normalized_method': method, 'count': 0})

    rows.sort(key=lambda row: row['date'])

    return rows

@promise
def get_significance_overview(total_variants_by_significance):
    overview = {
        'pathogenic': 0,
        'likely pathogenic': 0,
        'uncertain significance': 0,
        'likely benign': 0,
        'benign': 0,
    }

    for row in total_variants_by_significance.result():
        significance = row['significance']
        count = row['count']
        overview[significance] = count

    overview = dict(sorted(overview.items(), key=lambda pair: significance_rank(pair[0])))

    return overview

def int_arg(name, default = -1):
    arg = request.args.get(name)
    try:
        return int(arg) if arg else default
    except ValueError:
        abort(400)

def list_arg(name):
    return list(request.args.getlist(name)) or None

def validate_args(args):
    db = DB()
    bad = (
        (args.get('min_stars1') and (args['min_stars1'] < -1 or args['min_stars1'] > 4)) or
        (args.get('normalized_method1') and not db.is_method(args['normalized_method1'])) or
        (args.get('normalized_method2') and not db.is_method(args['normalized_method2'])) or
        (args.get('min_conflict_level') and (args['min_conflict_level'] < -1 or args['min_conflict_level'] > 5)) or
        (args.get('gene_type') and (args['gene_type'] < -1 or args['gene_type'] > 3)) or
        (args.get('date') and not db.is_date(args['date']))
    )
    if bad:
        abort(400)

def significance_rank(significance):
    significance_ranks = [
        'pathogenic',
        'likely pathogenic',
        'uncertain significance',
        'likely benign',
        'benign',
        'other',
        'not provided',
    ]
    try:
        rank = significance_ranks.index(nonstandard_significance_term_map.get(significance, significance))
    except ValueError:
        rank = len(significance_ranks) - 2.5 #insert after everything but "other" and "not provided"
    #sort alphabetically to be consistent if there are two or more unranked significance terms
    return rank, significance

@app.template_filter('conflictlevel')
def conflict_level_string(conflict_level):
    return [
        'no conflict',
        'synonymous conflict',
        'confidence conflict',
        'benign vs uncertain conflict',
        'category conflict',
        'clinically significant conflict',
    ][conflict_level]

@app.template_filter('extrabreaks')
def extra_breaks(text):
    #provide additional line breaking opportunities
    ret = (text
        .replace('(', '<wbr/>(')
        .replace(')', ')<wbr/>')
        .replace(',', ',<wbr/>')
        .replace('.', '.<wbr/>')
        .replace(':', '<wbr/>:<wbr/>')
        .replace('-', '-<wbr/>')
    )
    ret = re.sub(r'([a-z])([A-Z])', r'\1<wbr/>\2', ret) #camelCase
    return ret

@app.template_filter('json')
def json_filter(obj):
    class SQLiteJSONEncoder(JSONEncoder):
        def default(self, obj):
            if isinstance(obj, sqlite3.Row):
                return dict(obj)
            return obj
    return json.dumps(obj, cls=SQLiteJSONEncoder)

@app.template_filter('genelink')
def gene_link(gene):
    return '<a class="external" href="https://ghr.nlm.nih.gov/gene/' + gene + '">' + gene + '</a>' if gene else ''

@app.template_filter('rcvlink')
def rcv_link(rcv):
    rcv = 'RCV' + str(rcv).zfill(9)
    return '<a class="external" href="https://www.ncbi.nlm.nih.gov/clinvar/' + rcv + '/">' + rcv + '</a>'

@app.template_filter('scv')
def scv_pretty(scv):
    return 'SCV' + str(scv).zfill(9)

@app.template_filter('tabledownloadlink')
def select_link(element_id):
    return '<a href="javascript:downloadTableAsCsv(\'' + element_id + '\')">Download table as spreadsheet</a>'

@app.template_filter('superescaped')
def super_escape(path):
    return SuperEscapedConverter.to_url(path)

@app.context_processor
def template_functions():
    def condition_tagline(condition_xrefs):
        tagline = ''
        for xref in condition_xrefs:
            condition_db, sep, condition_id = xref.partition(':')
            #put links to the most popular databases first
            if condition_db == 'MONDO':
                tagline += '<li><a class="external" href="https://monarchinitiative.org/disease/' + xref + '">'
                tagline += xref + '</a></li>'
            elif condition_db == 'UMLS':
                tagline += '<li><a class="external" href="https://www.ncbi.nlm.nih.gov/medgen/' + condition_id + '">'
                tagline += xref + '</a></li>'
            elif condition_db == 'OMIM':
                tagline += '<li><a class="external" href="https://www.omim.org/'
                tagline += 'phenotypicSeries/' if condition_id.startswith('PS') else 'entry/'
                tagline += condition_id.replace('.', '#') + '">'
                tagline += xref + '</a></li>'
            elif condition_db == 'ORPHANET':
                tagline += '<li><a class="external" href="https://www.orpha.net/consor/cgi-bin/OC_Exp.php?Expert=' + condition_id + '">'
                tagline += xref + '</a></li>'
            elif condition_db == 'HP':
                tagline += '<li><a class="external" href="http://compbio.charite.de/hpoweb/showterm?id=' + xref + '">'
                tagline += xref + '</a></li>'
            elif condition_db == 'SNOMEDCT_US':
                tagline += '<li><a class="external" href="http://browser.ihtsdotools.org/?perspective=full&conceptId1=' + condition_id + '">'
                tagline += xref + '</a></li>'
            elif condition_db == 'MESH':
                tagline += '<li><a class="external" href="https://www.ncbi.nlm.nih.gov/mesh/?term=' + condition_id + '">'
                tagline += xref + '</a></li>'
            elif condition_db == 'UNIPROT':
                tagline += '<li><a class="external" href="https://www.uniprot.org/'
                tagline += 'keywords/' if condition_id.startswith('KW') else 'uniprot/'
                tagline += condition_id + '">'
                tagline += xref + '</a></li>'
            elif condition_db == 'EFO':
                tagline += '<li><a class="external" href="https://www.ebi.ac.uk/ols/ontologies/efo/terms?iri=http%3A%2F%2Fwww.ebi.ac.uk%2Fefo%2FEFO_' + condition_id + '">'
                tagline += xref + '</a></li>'
        if tagline:
            tagline = '<div class="tagline">Coded as: <ul>' + tagline + '</ul></div>'
        return tagline

    def dates():
        return clinvar_versions

    def gene_tagline(gene_info, link_base):
        if not gene_info['see_also']:
            return ''
        tagline = '<div class="tagline">See also: <ul>'
        for gene in gene_info['see_also']:
            href = link_base + '/' + super_escape(gene) + query_suffix(request, 'min_conflict_level', 'original_terms', 'gene_type', 'original_genes')
            tagline += '<li><a href="' + href + '">' + gene + '</a></li>'
        tagline += '</ul></div>'
        return tagline

    def mondo_condition_tagline(clinvar_names):
        tagline = '<div class="tagline">Included ClinVar conditions (' + str(len(clinvar_names)) + '):<ul>'
        for name in clinvar_names:
            href = 'variants-by-condition/' + super_escape(name)
            tagline += '<li><a href="' + href + '">' + name + '</a></li>'
        tagline += '</ul></div>'
        return tagline

    def h2(text):
        section_id = text.lower().replace(' ', '-')
        return '<h2 id="' + section_id + '">' + text + ' <a class="internal" href="' + request.url + '#' + section_id + '">#</a></h2>'

    def table_search_box(element_id, tag = 'form'):
        return '''
            <''' + tag + ''' class="search">
                <input
                    autocomplete="off"
                    class="search-box"
                    disabled="disabled"
                    name=""
                    oninput="filterTable(''' + "'" + element_id + "'," + '''this.value)"
                    onkeypress="if (event.key == 'Enter') event.preventDefault()"
                    placeholder="Please wait..."
                    type="text"
                    value=""
                />
                <input disabled="disabled" type="submit" value=" "/>
            </''' + tag + '''>
        '''

    def submitter_link(submitter_id, submitter_name):
        if submitter_id == 0:
            return submitter_name
        return '<a class="external" href="https://www.ncbi.nlm.nih.gov/clinvar/submitters/' + str(submitter_id) + '/">' + extra_breaks(submitter_name) + '</a>'

    def submitter_tagline(submitter_info, submitter_primary_method):
        tagline = '<div class="tagline">'
        if 'country_name' in submitter_info:
            tagline += 'Location: ' + (submitter_info['country_name'] or 'unspecified') + ' &mdash; '
        tagline += 'Primary collection method: ' + submitter_primary_method
        tagline += '</div>'
        return tagline

    def query_suffix(*extra_allowed_params):
        if not request.args:
            return ''

        always_allowed_params = [
            'min_stars1',
            'min_stars2',
            'method1',
            'method2'
        ]

        args = []
        for key in request.args:
            value = request.args.get(key)
            if (key in always_allowed_params or key in extra_allowed_params) and value:
                args.append(quote(key, safe='') + '=' + quote(request.args[key], safe=''))
        return '?' + '&'.join(args) if args else ''

    def variant_link(variant_id, variant_name, rsid):
        if variant_id == 0:
            return variant_name

        ret = '<a class="external" href="https://www.ncbi.nlm.nih.gov/clinvar/variation/' + str(variant_id) + '/">' + extra_breaks(variant_name) + '</a>'

        if rsid:
            ret += ' (<a class="external" href="https://www.ncbi.nlm.nih.gov/SNP/snp_ref.cgi?rs=' + rsid + '">' + rsid + '</a>)'

        return ret

    return {
        'condition_tagline': condition_tagline,
        'dates': dates,
        'gene_tagline': gene_tagline,
        'mondo_condition_tagline': mondo_condition_tagline,
        'h2': h2,
        'submitter_link': submitter_link,
        'submitter_tagline': submitter_tagline,
        'query_suffix': query_suffix,
        'table_search_box': table_search_box,
        'variant_link': variant_link,
    }

@app.before_request
def cache_get():
    response = cache.get(request.url)
    if not response or 'gzip' not in request.accept_encodings:
        return None

    server_etag = response.get_etag()[0]
    client_etags = request.if_none_match
    if server_etag in client_etags:
        return Response(status=304, headers={'ETag': server_etag})

    return response

@app.after_request
def cache_set(response):
    if (ttl >= 0 and not cache.has(request.url) and response.status_code == 200 and not response.direct_passthrough and
            'gzip' in request.accept_encodings):
        response.set_data(gzip.compress(response.get_data()))
        response.set_etag(sha256(response.get_data()).hexdigest())
        response.headers.set('Content-Encoding', 'gzip')
        response.freeze()
        cache.set(request.url, response, timeout=ttl)
    return response

@app.route('/variants-in-conflict-by-condition')
@app.route('/variants-in-conflict-by-condition/<superescaped:condition_name>')
def variants_in_conflict_by_condition(condition_name = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'min_stars2': int_arg('min_stars2'),
        'normalized_method2': request.args.get('method2'),
        'original_genes': request.args.get('original_genes'),
        'date': request.args.get('date'),
    }
    validate_args(args)
    min_conflict_level = max(1, int_arg('min_conflict_level'))

    if condition_name == None:
        args['condition1_name'] = list_arg('conditions')
        return render_template_async(
            'variants-in-conflict-by-condition.html',
            min_conflict_level=min_conflict_level,
            overview=get_conflict_overview(
                DB().total_variants_in_conflict_by_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
            total_variants=DB().total_variants(
                **args
            ),
            total_variants_potentially_in_conflict=DB().total_variants(
                min_conflict_level=0,
                **args
            ),
            total_variants_in_conflict=DB().total_variants(
                min_conflict_level=min_conflict_level,
                **args
            ),
            summary=get_conflict_summary_by_condition(
                DB().total_variants_by_condition(
                    **args
                ),
                DB().total_variants_by_condition(
                    min_conflict_level=0,
                    **args
                ),
                DB().total_variants_by_condition(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
                DB().total_variants_in_conflict_by_condition_and_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
        )

    if not DB().is_condition_name(condition_name):
        abort(404)
    args['condition1_name'] = condition_name
    args['original_terms'] = request.args.get('original_terms')

    return render_template_async(
        'variants-in-conflict-by-condition--condition.html',
        condition_name=condition_name,
        condition_xrefs=DB().condition_xrefs(condition_name, args['date']),
        min_conflict_level=min_conflict_level,
        overview=get_conflict_overview(
            DB().total_variants_in_conflict_by_conflict_level(
                min_conflict_level=min_conflict_level,
                **args
            ),
        ),
        total_variants=DB().total_variants(
            **args
        ),
        total_variants_potentially_in_conflict=DB().total_variants(
            min_conflict_level=0,
            **args
        ),
        breakdown=get_conflict_breakdown(
            DB().total_variants_in_conflict_by_significance_and_significance(
                min_conflict_level=min_conflict_level,
                **args
            )
        ),
        summary=get_conflict_summary_by_condition(
            DB().total_variants_by_condition(
                **args
            ),
            DB().total_variants_by_condition(
                min_conflict_level=0,
                **args
            ),
            DB().total_variants_by_condition(
                min_conflict_level=min_conflict_level,
                **args
            ),
            DB().total_variants_in_conflict_by_condition_and_conflict_level(
                min_conflict_level=min_conflict_level,
                **args
            ),
        ),
        variants=DB().variants(
            min_conflict_level=min_conflict_level,
            **args
        ),
    )

@app.route('/variants-in-conflict-by-gene')
@app.route('/variants-in-conflict-by-gene/<superescaped:gene>')
@app.route('/variants-in-conflict-by-gene/<superescaped:gene>/<superescaped:significance1>/<superescaped:significance2>')
def variants_in_conflict_by_gene(gene = None, significance1 = None, significance2 = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'min_stars2': int_arg('min_stars2'),
        'normalized_method2': request.args.get('method2'),
        'gene_type': int_arg('gene_type'),
        'original_genes': request.args.get('original_genes'),
        'date': request.args.get('date'),
    }
    validate_args(args)
    min_conflict_level = max(1, int_arg('min_conflict_level'))

    if not gene:
        args['gene'] = list_arg('genes')
        return render_template_async(
            'variants-in-conflict-by-gene.html',
            min_conflict_level=min_conflict_level,
            overview=get_conflict_overview(
                DB().total_variants_in_conflict_by_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
            total_variants=DB().total_variants(
                **args
            ),
            total_variants_potentially_in_conflict=DB().total_variants(
                min_conflict_level=0,
                **args
            ),
            total_variants_in_conflict=DB().total_variants(
                min_conflict_level=min_conflict_level,
                **args
            ),
            summary=get_conflict_summary_by_gene(
                DB().total_variants_by_gene(
                    **args
                ),
                DB().total_variants_by_gene(
                    min_conflict_level=0,
                    **args
                ),
                DB().total_variants_by_gene(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
                DB().total_variants_in_conflict_by_gene_and_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
        )

    if gene == 'intergenic':
        gene = ''
    elif not DB().is_gene(gene):
        abort(404)
    gene_info = DB().gene_info(gene, args['original_genes'], date=args['date'])
    args['gene'] = gene
    args['original_terms'] = request.args.get('original_terms')

    if not significance1:
        return render_template_async(
            'variants-in-conflict-by-gene--gene.html',
            gene_info=gene_info,
            min_conflict_level=min_conflict_level,
            overview=get_conflict_overview(
                DB().total_variants_in_conflict_by_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
            total_variants=DB().total_variants(
                **args
            ),
            total_variants_potentially_in_conflict=DB().total_variants(
                min_conflict_level=0,
                **args
            ),
            breakdown=get_conflict_breakdown(
                DB().total_variants_in_conflict_by_significance_and_significance(
                    min_conflict_level=min_conflict_level,
                    **args
                )
            ),
            variants=DB().variants(
                min_conflict_level=min_conflict_level,
                **args
            ),
        )
    if not DB().is_significance(significance1) or not DB().is_significance(significance2):
        abort(404)

    return render_template_async(
        'variants-in-conflict-by-gene--2significances.html',
        gene_info=gene_info,
        significance1=significance1,
        significance2=significance2,
        variants=DB().variants(
            significance1=significance1,
            significance2=significance2,
            min_conflict_level=1,
            **args
        ),
    )

@app.route('/variants-in-conflict-by-significance')
@app.route('/variants-in-conflict-by-significance/<superescaped:significance1>/<superescaped:significance2>')
def variants_in_conflict_by_significance(significance1 = None, significance2 = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'min_stars2': int_arg('min_stars2'),
        'normalized_method2': request.args.get('method2'),
        'original_terms': request.args.get('original_terms'),
        'date': request.args.get('date'),
    }
    validate_args(args)
    min_conflict_level = max(1, int_arg('min_conflict_level'))

    if not significance2:
        return render_template_async(
            'variants-in-conflict-by-significance.html',
            min_conflict_level=min_conflict_level,
            overview=get_conflict_overview(
                DB().total_variants_in_conflict_by_conflict_level(
                    **args
                ),
            ),
            total_variants=DB().total_variants(
                **args
            ),
            total_variants_potentially_in_conflict=DB().total_variants(
                min_conflict_level=0,
                **args
            ),
            total_variants_in_conflict=DB().total_variants(
                min_conflict_level=min_conflict_level,
                **args
            ),
            breakdown=get_conflict_breakdown(
                DB().total_variants_in_conflict_by_significance_and_significance(
                    min_conflict_level=min_conflict_level,
                    **args
                )
            )
        )

    if not DB().is_significance(significance1) or not DB().is_significance(significance2):
        abort(404)

    return render_template_async(
        'variants-in-conflict-by-significance--2significances.html',
        significance1=significance1,
        significance2=significance2,
        variants=DB().variants(
            significance1=significance1,
            significance2=significance2,
            min_conflict_level=1,
            **args
        ),
    )

@app.route('/variants-in-conflict-by-submitter')
@app.route('/variants-in-conflict-by-submitter/<int:submitter1_id>')
@app.route('/variants-in-conflict-by-submitter/<int:submitter1_id>/<int:submitter2_id>')
@app.route('/variants-in-conflict-by-submitter/<int:submitter1_id>/<int:submitter2_id>/<superescaped:significance1>/<superescaped:significance2>')
def variants_in_conflict_by_submitter(submitter1_id = None, submitter2_id = None, significance1 = None, significance2 = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'min_stars2': int_arg('min_stars2'),
        'normalized_method2': request.args.get('method2'),
        'gene_type': int_arg('gene_type'),
        'original_genes': request.args.get('original_genes'),
        'date': request.args.get('date'),
    }
    validate_args(args)
    min_conflict_level = max(1, int_arg('min_conflict_level'))

    if submitter1_id == None:
        args['submitter1_id'] = list_arg('submitters')
        return render_template_async(
            'variants-in-conflict-by-submitter.html',
            min_conflict_level=min_conflict_level,
            overview=get_conflict_overview(
                DB().total_variants_in_conflict_by_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
            total_variants=DB().total_variants(
                **args
            ),
            total_variants_potentially_in_conflict=DB().total_variants(
                min_conflict_level=0,
                **args
            ),
            total_variants_in_conflict=DB().total_variants(
                min_conflict_level=min_conflict_level,
                **args
            ),
            summary=get_conflict_summary_by_submitter(
                DB().total_variants_by_submitter(
                    **args
                ),
                DB().total_variants_by_submitter(
                    min_conflict_level=0,
                    **args
                ),
                DB().total_variants_by_submitter(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
                DB().total_variants_in_conflict_by_submitter_and_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
        )

    if not DB().is_submitter_id(submitter1_id):
        abort(404)
    submitter1_info = DB().submitter_info(submitter1_id, date=args['date'])
    args['submitter1_id'] = submitter1_id
    args['original_terms'] = request.args.get('original_terms')

    if submitter2_id == None:
        return render_template_async(
            'variants-in-conflict-by-submitter--1submitter.html',
            submitter1_info=submitter1_info,
            submitter1_primary_method=DB().submitter_primary_method(submitter1_id, args['date']),
            min_conflict_level=min_conflict_level,
            overview=get_conflict_overview(
                DB().total_variants_in_conflict_by_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
            total_variants=DB().total_variants(
                **args
            ),
            total_variants_potentially_in_conflict=DB().total_variants(
                min_conflict_level=0,
                **args
            ),
            total_variants_in_conflict=DB().total_variants(
                min_conflict_level=min_conflict_level,
                **args
            ),
            summary=get_conflict_summary_by_submitter(
                DB().total_variants_by_submitter(
                    **args
                ),
                DB().total_variants_by_submitter(
                    min_conflict_level=0,
                    **args
                ),
                DB().total_variants_by_submitter(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
                DB().total_variants_in_conflict_by_submitter_and_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
            breakdown=get_conflict_breakdown(
                DB().total_variants_in_conflict_by_significance_and_significance(
                    min_conflict_level=min_conflict_level,
                    **args
                )
            ),
            variants=DB().variants(
                min_conflict_level=min_conflict_level,
                **args
            ),
        )

    if submitter2_id == 0:
        submitter2_info = {'id': 0, 'name': 'any submitter'}
    else:
        if not DB().is_submitter_id(submitter2_id):
            abort(404)
        submitter2_info = DB().submitter_info(submitter2_id, args['date'])
    args['submitter2_id'] = submitter2_id

    if not significance1:
        return render_template_async(
            'variants-in-conflict-by-submitter--2submitters.html',
            submitter1_info=submitter1_info,
            submitter2_info=submitter2_info,
            min_conflict_level=min_conflict_level,
            overview=get_conflict_overview(
                DB().total_variants_in_conflict_by_conflict_level(
                    min_conflict_level=min_conflict_level,
                    **args
                ),
            ),
            total_variants=DB().total_variants(
                **args
            ),
            total_variants_potentially_in_conflict=DB().total_variants(
                min_conflict_level=0,
                **args
            ),
            breakdown=get_conflict_breakdown(
                DB().total_variants_in_conflict_by_significance_and_significance(
                    min_conflict_level=min_conflict_level,
                    **args
                )
            ),
            variants=DB().variants(
                min_conflict_level=min_conflict_level,
                **args
            ),
        )

    if not DB().is_significance(significance1) or not DB().is_significance(significance2):
        abort(404)

    return render_template_async(
        'variants-in-conflict-by-submitter--2significances.html',
        submitter1_info=submitter1_info,
        submitter2_info=submitter2_info,
        significance1=significance1,
        significance2=significance2,
        variants=DB().variants(
            significance1=significance1,
            significance2=significance2,
            min_conflict_level=1,
            **args
        ),
    )

@app.route('/')
def index():
    return render_template_async(
        'index.html',
        max_date=datetime.strptime(DB().max_date(), '%Y-%m-%d'),
        total_submissions=DB().total_submissions(),
        total_variants=DB().total_variants(),
    )

@app.route('/robots.txt')
def robots_txt():
    return app.send_static_file('robots.txt')

@app.route('/search')
def search():
    db = DB()
    query = request.args.get('q')

    #blank
    if not query:
        return redirect(request.url_root)

    #rsID, RCV, SCV
    variant_name = (
        db.variant_name_from_rsid(query.lower()) or
        db.variant_name_from_rcv(query.upper()) or
        db.variant_name_from_scv(query.upper())
    )
    if variant_name:
        return redirect(request.script_root + '/submissions-by-variant/' + super_escape(variant_name))

    #an rsID always uniquely identifies a gene even if it doesn't uniquely identify a variant
    gene = db.gene_from_rsid(query.lower())
    if gene != None:
        if gene == '':
            return redirect(request.script_root + '/variants-by-gene/intergenic')
        else:
            return redirect(request.script_root + '/variants-by-gene/' + super_escape(gene))

    #gene
    if db.is_gene(query.upper()):
        return redirect(request.script_root + '/variants-by-gene/' + super_escape(query.upper()))
    if query.lower() == 'intergenic':
        return redirect(request.script_root + '/variants-by-gene/intergenic')

    #HGVS
    if db.is_variant_name(query):
        return redirect(request.script_root + '/submissions-by-variant/' + super_escape(query))

    #condition
    if db.is_condition_name(query):
        return redirect(request.script_root + '/variants-by-condition/' + super_escape(query))

    #submitter
    submitter_id = db.submitter_id_from_name(query)
    if submitter_id:
        return redirect(request.script_root + '/variants-by-submitter/' + str(submitter_id))

    keywords = urlparse(request.url_root)[1] + ' ' + query
    return redirect('https://www.google.com/search?q=site:' + quote(keywords, safe=''))

@app.route('/significance-terms')
def significance_terms():
    return render_template_async(
        'significance-terms.html',
        total_significance_terms_over_time=DB().total_significance_terms_over_time(),
        significance_term_info=DB().significance_term_info(),
        max_date=DB().max_date(),
    )

@app.route('/submissions-by-variant/<superescaped:variant_name>')
def submissions_by_variant(variant_name):
    args = {
        'min_stars': int_arg('min_stars1'),
        'normalized_method': request.args.get('method1'),
        'min_conflict_level': int_arg('min_conflict_level'),
        'variant_name': variant_name,
        'date': request.args.get('date'),
    }
    validate_args(args)

    if not DB().is_variant_name(variant_name):
        abort(404)
    variant_info = DB().variant_info(variant_name, args['date'])

    return render_template_async(
        'submissions-by-variant--variant.html',
        variant_info=variant_info,
        submissions=DB().submissions(**args),
    )

@app.route('/total-submissions-by-country')
@app.route('/total-submissions-by-country/', defaults={'country_code': ''})
@app.route('/total-submissions-by-country/<country_code>')
def total_submissions_by_country(country_code = None):
    args = {
        'min_stars': int_arg('min_stars1'),
        'normalized_method': request.args.get('method1'),
        'min_conflict_level': int_arg('min_conflict_level'),
        'date': request.args.get('date'),
    }
    validate_args(args)

    if country_code == None:
        return render_template_async(
            'total-submissions-by-country.html',
            total_submissions_by_country=DB().total_submissions_by_country(**args),
            total_submissions=DB().total_submissions(**args),
        )

    country_name = DB().country_name(country_code)
    if country_name == None:
        abort(404)

    return render_template_async(
        'total-submissions-by-country--country.html',
        country_name=country_name,
        total_submissions_by_submitter=DB().total_submissions_by_submitter(
            country_code=country_code,
            **args
        ),
        total_submissions=DB().total_submissions(
            country_code=country_code,
            **args
        ),
    )

@app.route('/total-submissions-by-method')
def total_submissions_by_method():
    args = {
        'min_stars': int_arg('min_stars1'),
        'min_conflict_level': int_arg('min_conflict_level'),
        'date': request.args.get('date'),
    }
    validate_args(args)

    return render_template_async(
        'total-submissions-by-method.html',
        total_submissions_by_normalized_method_over_time=get_graph_data_for_submissions_by_normalized_method(
            DB().total_submissions_by_normalized_method_over_time(**args)
        ),
        total_submissions_by_method=DB().total_submissions_by_method(**args),
        total_submissions=DB().total_submissions(**args),
    )

@app.route('/variants-by-condition')
@app.route('/variants-by-condition/<superescaped:condition_name>')
@app.route('/variants-by-condition/<superescaped:condition_name>/significance/any', defaults={'significance': ''})
@app.route('/variants-by-condition/<superescaped:condition_name>/significance/<superescaped:significance>')
@app.route('/variants-by-condition/<superescaped:condition_name>/gene/<superescaped:gene>', defaults={'significance': ''})
@app.route('/variants-by-condition/<superescaped:condition_name>/gene/<superescaped:gene>/<superescaped:significance>')
@app.route('/variants-by-condition/<superescaped:condition_name>/submitter/<int:submitter_id>', defaults={'significance': ''})
@app.route('/variants-by-condition/<superescaped:condition_name>/submitter/<int:submitter_id>/<superescaped:significance>')
def variants_by_condition(significance = None, condition_name = None, gene = None, submitter_id = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'min_stars2': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'normalized_method2': request.args.get('method1'),
        'min_conflict_level': int_arg('min_conflict_level'),
        'gene_type': int_arg('gene_type'),
        'original_genes': request.args.get('original_genes'),
        'date': request.args.get('date'),
    }
    validate_args(args)

    if condition_name == None:
        args['condition1_name'] = list_arg('conditions')
        return render_template_async(
            'variants-by-condition.html',
            total_variants_by_condition=DB().total_variants_by_condition(**args),
            total_variants=DB().total_variants(**args),
            total_genes=DB().total_genes(**args),
            total_submitters=DB().total_submitters(**args),
        )

    if not DB().is_condition_name(condition_name):
        abort(404)
    args['condition1_name'] = condition_name
    args['original_terms'] = request.args.get('original_terms')

    if significance == None and gene == None and submitter_id == None:
        return render_template_async(
            'variants-by-condition--condition.html',
            condition_name=condition_name,
            condition_xrefs=DB().condition_xrefs(condition_name, args['date']),
            overview=get_significance_overview(
                DB().total_variants_by_significance(**args)
            ),
            breakdown_by_gene_and_significance=get_breakdown_by_gene_and_significance(
                DB().total_variants_by_gene(**args),
                DB().total_variants_by_gene_and_significance(**args)
            ),
            breakdown_by_submitter_and_significance=get_breakdown_by_submitter_and_significance(
                DB().total_variants_by_submitter(**args),
                DB().total_variants_by_submitter_and_significance(**args)
            ),
            total_variants=DB().total_variants(**args),
        )

    if significance and not DB().is_significance(significance):
        abort(404)
    args['significance1'] = significance

    if gene == None and submitter_id == None:
        return render_template_async(
            'variants-by-condition--condition-significance.html',
            condition_name=condition_name,
            significance=significance,
            variants=DB().variants(**args),
        )

    if gene:
        if gene == 'intergenic':
            gene = ''
        elif not DB().is_gene(gene):
            abort(404)
        gene_info = DB().gene_info(gene, args['original_genes'], args['date'])
        args['gene'] = gene

        return render_template_async(
            'variants-by-condition--condition-gene-significance.html',
            condition_name=condition_name,
            gene_info=gene_info,
            significance=significance,
            variants=DB().variants(**args),
        )

    submitter_info = DB().submitter_info(submitter_id)
    if not DB().is_submitter_id(submitter_id):
        abort(404)
    args['submitter1_id'] = submitter_id

    return render_template_async(
        'variants-by-condition--condition-submitter-significance.html',
        condition_name=condition_name,
        submitter_info=submitter_info,
        significance=significance,
        variants=DB().variants(**args),
    )

@app.route('/variants-by-gene')
@app.route('/variants-by-gene/<superescaped:gene>')
@app.route('/variants-by-gene/<superescaped:gene>/significance/any', defaults={'significance': ''})
@app.route('/variants-by-gene/<superescaped:gene>/significance/<superescaped:significance>')
@app.route('/variants-by-gene/<superescaped:gene>/submitter/<int:submitter_id>', defaults={'significance': ''})
@app.route('/variants-by-gene/<superescaped:gene>/submitter/<int:submitter_id>/<superescaped:significance>')
@app.route('/variants-by-gene/<superescaped:gene>/condition/<superescaped:condition_name>', defaults={'significance': ''})
@app.route('/variants-by-gene/<superescaped:gene>/condition/<superescaped:condition_name>/<superescaped:significance>')
def variants_by_gene(gene = None, significance = None, submitter_id = None, condition_name = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'min_stars2': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'normalized_method2': request.args.get('method1'),
        'min_conflict_level': int_arg('min_conflict_level'),
        'gene_type': int_arg('gene_type'),
        'original_genes': request.args.get('original_genes'),
        'date': request.args.get('date'),
    }
    validate_args(args)

    if gene == None:
        args['gene'] = list_arg('genes')
        return render_template_async(
            'variants-by-gene.html',
            total_variants_by_gene=DB().total_variants_by_gene(**args),
            total_variants=DB().total_variants(**args),
            total_conditions=DB().total_conditions(**args),
            total_submitters=DB().total_submitters(**args),
        )

    if gene == 'intergenic':
        gene = ''
    elif not DB().is_gene(gene):
        abort(404)
    gene_info = DB().gene_info(gene, args['original_genes'], args['date'])
    args['gene'] = gene
    args['original_terms'] = request.args.get('original_terms')

    if significance == None and submitter_id == None and condition_name == None:
        return render_template_async(
            'variants-by-gene--gene.html',
            gene_info=gene_info,
            overview=get_significance_overview(
                DB().total_variants_by_significance(**args)
            ),
            breakdown_by_condition_and_significance=get_breakdown_by_condition_and_significance(
                DB().total_variants_by_condition(**args),
                DB().total_variants_by_condition_and_significance(**args)
            ),
            breakdown_by_submitter_and_significance=get_breakdown_by_submitter_and_significance(
                DB().total_variants_by_submitter(**args),
                DB().total_variants_by_submitter_and_significance(**args)
            ),
            total_variants=DB().total_variants(**args),
        )

    if significance and not DB().is_significance(significance):
        abort(404)
    args['significance1'] = significance

    if submitter_id == None and condition_name == None:
        return render_template_async(
            'variants-by-gene--gene-significance.html',
            gene_info=gene_info,
            significance=significance,
            variants=DB().variants(**args),
        )

    if condition_name:
        if not DB().is_condition_name(condition_name):
            abort(404)
        args['condition1_name'] = condition_name

        return render_template_async(
            'variants-by-gene--gene-condition-significance.html',
            gene_info=gene_info,
            condition_name=condition_name,
            significance=significance,
            variants=DB().variants(**args),
        )

    if not DB().is_submitter_id(submitter_id):
        abort(404)
    submitter_info = DB().submitter_info(submitter_id)
    args['submitter1_id'] = submitter_id

    return render_template_async(
        'variants-by-gene--gene-submitter-significance.html',
        gene_info=gene_info,
        submitter_info=submitter_info,
        significance=significance,
        variants=DB().variants(**args),
    )

@app.route('/variants-by-mondo-condition')
@app.route('/variants-by-mondo-condition/<int:mondo_condition_id>')
@app.route('/variants-by-mondo-condition/<int:mondo_condition_id>/significance/any', defaults={'significance': ''})
@app.route('/variants-by-mondo-condition/<int:mondo_condition_id>/significance/<superescaped:significance>')
@app.route('/variants-by-mondo-condition/<int:mondo_condition_id>/gene/<superescaped:gene>', defaults={'significance': ''})
@app.route('/variants-by-mondo-condition/<int:mondo_condition_id>/gene/<superescaped:gene>/<superescaped:significance>')
@app.route('/variants-by-mondo-condition/<int:mondo_condition_id>/submitter/<int:submitter_id>', defaults={'significance': ''})
@app.route('/variants-by-mondo-condition/<int:mondo_condition_id>/submitter/<int:submitter_id>/<superescaped:significance>')
def variants_by_mondo_condition(mondo_condition_id = None, gene = None, significance = None, submitter_id = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'min_stars2': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'normalized_method2': request.args.get('method1'),
        'min_conflict_level': int_arg('min_conflict_level'),
        'gene_type': int_arg('gene_type'),
        'original_genes': request.args.get('original_genes'),
        'date': request.args.get('date'),
    }
    validate_args(args)

    if mondo_condition_id == None:
        return render_template_async(
            'variants-by-mondo-condition.html',
            mondo_conditions=DB().mondo_conditions(args['date']),
        )

    if not DB().is_mondo_condition_id(mondo_condition_id):
        abort(404)
    mondo_name = DB().mondo_name(mondo_condition_id)
    clinvar_names = DB().clinvar_names_from_mondo_id(mondo_condition_id, args['date'])
    args['condition1_name'] = clinvar_names
    args['original_terms'] = request.args.get('original_terms')

    if significance == None and gene == None and submitter_id == None:
        return render_template_async(
            'variants-by-mondo-condition--condition.html',
            mondo_name=mondo_name,
            clinvar_names = clinvar_names,
            overview=get_significance_overview(
                DB().total_variants_by_significance(**args)
            ),
            breakdown_by_gene_and_significance=get_breakdown_by_gene_and_significance(
                DB().total_variants_by_gene(**args),
                DB().total_variants_by_gene_and_significance(**args)
            ),
            breakdown_by_submitter_and_significance=get_breakdown_by_submitter_and_significance(
                DB().total_variants_by_submitter(**args),
                DB().total_variants_by_submitter_and_significance(**args)
            ),
            total_variants=DB().total_variants(**args),
        )

    if significance and not DB().is_significance(significance):
        abort(404)
    args['significance1'] = significance

    if gene == None and submitter_id == None:
        return render_template_async(
            'variants-by-mondo-condition--condition-significance.html',
            mondo_name=mondo_name,
            clinvar_names=clinvar_names,
            significance=significance,
            variants=DB().variants(**args),
        )

    if gene:
        if gene == 'intergenic':
            gene = ''
        elif not DB().is_gene(gene):
            abort(404)
        gene_info = DB().gene_info(gene, args['original_genes'], args['date'])
        args['gene'] = gene

        return render_template_async(
            'variants-by-mondo-condition--condition-gene-significance.html',
            mondo_name=mondo_name,
            clinvar_names=clinvar_names,
            gene_info=gene_info,
            significance=significance,
            variants=DB().variants(**args),
        )

    submitter_info = DB().submitter_info(submitter_id)
    if not DB().is_submitter_id(submitter_id):
        abort(404)
    args['submitter1_id'] = submitter_id

    return render_template_async(
        'variants-by-mondo-condition--condition-submitter-significance.html',
        mondo_name=mondo_name,
        clinvar_names=clinvar_names,
        submitter_info=submitter_info,
        significance=significance,
        variants=DB().variants(**args),
    )

@app.route('/variants-by-significance')
@app.route('/variants-by-significance/<superescaped:significance>')
def variants_by_significance(significance = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'min_stars2': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'normalized_method2': request.args.get('method1'),
        'min_conflict_level': int_arg('min_conflict_level'),
        'gene_type': int_arg('gene_type'),
        'original_genes': request.args.get('original_genes'),
        'original_terms': request.args.get('original_terms'),
        'date': request.args.get('date'),
    }
    validate_args(args)

    if significance == None:
        return render_template_async(
            'variants-by-significance.html',
            total_variants_by_significance=DB().total_variants_by_significance(**args),
            total_variants=DB().total_variants(**args),
            total_genes=DB().total_genes(**args),
            total_conditions=DB().total_conditions(**args),
            total_submitters=DB().total_submitters(**args),
        )

    if not DB().is_significance(significance):
        abort(404)

    return render_template_async(
        'variants-by-significance--significance.html',
        significance=significance,
        total_variants=DB().total_variants(**args),
        total_variants_ever=DB().total_variants(
            significance1=significance,
            **args
        ),
        total_variants_never=DB().total_variants_without_significance(
            significance=significance,
            **args
        ),
        total_variants_by_submitter=DB().total_variants_by_submitter(
            significance1=significance,
            **args
        ),
        total_variants_by_gene=DB().total_variants_by_gene(
            significance1=significance,
            **args
        ),
        total_variants_by_condition=DB().total_variants_by_condition(
            significance1=significance,
            **args
        ),
    )

@app.route('/variants-by-submitter')
@app.route('/variants-by-submitter/<int:submitter_id>')
@app.route('/variants-by-submitter/<int:submitter_id>/significance/any', defaults={'significance': ''})
@app.route('/variants-by-submitter/<int:submitter_id>/significance/<superescaped:significance>')
@app.route('/variants-by-submitter/<int:submitter_id>/gene/<superescaped:gene>', defaults={'significance': ''})
@app.route('/variants-by-submitter/<int:submitter_id>/gene/<superescaped:gene>/<superescaped:significance>')
@app.route('/variants-by-submitter/<int:submitter_id>/condition/<superescaped:condition_name>', defaults={'significance': ''})
@app.route('/variants-by-submitter/<int:submitter_id>/condition/<superescaped:condition_name>/<superescaped:significance>')
def variants_by_submitter(submitter_id = None, significance = None, gene = None, condition_name = None):
    args = {
        'min_stars1': int_arg('min_stars1'),
        'min_stars2': int_arg('min_stars1'),
        'normalized_method1': request.args.get('method1'),
        'normalized_method2': request.args.get('method1'),
        'min_conflict_level': int_arg('min_conflict_level'),
        'gene_type': int_arg('gene_type'),
        'original_genes': request.args.get('original_genes'),
        'date': request.args.get('date'),
    }
    validate_args(args)

    if submitter_id == None:
        submitters = list_arg('submitters')
        return render_template_async(
            'variants-by-submitter.html',
            total_variants_by_submitter=DB().total_variants_by_submitter(
                submitter_ids=submitters,
                **args
            ),
            total_variants=DB().total_variants(
                submitter1_id=submitters,
                **args
            ),
            total_genes=DB().total_genes(
                submitter1_id=submitters,
                **args
            ),
            total_conditions=DB().total_conditions(
                submitter1_id=submitters,
                **args
            ),
        )

    if not DB().is_submitter_id(submitter_id):
        abort(404)
    submitter_info = DB().submitter_info(submitter_id, args['date'])
    args['submitter1_id'] = submitter_id

    if significance == None and gene == None and condition_name == None:
        return render_template_async(
            'variants-by-submitter--submitter.html',
            submitter_info=submitter_info,
            submitter_primary_method=DB().submitter_primary_method(submitter_id, args['date']),
            overview=get_significance_overview(
                DB().total_variants_by_significance(**args),
            ),
            breakdown_by_gene_and_significance=get_breakdown_by_gene_and_significance(
                DB().total_variants_by_gene(**args),
                DB().total_variants_by_gene_and_significance(**args),
            ),
            breakdown_by_condition_and_significance=get_breakdown_by_condition_and_significance(
                DB().total_variants_by_condition(**args),
                DB().total_variants_by_condition_and_significance(**args),
            ),
            total_variants=DB().total_variants(**args),
        )

    if significance and not DB().is_significance(significance):
        abort(404)
    args['significance1'] = significance

    if gene == None and condition_name == None:
        return render_template_async(
            'variants-by-submitter--submitter-significance.html',
            submitter_info=submitter_info,
            significance=significance,
            variants=DB().variants(**args),
        )

    if gene:
        if gene == 'intergenic':
            gene = ''
        elif not DB().is_gene(gene):
            abort(404)
        gene_info = DB().gene_info(gene, args['original_genes'], args['date'])
        args['gene'] = gene

        return render_template_async(
            'variants-by-submitter--submitter-gene-significance.html',
            gene_info=gene_info,
            submitter_info=submitter_info,
            significance=significance,
            variants=DB().variants(**args),
        )

    if condition_name:
        if not DB().is_condition_name(condition_name):
            abort(404)
        args['condition1_name'] = condition_name

        return render_template_async(
            'variants-by-submitter--submitter-condition-significance.html',
            condition_name=condition_name,
            submitter_info=submitter_info,
            significance=significance,
            variants=DB().variants(**args),
        )
