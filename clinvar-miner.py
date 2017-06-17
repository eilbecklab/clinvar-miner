#!/usr/bin/env python3

import gzip
import urllib
from collections import OrderedDict
from datetime import datetime
from db import DB
from flask import Flask
from flask import Response
from flask import abort
from flask import render_template
from flask import request
from hashlib import sha256
from os import environ
from werkzeug.contrib.cache import SimpleCache

app = Flask(__name__)
cache = SimpleCache()
ttl = float(environ.get('TTL', 'inf'))

app.jinja_env.trim_blocks = True
app.jinja_env.lstrip_blocks = True

nonstandard_significance_term_map = dict(map(
    lambda line: line[0:-1].split('\t'),
    open('nonstandard_significance_terms.tsv')
))

def break_punctuation(text):
    #provide additional line breaking opportunities
    return (text
        .replace('(', '<wbr/>(')
        .replace(')', ')<wbr/>')
        .replace(',', ',<wbr/>')
        .replace('.', '.<wbr/>')
        .replace(':', '<wbr/>:<wbr/>')
        .replace('-', '-<wbr/>')
    )

def get_breakdown_by_gene_and_significance(total_variants_by_gene_and_significance):
    breakdown = {}
    significances = set()

    for row in total_variants_by_gene_and_significance:
        gene = row['gene']
        significance = row['significance']
        count = row['count']

        if not gene in breakdown:
            breakdown[gene] = {}
        breakdown[gene][significance] = count

        significances.add(significance)

    #sort alphabetically to be consistent if there are two or more unranked significance terms
    significances = sorted(significances)

    #sort by rank
    significances = sorted(significances, key=significance_rank)

    return breakdown, significances

def get_breakdown_by_submitter_and_significance(total_variants_by_submitter_and_significance):
    breakdown = {}
    significances = set()

    for row in total_variants_by_submitter_and_significance:
        submitter_id = row['submitter_id']
        submitter_name = row['submitter_name']
        significance = row['significance']
        count = row['count']

        if not submitter_id in breakdown:
            breakdown[submitter_id] = {'name': submitter_name, 'counts': {}}
        breakdown[submitter_id]['counts'][significance] = count

        significances.add(significance)

    #sort alphabetically to be consistent if there are two or more unranked significance terms
    significances = sorted(significances)

    #sort by rank
    significances = sorted(significances, key=significance_rank)

    return breakdown, significances

def get_breakdown_by_trait_and_significance(total_variants_by_trait_and_significance):
    breakdown = {}
    significances = set()

    for row in total_variants_by_trait_and_significance:
        trait_db = row['trait_db']
        trait_id = row['trait_id']
        trait_name = row['trait_name']
        significance = row['significance']
        count = row['count']

        if not trait_name in breakdown:
            breakdown[trait_name] = {'db': trait_db, 'id': trait_id, 'counts': {}}
        breakdown[trait_name]['counts'][significance] = count

        significances.add(significance)

    #sort alphabetically to be consistent if there are two or more unranked significance terms
    significances = sorted(significances)

    #sort by rank
    significances = sorted(significances, key=significance_rank)

    return breakdown, significances

def get_conflict_breakdown(total_conflicting_variants_by_significance_and_significance):
    breakdown = {}
    submitter1_significances = set()
    submitter2_significances = set()

    for row in total_conflicting_variants_by_significance_and_significance:
        significance1 = row['significance1']
        significance2 = row['significance2']
        count = row['count']

        if not significance1 in breakdown:
            breakdown[significance1] = {}
        breakdown[significance1][significance2] = count

        submitter1_significances.add(significance1)
        submitter2_significances.add(significance2)

    #sort alphabetically to be consistent if there are two or more unranked significance terms
    submitter1_significances = sorted(submitter1_significances)
    submitter2_significances = sorted(submitter2_significances)

    #sort by rank
    submitter1_significances = sorted(submitter1_significances, key=significance_rank)
    submitter2_significances = sorted(submitter2_significances, key=significance_rank)

    return breakdown, submitter1_significances, submitter2_significances

def get_conflict_overview(total_conflicting_variants_by_conflict_level):
    overview = {
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
    }

    for row in total_conflicting_variants_by_conflict_level:
        conflict_level = row['conflict_level']
        count = row['count']
        overview[conflict_level] = count

    return overview

def int_arg(name, default = 0):
    arg = request.args.get(name)
    try:
        return int(arg) if arg else default
    except ValueError:
        abort(400)

def significance_rank(significance):
    significance_ranks = [
        'pathogenic',
        'likely pathogenic',
        'uncertain significance',
        'likely benign',
        'benign',
        'risk allele',
        'assocation',
        'protective allele',
        'drug response',
        'confers sensitivity',
        'other',
        'not provided',
    ]
    try:
        rank = significance_ranks.index(nonstandard_significance_term_map.get(significance, significance))
    except ValueError:
        rank = len(significance_ranks) - 2.5 #insert after everything but "other" and "not provided"
    return rank

@app.template_filter('genelink')
def gene_link(gene):
    return '<a class="external" href="https://ghr.nlm.nih.gov/gene/' + gene + '">' + gene + '</a>'

@app.template_filter('date')
def prettify_date(iso_date):
    return datetime.strptime(iso_date[:10], '%Y-%m-%d').strftime('%e %b %Y') if iso_date else ''

@app.template_filter('querysuffix')
def query_suffix(request):
    return '?' + request.query_string.decode('utf-8') if request.query_string else ''

@app.template_filter('quotepath')
def quote_path(path):
    return urllib.parse.quote(path).replace('/', '%252F')

@app.template_filter('rcvlink')
def rcv_link(rcv):
    return '<a class="external" href="https://www.ncbi.nlm.nih.gov/clinvar/' + rcv + '/">' + rcv + '</a>'

@app.template_filter('orintergenic')
def string_or_space(path):
    return path if path else 'intergenic'

@app.template_filter('orspace')
def string_or_space(path):
    return path if path else '\u200B'

@app.context_processor
def template_functions():
    def h2(text):
        section_id = text.lower().replace(' ', '-')
        return '<h2 id="' + section_id + '">' + text + ' <a class="internal" href="' + request.url + '#' + section_id + '">#</a></h2>'

    def submitter_link(submitter_id, submitter_name):
        if submitter_id == 0:
            return submitter_name
        return '<a class="external" href="https://www.ncbi.nlm.nih.gov/clinvar/submitters/' + str(submitter_id) + '/">' + break_punctuation(submitter_name) + '</a>'

    def trait_link(trait_db, trait_id, trait_name):
        #find and order DB names and examples with:
        #SELECT trait_db, trait_id, COUNT(*) FROM current_submissions GROUP BY trait_db ORDER BY COUNT(*) DESC
        trait_db = trait_db.lower()
        if trait_db in ('medgen', 'umls'):
            url = 'https://www.ncbi.nlm.nih.gov/medgen/' + trait_id
        elif trait_db == 'omim':
            url = 'https://www.omim.org/entry/' + trait_id
        elif trait_db == 'genereviews':
            url = 'https://www.ncbi.nlm.nih.gov/books/' + trait_id
        elif trait_db == 'hp':
            url = 'http://compbio.charite.de/hpoweb/showterm?id=' + trait_id
        elif trait_db == 'mesh':
            url = 'https://www.ncbi.nlm.nih.gov/mesh/?term=' + trait_id
        elif trait_db == 'omim phenotypic series':
            url = 'https://www.omim.org/phenotypicseries/' + trait_id
        else:
            url = None

        if url:
            return '<a class="external" href="' + url + '">' + trait_name + '</a>'
        else:
            return trait_name

    def variant_link(variant_id, variant_name):
        if variant_id == 0:
            return variant_name
        return '<a class="external" href="https://www.ncbi.nlm.nih.gov/clinvar/variation/' + str(variant_id) + '/">' + break_punctuation(variant_name) + '</a>'

    return {
        'h2': h2,
        'submitter_link': submitter_link,
        'trait_link': trait_link,
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
    if (not cache.has(request.url) and ttl > 0 and response.status_code == 200 and not response.direct_passthrough and
            'gzip' in request.accept_encodings):
        response.set_data(gzip.compress(response.get_data()))
        response.set_etag(sha256(response.get_data()).hexdigest())
        response.headers.set('Content-Encoding', 'gzip')
        response.freeze()
        cache.set(request.url, response, timeout=ttl)
    return response

@app.route('/conflicting-variants-by-significance')
@app.route('/conflicting-variants-by-significance/<significance1>/<significance2>')
def conflicting_variants_by_significance(significance1 = None, significance2 = None):
    db = DB()

    if not significance2:
        breakdown, submitter1_significances, submitter2_significances = get_conflict_breakdown(
            db.total_conflicting_variants_by_significance_and_significance(
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
                original_terms=request.args.get('original_terms'),
            )
        )

        return render_template(
            'conflicting-variants-by-significance.html',
            overview=get_conflict_overview(
                db.total_conflicting_variants_by_conflict_level(
                    min_stars1=int_arg('min_stars1'),
                    standardized_method1=request.args.get('method1'),
                    min_stars2=int_arg('min_stars2'),
                    standardized_method2=request.args.get('method2'),
                ),
            ),
            total_variants=db.total_variants(
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
            ),
            total_conflicting_variants=db.total_variants(
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
                min_conflict_level=1,
            ),
            breakdown=breakdown,
            submitter1_significances=submitter1_significances,
            submitter2_significances=submitter2_significances,
        )

    significance1 = significance1.replace('%2F', '/')
    significance2 = significance2.replace('%2F', '/')

    return render_template(
        'conflicting-variants-by-significance--2significances.html',
        significance1=significance1,
        significance2=significance2,
        variants=db.variants(
            significance1=significance1,
            significance2=significance2,
            min_stars1=int_arg('min_stars1'),
            standardized_method1=request.args.get('method1'),
            min_stars2=int_arg('min_stars2'),
            standardized_method2=request.args.get('method2'),
            original_terms=request.args.get('original_terms'),
        ),
    )

@app.route('/conflicting-variants-by-submitter')
@app.route('/conflicting-variants-by-submitter/<submitter1_id>')
@app.route('/conflicting-variants-by-submitter/<submitter1_id>/<submitter2_id>')
@app.route('/conflicting-variants-by-submitter/<submitter1_id>/<submitter2_id>/<significance1>/<significance2>')
def conflicting_variants_by_submitter(submitter1_id = None, submitter2_id = None, significance1 = None, significance2 = None):
    db = DB()

    if submitter1_id == None:
        return render_template(
            'conflicting-variants-by-submitter.html',
            total_conflicting_variants_by_submitter=db.total_conflicting_variants_by_submitter(
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
            ),
        )

    try:
        submitter1_id = int(submitter1_id)
    except ValueError:
        return abort(404)

    if submitter2_id == None:
        total_conflicting_variants_by_submitter = db.total_conflicting_variants_by_submitter(
            submitter1_id=submitter1_id,
            min_stars1=int_arg('min_stars1'),
            standardized_method1=request.args.get('method1'),
            min_stars2=int_arg('min_stars2'),
            standardized_method2=request.args.get('method2'),
        )
        total_conflicting_variants_by_submitter_and_conflict_level = db.total_conflicting_variants_by_submitter_and_conflict_level(
            submitter1_id=submitter1_id,
            min_stars1=int_arg('min_stars1'),
            standardized_method1=request.args.get('method1'),
            min_stars2=int_arg('min_stars2'),
            standardized_method2=request.args.get('method2'),
        )
        summary = OrderedDict()
        for row in total_conflicting_variants_by_submitter:
            submitter2_id = row['submitter_id']
            submitter2_name = row['submitter_name']
            count = row['count']
            summary[submitter2_id] = {
                'name': submitter2_name,
                1: 0,
                2: 0,
                3: 0,
                4: 0,
                5: 0,
                'total': count,
            }
        for row in total_conflicting_variants_by_submitter_and_conflict_level:
            submitter2_id = row['submitter_id']
            submitter2_name = row['submitter_name']
            conflict_level = row['conflict_level']
            count = row['count']
            summary[submitter2_id][conflict_level] = count

        breakdown, submitter1_significances, submitter2_significances = get_conflict_breakdown(
            db.total_conflicting_variants_by_significance_and_significance(
                submitter1_id=submitter1_id,
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
                original_terms=request.args.get('original_terms'),
            )
        )

        return render_template(
            'conflicting-variants-by-submitter--1submitter.html',
            submitter1_info=db.submitter_info(submitter1_id),
            submitter2_info={'id': 0, 'name': 'All submitters'},
            submitter_primary_method=db.submitter_primary_method(submitter1_id),
            overview=get_conflict_overview(
                db.total_conflicting_variants_by_conflict_level(
                    submitter1_id=submitter1_id,
                    min_stars1=int_arg('min_stars1'),
                    standardized_method1=request.args.get('method1'),
                    min_stars2=int_arg('min_stars2'),
                    standardized_method2=request.args.get('method2'),
                ),
            ),
            total_variants=db.total_variants(
                submitter1_id=submitter1_id,
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
            ),
            total_conflicting_variants=db.total_variants(
                submitter1_id=submitter1_id,
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
                min_conflict_level=1,
            ),
            summary=summary,
            breakdown=breakdown,
            submitter1_significances=submitter1_significances,
            submitter2_significances=submitter2_significances,
        )

    try:
        submitter2_id = int(submitter2_id)
    except ValueError:
        abort(404)

    if submitter2_id == 0:
        submitter2_info = {'id': 0, 'name': 'any other submitter'}
    else:
        submitter2_info = db.submitter_info(submitter2_id)

    if not significance1:
        breakdown, submitter1_significances, submitter2_significances = get_conflict_breakdown(
            db.total_conflicting_variants_by_significance_and_significance(
                submitter1_id=submitter1_id,
                submitter2_id=submitter2_id,
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
                original_terms=request.args.get('original_terms'),
            )
        )

        return render_template(
            'conflicting-variants-by-submitter--2submitters.html',
            submitter1_info=db.submitter_info(submitter1_id),
            submitter2_info=submitter2_info,
            total=db.total_variants(
                submitter1_id=submitter1_id,
                submitter2_id=submitter2_id,
                min_stars1=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                min_stars2=int_arg('min_stars2'),
                standardized_method2=request.args.get('method2'),
                min_conflict_level=1,
            ),
            submitter1_significances=submitter1_significances,
            submitter2_significances=submitter2_significances,
            breakdown=breakdown,
        )

    significance1 = significance1.replace('%2F', '/')
    significance2 = significance2.replace('%2F', '/')

    return render_template(
        'conflicting-variants-by-submitter--2significances.html',
        submitter1_info=db.submitter_info(submitter1_id),
        submitter2_info=submitter2_info,
        significance1=significance1,
        significance2=significance2,
        variants=db.variants(
            submitter1_id=submitter1_id,
            submitter2_id=submitter2_id,
            significance1=significance1,
            significance2=significance2,
            min_stars1=int_arg('min_stars1'),
            standardized_method1=request.args.get('method1'),
            min_stars2=int_arg('min_stars2'),
            standardized_method2=request.args.get('method2'),
            original_terms=request.args.get('original_terms'),
        ),
    )

@app.route('/')
def index():
    db = DB()
    return render_template(
        'index.html',
        max_date=datetime.strptime(db.max_date(), '%Y-%m'),
        total_submissions=db.total_submissions(),
        total_variants=db.total_variants(),
    )

@app.route('/significance-terms')
@app.route('/significance-terms/', defaults={'term': ''})
@app.route('/significance-terms/<term>')
def significance_terms(term = None):
    db = DB()

    if term == None:
        return render_template(
            'significance-terms.html',
            total_significance_terms_over_time=db.total_significance_terms_over_time(),
            significance_term_info=db.significance_term_info(),
            old_significance_term_info=db.old_significance_term_info(),
        )

    term = term.replace('%2F', '/')

    return render_template(
        'significance-terms--term.html',
        term=term,
        total_significance_terms=db.total_significance_terms(term),
    )

@app.route('/submissions-by-variant/<variant_name>')
def submissions_by_variant(variant_name):
    db = DB()

    variant_name = variant_name.replace('%2F', '/')

    return render_template(
        'submissions-by-variant--variant.html',
        variant_name=variant_name,
        variant_id=db.variant_id(variant_name),
        submissions=db.submissions(
            variant_name=variant_name,
            min_stars=int_arg('min_stars1'),
            standardized_method=request.args.get('method1'),
            min_conflict_level=int_arg('min_conflict_level'),
        ),
    )

@app.route('/total-submissions-by-country')
@app.route('/total-submissions-by-country/', defaults={'country_code': ''})
@app.route('/total-submissions-by-country/<country_code>')
def total_submissions_by_country(country_code = None):
    db = DB()

    if country_code == None:
        return render_template(
            'total-submissions-by-country.html',
            total_submissions_by_country=db.total_submissions_by_country(),
        )

    return render_template(
        'total-submissions-by-country--country.html',
        country_name=db.country_name(country_code),
        total_submissions_by_submitter=db.total_submissions_by_submitter(country_code=country_code),
    )

@app.route('/total-submissions-by-method')
def total_submissions_by_method():
    db = DB()
    rows = db.total_submissions_by_standardized_method_over_time(
        min_stars=int_arg('min_stars1'),
        min_conflict_level=int_arg('min_conflict_level'),
    )

    dates = set()
    methods = set()
    date_method_pairs = set()
    for row in rows:
        date = row['date']
        method = row['standardized_method']
        count = row['count']

        dates.add(date)
        methods.add(method)
        date_method_pairs.add((date, method))

    for date in dates:
        for method in methods:
            if not (date, method) in date_method_pairs:
                rows.append({'date': date, 'standardized_method': method, 'count': 0})

    rows.sort(key=lambda row: row['date'])

    return render_template(
        'total-submissions-by-method.html',
        total_submissions_by_standardized_method_over_time=rows,
        total_submissions_by_method=db.total_submissions_by_method(
            min_stars=int_arg('min_stars1'),
            min_conflict_level=int_arg('min_conflict_level'),
        ),
    )

@app.route('/variants-by-gene')
@app.route('/variants-by-gene/', defaults={'gene': ''})
@app.route('/variants-by-gene/<gene>')
@app.route('/variants-by-gene/<gene>/submitter/<submitter_id>/<significance>')
@app.route('/variants-by-gene/<gene>/trait/<trait_name>/<significance>')
def variants_by_gene(gene = None, submitter_id = None, trait_name = None, significance = None):
    db = DB()

    if gene == None:
        return render_template(
            'variants-by-gene.html',
            total_variants_by_gene=db.total_variants_by_gene(
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
        )

    gene = '' if gene == 'intergenic' else gene.replace('%2F', '/')

    if submitter_id == None and trait_name == None:
        breakdown_by_trait_and_significance, significances = get_breakdown_by_trait_and_significance(
            db.total_variants_by_trait_and_significance(
                gene=gene,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
                original_terms=request.args.get('original_terms'),
            )
        )

        breakdown_by_submitter_and_significance, significances = get_breakdown_by_submitter_and_significance(
            db.total_variants_by_submitter_and_significance(
                gene=gene,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
                original_terms=request.args.get('original_terms'),
            )
        )

        return render_template(
            'variants-by-gene--gene.html',
            gene=gene,
            significances=significances,
            breakdown_by_trait_and_significance=breakdown_by_trait_and_significance,
            breakdown_by_submitter_and_significance=breakdown_by_submitter_and_significance,
            variants=db.variants(
                gene=gene,
                min_stars1=int_arg('min_stars1'),
                min_stars2=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                standardized_method2=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
        )

    if submitter_id:
        return render_template(
            'variants-by-gene--gene-submitter-significance.html',
            gene=gene,
            submitter_info=db.submitter_info(submitter_id),
            significance=significance,
            total_submissions_by_variant=db.total_submissions_by_variant(
                gene=gene,
                submitter_id=submitter_id,
                significance=significance,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
                original_terms=request.args.get('original_terms'),
            ),
        )

    if trait_name:
        trait_name = trait_name.replace('%2F', '/')

        return render_template(
            'variants-by-gene--gene-trait-significance.html',
            gene=gene,
            trait_info=db.trait_info(trait_name),
            significance=significance,
            total_submissions_by_variant=db.total_submissions_by_variant(
                gene=gene,
                trait_name=trait_name,
                significance=significance,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
                original_terms=request.args.get('original_terms'),
            ),
        )

@app.route('/variants-by-submitter')
@app.route('/variants-by-submitter/<submitter_id>')
@app.route('/variants-by-submitter/<submitter_id>/gene/<gene>/<significance>')
@app.route('/variants-by-submitter/<submitter_id>/trait/<trait_name>/<significance>')
def variants_by_submitter(submitter_id = None, gene = None, trait_name = None, significance = None):
    db = DB()

    if submitter_id == None:
        return render_template(
            'variants-by-submitter.html',
            total_variants_by_submitter=db.total_variants_by_submitter(
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
        )

    if gene == None and trait_name == None:
        breakdown_by_gene_and_significance, significances = get_breakdown_by_gene_and_significance(
            db.total_variants_by_gene_and_significance(
                submitter_id=submitter_id,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
                original_terms=request.args.get('original_terms'),
            )
        )

        breakdown_by_trait_and_significance, significances = get_breakdown_by_trait_and_significance(
            db.total_variants_by_trait_and_significance(
                submitter_id=submitter_id,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
                original_terms=request.args.get('original_terms'),
            )
        )

        return render_template(
            'variants-by-submitter--submitter.html',
            submitter_info=db.submitter_info(submitter_id),
            total=db.total_variants(
                submitter1_id=submitter_id,
                min_stars1=int_arg('min_stars1'),
                min_stars2=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                standardized_method2=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
            significances=significances,
            breakdown_by_gene_and_significance=breakdown_by_gene_and_significance,
            breakdown_by_trait_and_significance=breakdown_by_trait_and_significance,
        )

    if gene:
        gene = '' if gene == 'intergenic' else gene.replace('%2F', '/')

        return render_template(
            'variants-by-submitter--submitter-gene-significance.html',
            gene=gene,
            submitter_info=db.submitter_info(submitter_id),
            significance=significance,
            total_submissions_by_variant=db.total_submissions_by_variant(
                gene=gene,
                submitter_id=submitter_id,
                significance=significance,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
        )

    if trait_name:
        trait_name = trait_name.replace('%2F', '/')

        return render_template(
            'variants-by-submitter--submitter-trait-significance.html',
            trait_info=db.trait_info(trait_name),
            submitter_info=db.submitter_info(submitter_id),
            significance=significance,
            total_submissions_by_variant=db.total_submissions_by_variant(
                trait_name=trait_name,
                submitter_id=submitter_id,
                significance=significance,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
        )

@app.route('/variants-by-trait')
@app.route('/variants-by-trait/<trait_name>')
@app.route('/variants-by-trait/<trait_name>/gene/<gene>/<significance>')
@app.route('/variants-by-trait/<trait_name>/submitter/<submitter_id>/<significance>')
def variants_by_trait(trait_name = None, gene = None, submitter_id = None, significance = None):
    db = DB()

    if trait_name == None:
        return render_template(
            'variants-by-trait.html',
            total_variants_by_trait=db.total_variants_by_trait(
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
        )

    trait_name = trait_name.replace('%2F', '/')

    if gene == None and submitter_id == None:
        breakdown_by_gene_and_significance, significances = get_breakdown_by_gene_and_significance(
            db.total_variants_by_gene_and_significance(
                trait_name=trait_name,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
                original_terms=request.args.get('original_terms'),
            )
        )

        breakdown_by_submitter_and_significance, significances = get_breakdown_by_submitter_and_significance(
            db.total_variants_by_submitter_and_significance(
                trait_name=trait_name,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
                original_terms=request.args.get('original_terms'),
            )
        )

        return render_template(
            'variants-by-trait--trait.html',
            trait_info=db.trait_info(trait_name),
            total=db.total_variants(
                trait_name=trait_name,
                min_stars1=int_arg('min_stars1'),
                min_stars2=int_arg('min_stars1'),
                standardized_method1=request.args.get('method1'),
                standardized_method2=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
            significances=significances,
            breakdown_by_gene_and_significance=breakdown_by_gene_and_significance,
            breakdown_by_submitter_and_significance=breakdown_by_submitter_and_significance,
        )

    if gene:
        gene = '' if gene == 'intergenic' else gene.replace('%2F', '/')

        return render_template(
            'variants-by-trait--trait-gene-significance.html',
            trait_info=db.trait_info(trait_name),
            gene=gene,
            significance=significance,
            total_submissions_by_variant=db.total_submissions_by_variant(
                gene=gene,
                trait_name=trait_name,
                significance=significance,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
        )

    if submitter_id:
        return render_template(
            'variants-by-trait--trait-submitter-significance.html',
            trait_info=db.trait_info(trait_name),
            submitter_info=db.submitter_info(submitter_id),
            significance=significance,
            total_submissions_by_variant=db.total_submissions_by_variant(
                trait_name=trait_name,
                submitter_id=submitter_id,
                significance=significance,
                min_stars=int_arg('min_stars1'),
                standardized_method=request.args.get('method1'),
                min_conflict_level=int_arg('min_conflict_level'),
            ),
        )
