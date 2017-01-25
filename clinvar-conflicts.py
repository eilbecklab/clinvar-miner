#!/usr/bin/env python3

import urllib
from collections import OrderedDict
from db import DB
from flask import Flask
from flask import render_template
from flask import request

app = Flask(__name__)

ALL_OTHER_SUBMITTERS = 'all other submitters'

def create_breakdown_table(significances):
    breakdown_table = OrderedDict()
    for significance1 in significances:
        breakdown_table[significance1] = OrderedDict()
        for significance2 in significances:
            breakdown_table[significance1][significance2] = 0
    return breakdown_table

@app.template_filter('orspace')
def string_or_space(path):
    return path if path else '\u200B'

@app.template_filter('quotepath')
def quote_path(path):
    return urllib.parse.quote(path).replace('/', '%252F')

@app.template_filter('rcvlink')
def rcv_link(rcv):
    return '<a href="https://www.ncbi.nlm.nih.gov/clinvar/' + rcv + '/">' + rcv + '</a>'

@app.context_processor
def template_functions():
    def submitter_link(submitter_id, submitter_name):
        if submitter_id == '0':
            return submitter_name
        return '<a href="https://www.ncbi.nlm.nih.gov/clinvar/submitters/' + submitter_id + '/">' + submitter_name + '</a>'
    return {'submitter_link': submitter_link}

@app.route('/conflicts-by-gene')
@app.route('/conflicts-by-gene/<gene>')
def conflicts_by_gene(gene = None):
    db = DB()

    if not gene:
        return render_template(
            'conflicts-by-gene-index.html',
            title='Conflicts by Gene',
            total_conflicts_by_gene=db.total_conflicts_by_gene()
        )

    gene = gene.replace('%2F', '/')
    conflicts = db.conflicts_by_gene(gene)

    return render_template(
        'conflicts-by-gene.html',
        title='Conflicts for gene ' + gene,
        conflicts=conflicts,
    )

@app.route('/conflicts-by-submitter')
@app.route('/conflicts-by-submitter/<submitter1_id>')
@app.route('/conflicts-by-submitter/<submitter1_id>/<submitter2_id>')
@app.route('/conflicts-by-submitter/<submitter1_id>/<submitter2_id>/<significance1>/<significance2>')
def conflicts_by_submitter(submitter1_id = None, submitter2_id = None, significance1 = None, significance2 = None):
    min_stars = request.args.get('min_stars')
    min_stars = int(min_stars) if min_stars else 0
    method = request.args.get('method')

    db = DB()

    if not submitter1_id:
        return render_template(
            'conflicts-by-submitter-index.html',
            title='Conflicts by Submitter',
            total_conflicts_by_submitter=db.total_conflicts_by_submitter(),
        )

    methods = db.methods()
    submitter1_info = db.submitter_info(submitter1_id)
    if not submitter1_info:
        submitter1_info = {'id': submitter1_id, 'name': submitter1_id}

    if not submitter2_id:
        conflict_overviews = db.conflict_overview(submitter_id=submitter1_id, min_stars=min_stars, method=method)
        significances = db.significances()
        submitter_primary_method = db.submitter_primary_method(submitter1_id)

        summary = OrderedDict()
        summary['0'] = {'name': ALL_OTHER_SUBMITTERS, 'total': 0}
        breakdowns = OrderedDict()
        breakdowns['0'] = {'name': ALL_OTHER_SUBMITTERS, 'table': create_breakdown_table(significances)}
        for row in conflict_overviews:
            submitter2_id = row['submitter2_id']
            submitter2_name = row['submitter2_name']
            clin_sig1 = row['clin_sig1']
            clin_sig2 = row['clin_sig2']
            count = row['count']

            summary['0']['total'] += count
            if not submitter2_id in summary:
                summary[submitter2_id] = {'name': submitter2_name, 'total': 0}
            summary[submitter2_id]['total'] += count

            breakdowns['0']['table'][clin_sig1][clin_sig2] += count
            if not submitter2_id in breakdowns:
                breakdowns[submitter2_id] = {'name': submitter2_name, 'table': create_breakdown_table(significances)}
            breakdowns[submitter2_id]['table'][clin_sig1][clin_sig2] = count

        return render_template(
            'conflicts-by-submitter-1submitter.html',
            title='Conflicts with ' + submitter1_info['name'],
            submitter_info=submitter1_info,
            submitter_primary_method=submitter_primary_method,
            summary=summary,
            breakdowns=breakdowns,
            method_options=methods,
            min_stars=min_stars,
            method=method,
        )

    if submitter2_id == '0':
        submitter2_info = {'id': '0', 'name': ALL_OTHER_SUBMITTERS}
    else:
        submitter2_info = db.submitter_info(submitter2_id)
        if not submitter2_info:
            submitter2_info = {'id': submitter2_id, 'name': submitter2_id}

    if not significance1:
        conflicts = db.conflicts_by_submitter(
            submitter1_id=submitter1_id,
            submitter2_id=submitter2_id if submitter2_id != '0' else None,
            min_stars=min_stars,
            method=method,
        )
        return render_template(
            'conflicts-by-submitter-2submitters.html',
            title='Conflicts between ' + submitter1_info['name'] + ' and ' + submitter2_info['name'],
            submitter1_info=submitter1_info,
            submitter2_info=submitter2_info,
            conflicts=conflicts,
            method_options=methods,
            min_stars=min_stars,
            method=method,
        )

    conflicts = db.conflicts_by_submitter(
        submitter1_id=submitter1_id,
        submitter2_id=submitter2_id if submitter2_id != '0' else None,
        significance1=significance1,
        significance2=significance2,
        min_stars=min_stars,
        method=method,
    )
    return render_template(
        'conflicts-by-submitter-2significances.html',
        title='Conflicts between ' + significance1 + ' variants from ' + submitter1_info['name'] + ' and ' + significance2 + ' variants from ' + submitter2_info['name'],
        submitter1_info=submitter1_info,
        submitter2_info=submitter2_info,
        significance1=significance1,
        significance2=significance2,
        conflicts=conflicts,
        method_options=methods,
        min_stars=min_stars,
        method=method,
    )

@app.route('/conflicts-by-significance')
def conflicts_by_significance():
    min_stars = request.args.get('min_stars')
    min_stars = int(min_stars) if min_stars else 0
    method = request.args.get('method')

    db = DB()
    conflict_overview = db.conflict_overview(min_stars=min_stars, method=method)
    significances = db.significances()
    methods = db.methods()

    breakdown = create_breakdown_table(significances)
    for row in conflict_overview:
        clin_sig1 = row['clin_sig1']
        clin_sig2 = row['clin_sig2']
        count = row['count']
        breakdown[clin_sig1][clin_sig2] = count

    return render_template(
        'conflicts-by-significance.html',
        title='Conflicts by Significance',
        breakdown=breakdown,
        method_options=methods,
        min_stars=min_stars,
        method=method,
    )

@app.route('/')
def index():
    db = DB()
    return render_template(
        'index.html',
        title='Home',
        max_date=db.max_date(),
    )

@app.route('/significance-terms')
@app.route('/significance-terms/', defaults={'term': ''})
@app.route('/significance-terms/<term>')
def significance_terms(term = None):
    db = DB()

    if term == None:
        return render_template(
            'significance-terms-index.html',
            title='Significance Terms',
            total_significance_terms_over_time=db.total_significance_terms_over_time(),
            significance_term_info=db.significance_term_info(),
            old_significance_term_info=db.old_significance_term_info(),
        )

    term = term.replace('%2F', '/')

    return render_template(
        'significance-terms.html',
        title='Submitters of "' + term + '" Variants',
        total_significance_terms=db.total_significance_terms(term),
    )

@app.route('/total-conflicts-by-method')
def total_conflicts_by_method():
    db = DB()
    return render_template(
        'total-conflicts-by-method.html',
        title='Total Conflicts By Method',
        total_conflicts_by_method_over_time=db.total_conflicts_by_method_over_time(),
    )

@app.route('/total-submissions-by-method')
def total_submissions_by_method():
    db = DB()
    return render_template(
        'total-submissions-by-method.html',
        title='Total Submissions by Method',
        total_submissions_by_method_over_time=db.total_submissions_by_method_over_time(),
    )
