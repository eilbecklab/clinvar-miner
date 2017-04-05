#!/bin/bash
FLASK_DEBUG=1 FLASK_APP=clinvar-conflicts.py TTL=${TTL:-0} flask run
