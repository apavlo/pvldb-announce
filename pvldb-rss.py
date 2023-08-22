#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging
import argparse
import sqlite3
from feedgen.feed import FeedGenerator

from config import *

## ==============================================
## LOGGING
## ==============================================
LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## writeRSS
## ==============================================
def writeRSS(papers, output):
    fg = FeedGenerator()
    fg.id(RSS_URL)
    fg.title(RSS_TITLE)
    fg.subtitle(RSS_SUBTITLE)
    fg.author(RSS_AUTHOR)
    fg.link( href='https://www.vldb.org/pvldb/', rel='alternate' )
    fg.language('en')
    
    for p in papers:
        summary = "%(title)s\nAuthors: %(authors)s\n[PVLDB Volume %(volume)d, Number %(number)d]" % p
        
        fe = fg.add_entry()
        fe.author(name=p["authors"])
        fe.title(p["title"])
        fe.link(href=p["link"]) 
        fe.id(p["link"])
        fe.published(published=p["published"])
        # fe.description(description=summary, isSummary=True)
        fe.content(summary)
    ## FOR
    
    atomfeed = fg.atom_str(pretty=True) # Get the ATOM feed as string
    atom_file = os.path.join(output, 'pvldb-atom.xml')
    fg.atom_file(atom_file) # Write the ATOM feed to a file
    LOG.info("Created ATOM '%s'" % atom_file)
    
    rssfeed  = fg.rss_str(pretty=True) # Get the RSS feed as string
    rss_file = os.path.join(output, RSS_FILE)
    fg.rss_file(rss_file) # Write the RSS feed to a file
    LOG.info("Created RSS '%s'" % rss_file)
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='PVLDB Announcements Script')
    aparser.add_argument('dbpath', type=str, help='Database Path')
    aparser.add_argument('rsspath', type=str, help='RSS output directory')
    aparser.add_argument("--debug", action='store_true')

    args = vars(aparser.parse_args())

    ## ----------------------------------------------
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)

    ## ----------------------------------------------
    
    if not os.path.exists(args['dbpath']):
        raise Exception("Database file %s does not exist", args['dbpath'])
    db = sqlite3.connect(args['dbpath'])
    cur = db.cursor()
        
    # Always create the RSS files from scratch
    assert args["rsspath"]
    if not os.path.exists(args["rsspath"]):
        os.makedirs(args["rsspath"])

    sql = "SELECT link, title, authors, volume, number, published FROM papers ORDER BY volume ASC, number ASC, link"
    papers = [ ]
    for row in cur.execute(sql):
        paper = {
            "link":     row[0],
            "title":    row[1],
            "authors":  row[2],
            "volume":   row[3],
            "number":   row[4],
            "published":row[5],
        }
        papers.append(paper)
    ## FOR
    writeRSS(papers, args["rsspath"])
    
    db.close()
## MAIN
    
    

