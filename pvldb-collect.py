#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import urllib.request, urllib.parse, urllib.error
import logging
import pytz
import time
import argparse
import sqlite3
from datetime import datetime
from datetime import tzinfo
from pprint import pprint, pformat
from bs4 import BeautifulSoup

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
## getVolumeUrls
## ==============================================
def getVolumeUrls(url):
    volumes = [ ]
    
    # Brute force search for volumes
    for vol in range(15,16):
        url = START_URL % vol
        LOG.debug("Checking whether volume '%s' exists" % url)
        try:
            r = urllib.request.urlopen(url).read()
            if not url in volumes:
                volumes.append(url)
        except:
            LOG.debug("Volume #%d does not exist. Skipping..." % vol)
            pass
    ## FOR
    assert len(volumes) > 0    
    return volumes
        
    #soup = BeautifulSoup(r, "lxml")
    #regex = re.compile("\/vol[\d]+-volume-info\.html")
    #for a in soup.find_all('a'):
        #if not "href" in a.attrs:
            ## pprint(a.__dict__)
            #LOG.warn("No 'href' tag found. Skipping '%s'" % str(a))
            #continue
        
        #m = regex.search(a["href"])
        #if a["href"].find("/pvldb/vol") != -1:
            #pprint(a.__dict__)
            #pprint(m)
            #print("="*20)
        #if m and not a["href"] in volumes:
            #if a["href"] in SKIP:
                #LOG.warn("Skipping '%s'" % a["href"])
                #continue
            #volumes.append(a["href"])
    ### FOR
    #assert len(volumes) > 0    
    #return [ BASE_URL + x for x in volumes ]
## DEF

## ==============================================
## getPapersFromDiv
## ==============================================
def getPapersFromDiv(volume, number, div):
    papers = [ ]
    paper_divs = div.find_all(class_="shadow-app")
    if paper_divs is None:
        LOG.warn("Failed to find any papers in DIV '%s'" % div.text)
        return papers

    for paper_div in paper_divs:
        data = [ ]
        # pprint(paper_div.__dict__)
        # print("="*100)

        try:
            # Pages, Authors
            results = paper_div.find_all('p')
            for element in results:
                # print("*"*60)
                # pprint(element.__dict__)
                data.append(element.contents[0])
            ## FOR

            # Title
            results = paper_div.find_all('h5')
            for element in results:
                # print("*"*60)
                # pprint(element.__dict__)
                data.append(element.contents[0])

            # Paper PDF URL
            results = paper_div.find_all('a')
            for element in results:
                url = element["href"].replace("http:", "https:")
                data.append(url)

            # print("%"*30)
            # pprint(data)

            papers.append({
                "authors":      data[1],
                "title":        data[2],
                "volume":       volume,
                "number":       number,
                "link":         data[3],
                "published":    datetime.today().replace(tzinfo=pytz.utc),
            })
            LOG.debug("Found new paper for 'Vol:%d, Number:%d'\n%s", volume, number, pformat(papers[-1]))
        except:
            LOG.error("Unexpected error for 'Vol:%d, Number:%d' DIV", volume, number)
            raise
    ## FOR
    return papers
## DEF


## ==============================================
## getPapers
## ==============================================
def getPapers(volume, vol_url):
    LOG.debug("Retreiving papers for %s" % vol_url)

    html = None
    # with open("/tmp/pvldb.html", "r") as fd:
    #     html = fd.read()
    r = urllib.request.urlopen(vol_url).read()
    soup = BeautifulSoup(r, "lxml")

    number = 1
    papers = { }
    while True:
        LOG.debug("Looking for 'Vol:%d, Number:%d' DIV", volume, number)
        div = soup.find('div', {"id": "issue-%d" % number})
        if not div:
            break

        key = (volume, number)
        papers[key] = getPapersFromDiv(volume, number, div)
        if len(papers[key]) > 0:
            LOG.debug("Found %d papers for 'Vol:%d, Number:%d'\n%s", len(papers[key]), volume, number, pformat(papers))

        number = number + 1
    ## WHILE
    return (papers)
## DEF

## ==============================================
## createDatabase
## ==============================================
def createDatabase():
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()
    
    sql = """
    CREATE TABLE papers (
        link VARCHAR(255) PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT NOT NULL,
        volume INT NOT NULL,
        number INT NOT NULL,
        published DATE NOT NULL,
        twitter INT NOT NULL DEFAULT 0,
        mastodon INT NOT NULL DEFAULT 0,
        created timestamp DEFAULT CURRENT_TIMESTAMP
    );"""
    cur.execute(sql)
    db.commit()
    db.close()
    
## FOR


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='PVLDB Announcements Collection Script')
    aparser.add_argument('dbpath', help='Database Path')
    aparser.add_argument("--debug", action='store_true')

    ## Collection Parameters
    agroup = aparser.add_argument_group('Collection Parameters')
    agroup.add_argument('--collect-start', type=int, help='Start volume to check')
    agroup.add_argument('--collect-stop', type=int, help='Stop volume to check (inclusive)')
    
    args = vars(aparser.parse_args())

    ## ----------------------------------------------
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)

    ## ----------------------------------------------
    
    # Create the database if we don't have it
    if not os.path.exists(DB_PATH):
        createDatabase()
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()
        
    # Get the volume URLs
    papers = { }
    for vol in range(args["collect_start"], args["collect_stop"]+1):
        url = START_URL % vol
        p = getPapers(vol, url)
        if p: papers.update(p)

    # Figure out what papers are new
    for key in reversed(sorted(papers.keys())):
        LOG.debug("key=%s", key)
        for p in papers[key]:
            sql = "SELECT * FROM papers WHERE link = ?"
            cur.execute(sql, (p["link"],))
            row = cur.fetchone()
            if row is None:
                LOG.debug("Adding %s" % p["link"])

                sql = """INSERT INTO papers (
                            link, title, authors, volume, number, published
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?)"""
                cur.execute(sql, (p["link"], p["title"], p["authors"], p["volume"], p["number"], p["published"],))
        ## FOR
    ## FOR
    db.commit()
    db.close()
## MAIN
    
    

