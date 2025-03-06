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
import json
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
            #LOG.warning("No 'href' tag found. Skipping '%s'" % str(a))
            #continue
        
        #m = regex.search(a["href"])
        #if a["href"].find("/pvldb/vol") != -1:
            #pprint(a.__dict__)
            #pprint(m)
            #print("="*20)
        #if m and not a["href"] in volumes:
            #if a["href"] in SKIP:
                #LOG.warning("Skipping '%s'" % a["href"])
                #continue
            #volumes.append(a["href"])
    ### FOR
    #assert len(volumes) > 0    
    #return [ BASE_URL + x for x in volumes ]
## DEF

## ==============================================
## getPapers
## ==============================================
def getPapers(volume, vol_url):
    LOG.debug("Retreiving papers for %s", vol_url)

    vol_papers = { }
    html = None
    # with open("/tmp/pvldb.html", "r") as fd:
    #     html = fd.read()
    html = urllib.request.urlopen(vol_url).read()
    # with open("/tmp/pvldb.html", "wb") as fd:
    #     fd.write(html)
    soup = BeautifulSoup(html, "lxml")

    # They embed a JSON object in the HTML that we can use to extract the data we need
    # This is way easier than messing around with parsing HTML
    data = soup.find('script', type="application/json")
    if not data:
        LOG.error("Unable to find JSON data from %s", vol_url)
        return (vol_papers)

    json_data = json.loads(data.contents[0])
    try:
        volume_data = json_data["props"]["pageProps"]["groupedIssues"]
    except:
        LOG.warning("Invalid JSON data for volume %d", volume)
        return (vol_papers)

    for number, number_papers in volume_data.items():
        number = int(number)
        papers = []
        LOG.debug("Extracting papers for 'Vol:%d, Number:%d'", volume, number)
        for paper in number_papers:
            if paper["title"].lower() == 'front matter': continue
            LOG.debug(paper)
            papers.append({
                "authors":      paper["authors"],
                "title":        paper["title"],
                "volume":       volume,
                "number":       number,
                "link":         paper["pdf"].replace("http:", "https:"),
                "published":    datetime.today().replace(tzinfo=pytz.utc),
            })
        key = (volume, number)
        vol_papers[key] = papers
        LOG.debug("Found %d papers for 'Vol:%d, Number:%d'\n%s", len(papers), volume, number, pformat(papers))

    return (vol_papers)

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

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='PVLDB Announcements Collection Script')
    aparser.add_argument('dbpath', help='Database Path')
    aparser.add_argument("--debug", action='store_true')
    aparser.add_argument("--dry-run", action='store_true')

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
    if not os.path.exists(args['dbpath']):
        LOG.info("Creating database file %s", args['dbpath'])
        createDatabase()
    db = sqlite3.connect(args['dbpath'])
    cur = db.cursor()
        
    # Get the volume URLs
    papers = { }
    for vol in range(args["collect_start"], args["collect_stop"]+1):
        url = START_URL % vol
        p = getPapers(vol, url)
        if p: papers.update(p)

    # Figure out what papers are new
    for key in reversed(sorted(papers.keys())):
        LOG.debug("KEY=%s -> #papers=%d", key, len(papers[key]))
        for p in papers[key]:
            LOG.debug("Checking whether paper '%s' already exists...", os.path.basename(p["link"]))
            sql = "SELECT * FROM papers WHERE link = ?"
            cur.execute(sql, (p["link"],))
            row = cur.fetchone()
            if row is None:
                LOG.debug("Adding %s" % p["link"])

                sql = """INSERT INTO papers (
                            link, title, authors, volume, number, published
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?)"""
                if not args["dry_run"]:
                    cur.execute(sql, (p["link"], p["title"], p["authors"], p["volume"], p["number"], p["published"],))
                else:
                    LOG.debug("Not inserting because dry-run is enabled")
        ## FOR
    ## FOR
    db.commit()
    db.close()
## MAIN
    
    

