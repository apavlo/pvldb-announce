#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import time
import argparse
import sqlite3
import requests
import tempfile
from PIL import Image
from pdf2image import convert_from_path
from pprint import pprint, pformat

import tweepy
from mastodon import Mastodon
from atproto import Client

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
## getImage
## ==============================================
def getImage(pdf_url):
    LOG.debug("Create thumbnail for '%s'", pdf_url)
    # Get the filename from the URL
    temp_dir = tempfile.gettempdir()
    pdf_filename = "pvldb-" + os.path.basename(pdf_url)
    pdf_path = os.path.join(temp_dir, pdf_filename)

    # Download the PDF file
    if not os.path.exists(pdf_path):
        response = requests.get(pdf_url)
        with open(pdf_path, 'wb') as pdf_file:
            pdf_file.write(response.content)
            LOG.debug("Downloaded PDF %s", pdf_path)

    # Convert the downloaded PDF to PNG images
    img_filename = f'{pdf_filename}.png'
    img_path = os.path.join(temp_dir, img_filename)
    if not os.path.exists(img_path):
        images = convert_from_path(pdf_path, first_page=0, last_page=1)
        if images:
            images[0].save(img_path, 'PNG')
            LOG.debug(f'Conversion successful. Image saved in temporary directory: {img_path}')
    else:
        LOG.debug(f'Reusing existing image: {img_path}')
    return img_path

## ==============================================
## resizeImage
## ==============================================
def resizeImage(img_path, max_size_kb: int, step=5):
    """
    Resize a PNG file incrementally until its size is below the given threshold.

    :param img_path: Path to the PNG file.
    :param max_size_kb: Maximum allowed file size in KB.
    :param step: Percentage decrease in image size per iteration.
    """
    max_size_bytes = max_size_kb * 1024

    if os.path.getsize(img_path) <= max_size_bytes:
      return img_path

    image = Image.open(img_path)
    width, height = image.size
    LOG.debug(f"Compressing Image '{img_path}' // (width={width}, height={height}) // File Size: {os.path.getsize(img_path) / 1024:.2f} KB")

    # First try converting it to JPG to see if that makes it small enough
    jpg_path = img_path.replace(".png", ".jpg")
    image.convert("RGB").save(jpg_path, format='JPEG', quality=85, optimize=True)
    if os.path.getsize(jpg_path) < max_size_bytes:
        LOG.debug(f"Converted to JPG with file size: {os.path.getsize(jpg_path) / 1024:.2f} KB")
        img_path = jpg_path
    else:
      LOG.debug("Incrementally resizing until image is small enough")
      while os.path.getsize(img_path) > max_size_bytes:
          width = int(width * (1 - step / 100))
          height = int(height * (1 - step / 100))
          image = image.resize((width, height), Image.LANCZOS)

          temp_path = img_path.replace(".png", "_temp.png")
          image.save(temp_path, format='PNG', optimize=True)

          if os.path.getsize(temp_path) < max_size_bytes:
              img_path = temp_path
              break
    LOG.debug(f"Final Image '{img_path}' // (width={width}, height={height}) // File Size: {os.path.getsize(img_path) / 1024:.2f} KB")

    return img_path

def getAuthorCaption(paper):
    # Figure whether there is more than one author
    if paper["authors"].find(",") != -1:
        caption = "👥 Authors"
    else:
        caption = "👤 Author"
    caption += ": %(authors)s" % paper
    return caption

def getPaperPost(paper, max_chars : int):
    post = u"Vol:%(volume)d No:%(number)d → %(title)s" % paper
    if paper["authors"]:
        post += "\n" + getAuthorCaption(paper)
    if len(post)+70 > max_chars:
        remaining = max_chars - (len(post)+70)
        post = post[:remaining-3] + u"..."
    post += "\n📄 PDF: " + paper["link"]

    return post

## ==============================================
## postMastodon
## ==============================================
def postMastodon(args, paper):
    LOG.info("Posting paper '%s' to Mastodon!", paper["title"])

    api = Mastodon(
        access_token=args["mastodon_api_key"],
        api_base_url=args["mastodon_url"]
    )

    post = getPaperPost(paper, POST_MAX_NUM_CHARS["mastodon"])
    LOG.debug("%s [Length=%d]: %s", "mastodon", len(post), post)

    if not args["dry_run"]:
        if "image" in paper and paper["image"]:
            caption = ""
            if not args["no_caption"]:
                caption = "Thumbnail: %(title)s" % paper

            media = api.media_post(paper["image"], focus=(0, 0.85), description=caption)
            status = api.status_post(post, visibility='public', media_ids=media)
        else:
            status = api.status_post(post, visibility='public')
        LOG.info("Wrote post to %s [status=%s]", args["mastodon_url"], str(status))
    else:
        LOG.debug("Not posting to mastodon because dry-run is enabled")
    return

## ==============================================
## postBluesky
## ==============================================
def postBluesky(args, paper):
    LOG.info("Posting paper '%s' to Bluesky!", paper["title"])

    api = Client()
    api.login(args["bluesky_handle"], args["bluesky_password"])

    post = getPaperPost(paper, POST_MAX_NUM_CHARS["bluesky"])
    LOG.debug("%s [Length=%d]: %s", "bluesky", len(post), post)

    if not args["dry_run"]:
        if "image" in paper and paper["image"]:
            caption = ""
            if not args["no_caption"]:
                caption = "Thumbnail: %(title)s" % paper

            # Bluesky has limits on image file size, so either convert it to a JPG or resize it
            img_path = resizeImage(paper["image"], 950)
            with open(img_path, 'rb') as f:
                img_data = f.read()
            status = api.send_image(text=post, image=img_data, image_alt=caption)
        else:
            status = api.send_post(text=post)
        LOG.info("Wrote post to %s [status=%s]", "bluesky", str(status))
    else:
        LOG.debug("Not posting to bluesky because dry-run is enabled")
    return

## ==============================================
## postTwitter
## ==============================================
def postTwitter(args, paper):
    LOG.info("Posting paper '%s' to twitter!" % paper["title"])

    client = tweepy.Client(
        # bearer_token=args['twitter_bearer_token'],
        consumer_key=args['twitter_api_key'],
        consumer_secret=args['twitter_api_secret'],
        access_token=args['twitter_access_token'],
        access_token_secret=args['twitter_access_secret']
    )

    auth = tweepy.OAuthHandler(args['twitter_api_key'], args['twitter_api_secret'])
    auth.set_access_token(args['twitter_access_token'], args['twitter_access_secret'])
    api = tweepy.API(auth)

    post = "Vol:%(volume)d No:%(number)d → %(title)s" % paper
    if len(post) + 24 > POST_MAX_NUM_CHARS["twitter"]:
        remaining = POST_MAX_NUM_CHARS["twitter"] - (len(post) + 24)
        post = post[:remaining - 3] + "..."
    post += " " + paper["link"]
    LOG.debug("%s [Length=%d]" % (post, len(post)))

    if not args["dry_run"]:
        if not args['no_image'] and "image" in paper and paper["image"]:
            media = api.media_upload(paper["image"])
            LOG.debug("Media:", media)

            if not args['no_caption']:
                caption = "%(title)s" % paper
                if paper["authors"]:
                    caption += "\n" + getAuthorCaption(paper)
                LOG.debug("Caption:", caption)
                api.update_status(status=caption, media_ids=[media.media_id])
            else:
                LOG.debug("Skipping image captions")

            status = client.create_tweet(text=post, media_ids=[media.media_id], user_auth=True)
        else:
            status = client.create_tweet(text=post, user_auth=True)
            # status = api.update_status(status=post)
        LOG.info("Posted tweet [status=%s]", str(status))
    else:
        LOG.debug("Not posting to twitter because dry-run is enabled")

    return

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='PVLDB Announcements Script')
    aparser.add_argument('dbpath', help='Database Path')
    aparser.add_argument("--debug", action='store_true')
    aparser.add_argument("--dry-run", action='store_true')

    aparser.add_argument('--limit', type=int, help='Number of papers to announce before stopping')
    aparser.add_argument('--no-image', action='store_true', help='Do not post images')
    aparser.add_argument('--no-caption', action='store_true', help='Do not include captions for images')
    aparser.add_argument('--sleep', type=int, default=POST_SLEEP_TIME, help='How many seconds to sleep between each post')
    aparser.add_argument('--preference', type=str, help='Author ordering preference')

    ## Mastodon Parameters
    agroup = aparser.add_argument_group('Mastodon Parameters')
    agroup.add_argument('--mastodon', action='store_true', help='Post announcements on Mastodon')
    agroup.add_argument('--mastodon-api-key', type=str, help='Mastodon API Key')
    agroup.add_argument('--mastodon-url', type=str, help='Mastodon Server URL')

    ## Bluesky Parameters
    agroup = aparser.add_argument_group('Bluesky Parameters')
    agroup.add_argument('--bluesky', action='store_true', help='Post announcements on Bluesky')
    agroup.add_argument('--bluesky-handle', type=str, help='Bluesky Handle')
    agroup.add_argument('--bluesky-password', type=str, help='Bluesky App Password')

    ## Twitter Parameters
    agroup = aparser.add_argument_group('Twitter Parameters')
    agroup.add_argument('--twitter', action='store_true', help='Post announcements on Twitter')
    agroup.add_argument('--twitter-api-key', type=str, help='Twitter API Key')
    agroup.add_argument('--twitter-api-secret', type=str, help='Twitter API Secret')
    agroup.add_argument('--twitter-access-token', type=str, help='Twitter Access Token Key')
    agroup.add_argument('--twitter-access-secret', type=str, help='Twitter Access Token Secret')
    agroup.add_argument('--twitter-bearer-token', type=str, help='Twitter Bearer Token')

    args = vars(aparser.parse_args())

    ## ----------------------------------------------
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)

    # If they want to post to a service, make sure they give us all the info
    # that we need to do this
    post_targets = [ ]
    all_targets = ["mastodon", "twitter", "bluesky"]
    for target in all_targets:
        if target not in args or not args[target]: continue
        LOG.debug("Checking %s input arguments", target)
        for k in args.keys():
            if k.startswith(target) and args[k] is None:
                LOG.error("Missing '%s' input parameter for %s", k, target)
                sys.exit(1)
        ## FOR
        post_targets.append(target)
    if not post_targets:
        raise Exception("No post target was specified [%s]", ",".join(all_targets))

    ## ----------------------------------------------

    if not os.path.exists(args['dbpath']):
        raise Exception("Database file '%s' does not exist" % args['dbpath'])
    db = sqlite3.connect(args['dbpath'])
    cur = db.cursor()

    ## Post new papers

    where = [ ]
    for target in post_targets:
        if target in args and args[target]:
            where.append("%s = 0" % target)
    assert len(where)

    sql = "SELECT * FROM papers WHERE %s " % " OR ".join(where)
    sql += "ORDER BY volume ASC, number ASC, "
    if 'preference' in args and args['preference']:
        sql += "CASE WHEN authors LIKE '%" + args['preference'] + "%' THEN NULL ELSE link END DESC"
    else:
        sql += "link"
    LOG.debug(sql)

    new_papers = [ ]
    for row in cur.execute(sql):
        paper = {
            "link":     row[0],
            "title":    row[1],
            "authors":  row[2],
            "volume":   row[3],
            "number":   row[4],
            "published":row[5],
        }
        new_papers.append(paper)
    ## FOR
    paper_count = 0
    for paper in new_papers:
        # Get a PNG image of the first page
        if not args['no_image']:
            paper["image"] = getImage(paper["link"])
            assert paper["image"]
        else:
            paper["image"] = ""

        for target in post_targets:
            try:
                if target == "mastodon":
                    postMastodon(args, paper)
                elif target == "bluesky":
                    postBluesky(args, paper)
                elif target == "twitter":
                    postTwitter(args, paper)
                cur = db.cursor()
                sql = "UPDATE papers SET %s = 1 WHERE link = ?" % target
                if not args["dry_run"]:
                    cur.execute(sql, (paper["link"],))
                    db.commit()
                else:
                    LOG.debug("Not updating %s [%s] because dry-run is enabled", os.path.basename(paper["link"]), target)
            except:
                raise
        paper_count += 1
        if args["limit"] and paper_count >= args["limit"]:
            break
        LOG.warning("Sleeping for %d seconds...", args["sleep"])
        time.sleep(args["sleep"])
    ## FOR
    
    db.close()
## MAIN
    
    

