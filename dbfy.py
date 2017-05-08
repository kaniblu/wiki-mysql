# encoding: utf-8

from __future__ import unicode_literals

import re
import os
import shutil
import tempfile
import six.moves.urllib.request as urllib

import bz2
import tqdm
import gensim
import configargparse as argparse

from database import Database
from filters import WikiTextFilter


def range_str(txt):
    assert "," in txt

    def str2int(str):
        if "x" in str:
            return int(str, base=16)
        else:
            return int(str)

    l, r = txt.split(",")

    return str2int(l), str2int(r)

def create_parser():
    parser = argparse.ArgParser()
    parser.add("--src", default="https://dumps.wikimedia.org/enwiki/latest/"
                                "enwiki-latest-pages-articles.xml.bz2",
               help="Wikipedia dump url. Alternatively, it can be used "
                    "to specify a local file by using 'file' scheme. ")
    parser.add("--silent", "-y", default=False, action="store_true",
               help="Disable all prompts.")

    g = parser.add_argument_group("Database Settings")
    g.add("--host", default="localhost", type=str)
    g.add("--port", default=3306, type=int)
    g.add("--db", default="wiki", type=str)
    g.add("--user", default="wiki", type=str)
    g.add("--passwd", required=True, type=str)
    g.add("--charset", default="utf8", type=str)
    g.add("--init_script", default="sql/init.sql", type=str)

    g = parser.add_argument_group("Filtering Options")
    g.add("--remove_html", default=1, type=int)
    g.add("--valid_unichrs", type=range_str, action="append",
          help="Valid unicode character ranges. This can be specified by "
               "supplying two integers delimited by a comma denoting "
               "left and right bounds (inclusive) respectively. e.g. "
               "'0x2466,0x2588'. Hex and decimals are all supported. "
               "Multiple ranges are allowed.")
    g.add("--invalid_unichrs", type=range_str, action="append",
          help="Invalid unicode character ranges. Refer to 'valid_unichrs'"
               "for usage help.")
    g.add("--remove_multiple_whitespaces", default=False, action="store_true",
          help="Toggle to remove consecutive whitespaces.")

    return parser

def check_url(url):
    try:
        filename = os.path.split(url)[-1]
        base, ext = os.path.splitext(filename)

        if ext != "bz2":
            return False
    except:
        return False
    finally:
        return True

def download_dump(src):
    if src.startswith("file"):
        dl_path = src.split("://")[-1]
        should_remove = False
    else:
        temp_path = tempfile.gettempdir()
        dl_path = os.path.join(temp_path, "wiki.xml.bz2")
        should_remove = True

        urllib.urlretrieve(src, dl_path)

    assert os.path.exists(dl_path)

    return dl_path, should_remove

def cleanup(dl_path, should_remove):
    if should_remove and os.path.exists(dl_path):
        os.remove(dl_path)

REDIRECT_PAT = re.compile(r"\#REDIRECT \[\[([^\]]*)\]\]")

def resolve(ttl, redirects, ttl2bid):
    if ttl in ttl2bid:
        return ttl2bid

    if ttl in redirects:
        return resolve(redirects[ttl], redirects, ttl2bid)

    return None

def dbfy(path, db, fltr):
    redirects = {}
    redirects_aid = {}
    ttl2bid = {}

    with bz2.BZ2File(path, "r") as f:
        it = gensim.corpora.wikicorpus.extract_pages(f, ("0", ))

        for i, (title, body, aid) in tqdm.tqdm(enumerate(it)):
            aid = int(aid)
            match = REDIRECT_PAT.match(body)

            if match:
                rdr_ttl = match.group(1)
                redirects[title] = rdr_ttl
                redirects_aid[title] = aid
                continue

            body = fltr(body)

            bid = db.insert("bodies", {
                "body": body
            }, auto_column="id")

            db.insert("articles", {
                "title": title,
                "body": bid,
                "aid": aid
            }, auto_column="id")

            db.commit()

    for ttl, rdr_ttl in redirects.items():
        bid = resolve(ttl, redirects, ttl2bid)

        # Failed to resolve
        # The page to which `ttl` redirects is probably omitted for
        # some unknown reason.
        if bid is None:
            continue

        aid = redirects_aid[ttl]

        db.insert("articles", {
            "title": ttl,
            "body": bid,
            "aid": aid
        }, auto_column="id")

    db.commit()

def main():
    args = create_parser().parse_args()

    url = args.src
    silent = args.silent

    host = args.host
    port = args.port
    dbname = args.db
    username = args.user
    passwd = args.passwd
    charset = args.charset
    init_path = args.init_script

    remove_html = args.remove_html
    valid_unichrs = args.valid_unichrs
    invalid_unichrs = args.invalid_unichrs

    # URL Check might be unnecessary.
    # if not check_url(url):
    #     raise ValueError("URL must be of valid format. ")

    db = Database(host=host, port=port, db=dbname, user=username,
                  password=passwd, charset=charset)

    print("This will reset the database at '{}'.".format(dbname))

    if not silent:
        ans = input("Continue? (y/n): ")
    else:
        ans = "y"

    if ans != "y":
        print("The script will now terminate.")
        exit(0)

    if os.path.exists(init_path):
        db.execute_script(init_path)

    fltr = WikiTextFilter(remove_html, valid_unichrs, invalid_unichrs)

    print("Downloading Wikipedia article dump from '{}'...".format(url))
    dmp_path, should_remove = download_dump(url)

    print("Parsing and storing articles to mysql...")
    dbfy(dmp_path, db, fltr)

    print("Cleaning up...")
    cleanup(dmp_path, should_remove)

    print("Done!")

if __name__ == '__main__':
    main()