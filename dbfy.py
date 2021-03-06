# encoding: utf-8

from __future__ import unicode_literals

import re
import os
import tempfile
import multiprocessing.pool as mp
import six.moves.urllib.request as urllib

import bz2
import tqdm
import gensim
import configargparse as argparse

from database import Database
from filters import WikiBodyFilter


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
    parser.add("--n_processes", type=int, default=4)

    g = parser.add_argument_group("Database Settings")
    g.add("--host", default="localhost", type=str)
    g.add("--port", default=3306, type=int)
    g.add("--db", default="wiki", type=str)
    g.add("--user", default="wiki", type=str)
    g.add("--passwd", required=True, type=str)
    g.add("--charset", default="utf8", type=str)
    g.add("--init_script", default="sql/init.sql", type=str)
    g.add("--limit", default=None, type=int)

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
    def _resolve(ttl):
        if ttl in ttl2bid:
            return ttl2bid[ttl]

        if ttl in redirects:
            return _resolve(redirects[ttl])

        return None

    try:
        return _resolve(ttl)
    except RecursionError:
        return None

def _process(x):
    global fltr, db

    title, body, aid = x

    aid = int(aid)
    match = REDIRECT_PAT.match(body)

    if match:
        rdr_ttl = match.group(1)

        return title, rdr_ttl, aid

    body = fltr(body)

    bid = db.insert("bodies", {
        "body": body
    }, auto_column="id", ignore_errors=True)

    if bid is None:
        return None

    db.insert("articles", {
        "title": title,
        "body": bid,
        "aid": aid
    }, auto_column="id", ignore_errors=True)

    db.commit()

    return title, bid

def _store_redirect(redirects, aids, articles):
    global db, fltr

    for ttl, rdr_ttl in tqdm.tqdm(redirects.items(), desc="resolving and storing redirects"):
        bid = resolve(ttl, redirects, articles)

        # Failed to resolve
        # The page to which `ttl` redirects is probably omitted for
        # some unknown reason.
        # Or maybe there was recursion.
        if bid is None:
            continue

        aid = aids[ttl]

        db.insert("articles", {
            "title": ttl,
            "body": bid,
            "aid": aid
        }, auto_column="id", ignore_errors=True)

    db.commit()

def dbfy(path, db_init, fltr_init, limit, *args, **kwargs):
    global db, fltr

    db = db_init()
    fltr = fltr_init()
    redirects = {}
    redirects_aid = {}
    ttl2bid = {}
    count = 0

    with bz2.BZ2File(path, "r") as f:
        it = gensim.corpora.wikicorpus.extract_pages(f, ("0",))

        for x in tqdm.tqdm(it, desc="storing articles"):
            x = _process(x)

            if not hasattr(x, "__len__"):
                continue

            if len(x) == 3:
                ttl, rdr_ttl, aid = x
                redirects[ttl] = rdr_ttl
                redirects_aid[ttl] = aid
            elif len(x) == 2:
                ttl, bid = x
                ttl2bid[ttl] = bid

            count += 1

            if limit is not None and count >= limit:
                break

    _store_redirect(redirects, redirects_aid, ttl2bid)

def dbfy_mp(path, db_init, fltr_init, n_processes, limit, *args, **kwargs):
    db = db_init()
    redirects = {}
    redirects_aid = {}
    ttl2bid = {}

    def _pool_init(dbi, fli):

        global db, fltr
        db = dbi()
        fltr = fli()

    _pool_init(db_init, fltr_init)
    pool = mp.Pool(n_processes, _pool_init, (db_init, fltr_init))
    count = 0

    with bz2.BZ2File(path, "r") as f:
        it = gensim.corpora.wikicorpus.extract_pages(f, ("0",))

        with tqdm.tqdm(desc="storing articles with multiprocessing") as t:
            for group in gensim.utils.chunkize(it, 40 * n_processes):
                for x in pool.imap(_process, group):
                    if not hasattr(x, "__len__"):
                        continue

                    if len(x) == 3:
                        ttl, rdr_ttl, aid = x
                        redirects[ttl] = rdr_ttl
                        redirects_aid[ttl] = aid
                    elif len(x) == 2:
                        ttl, bid = x
                        ttl2bid[ttl] = bid

                t.update(len(group))
                count += len(group)

                if limit is not None and count >= limit:
                    break

    _store_redirect(redirects, redirects_aid, ttl2bid)


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
    n_processes = args.n_processes
    limit = args.limit

    remove_html = args.remove_html
    valid_unichrs = args.valid_unichrs
    invalid_unichrs = args.invalid_unichrs

    # URL Check might be unnecessary.
    # if not check_url(url):
    #     raise ValueError("URL must be of valid format. ")

    def db_init():
        return Database(host=host, port=port, db=dbname, user=username,
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
        db_init().execute_script(init_path)

    def fltr_init():
        return WikiBodyFilter(remove_html, valid_unichrs, invalid_unichrs)

    print("Downloading Wikipedia article dump from '{}'...".format(url))
    dmp_path, should_remove = download_dump(url)

    if n_processes == 1:
        dbfy_fn = dbfy
    else:
        dbfy_fn = dbfy_mp

    print("Parsing and storing articles to mysql...")
    dbfy_fn(dmp_path, db_init, fltr_init, n_processes=n_processes, limit=limit)

    print("Cleaning up...")
    cleanup(dmp_path, should_remove)

    print("Done!")


if __name__ == '__main__':
    main()
