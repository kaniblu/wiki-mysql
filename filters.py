# encoding: utf-8

from __future__ import unicode_literals

import re
import string
import six

import gensim


class WikiTextFilter(object):
    """Generic wikipedia article filter

    Strips off illegal characters and markups. Borrows some basic logic from 
    gensim utils.
    """

    def __init__(self, remove_html=True, valid_unicodes=(), invalid_unicodes=()):
        self.valid_unicodes = valid_unicodes
        self.invalid_unicodes = invalid_unicodes
        self.remove_html = remove_html
        self.uni_patterns = []

        if valid_unicodes:
            valids = []
            for s, e in valid_unicodes:
                s_str = six.unichr(s)
                e_str = six.unichr(e)
                valids.append("{}-{}".format(s_str, e_str))

            valid_pat = re.compile(r"[^{}]".format("".join(valids)),
                                   re.UNICODE)
            self.uni_patterns.append(valid_pat)

        if invalid_unicodes:
            invalids = []
            for s, e in invalid_unicodes:
                s_str = six.unichr(s)
                e_str = six.unichr(e)

                invalids.append("{}-{}".format(s_str, e_str))

            invalid_pat = re.compile(r"[{}]".format("".join(invalids)),
                                     re.UNICODE)
            self.uni_patterns.append(invalid_pat)

        dbws_pat = re.compile(r"(\s)\s*")

        self.dbws_pattern = dbws_pat

    def __call__(self, text):
        text = gensim.utils.to_unicode(text, "utf8", errors="ignore")

        if self.remove_html:
            text = gensim.utils.decode_htmlentities(text)

        text = gensim.corpora.wikicorpus.remove_markup(text)

        for pat in self.uni_patterns:
            text = pat.sub("", text)

        text = self.dbws_pattern.sub(r"\g<1>", text)

        return text