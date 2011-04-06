#!/usr/bin/env python

from pybing.query import FileTypeQuery
import fileinput
import optparse
import sys



def do_query(word, filetype, key):

    query = FileTypeQuery(key, word, filetype.upper())

    try:
        for r in query.execute():
            try:
                yield r.url.decode('utf-8').encode('utf-8')
            except:
                pass
    except:
        pass

def parse_args(args):
    parser = optparse.OptionParser("%prog [-k] [-t] 'SEARCH TERMS'")
    parser.add_option("-k", "--key", help="Bing API key",
                      default="")
    parser.add_option("-t", "--filetype", help="File type e.g. [DOC, PDF,...]",
                     default="DOC")
    parser.add_option("-Q", "--query", help="query words", default=None)

    opts, args = parser.parse_args(args)

    return opts, args

def main(args):
    opts, args = parse_args(args)

    if opts.query is not None:
        queries = [opts.query]
    else:
        queries = fileinput.input(args[1:])

    for query in queries:
        for url in do_query(query, opts.filetype, opts.key):
            print url

    return 0

if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
