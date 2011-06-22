#!/usr/bin/env python

# seriously, fuck python 2.5
from __future__ import with_statement

import gevent
import gevent.backdoor
import gevent.coros
import gevent.monkey
import gevent.pool
import gevent.queue
import mechanize
import logging
import resource
import urlparse
import urllib2
import optparse
import fileinput
import os
import sys
import hashlib

useragent = "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"

gevent.monkey.patch_all()

log = logging.getLogger("fetching")
logging.basicConfig(
                    stream=sys.stderr,
                    level=logging.INFO,
                    format = '%(asctime)s %(levelname)s %(message)s'
                   )

class ShortReadError(RuntimeError): pass
class TimeoutError(RuntimeError): pass

class Browser(object):
    def __init__(self):
        self.br = mechanize.Browser()
        self.br.set_handle_robots(False)
        self.set_user_agent(useragent)

    def set_user_agent(self, ua):
        self.br.addheaders = [('User-Agent', ua)]

    def open(self, url, data=None):
        resp = self.br.open_novisit(url, data)
        data = resp.read()
        resp.seek(0,0)
        resp.close()
        return resp


def get_content_len(resp):
    try:
        return int(resp.info()['content-length'])
    except (ValueError,KeyError):
        return -1

def get_resp_len(resp):
    l = len(resp.read())
    resp.seek(0,0)
    return l


class Tokens(gevent.coros.Semaphore):
    def __init__(self, value=1):
        self.max_value = value
        super(Tokens, self).__init__(value)

    def resize(self, nsize):
        if nsize == self.max_value:
            return
        elif nsize > self.max_value:
            self.counter += nsize - self.max_value
        else:
            self.counter -= self.max_value - nsize
        self.max_value = nsize


class Fetchers(object):
    page_timeout = 600.0

    def __init__(self, outq, count=1000, ntries=3):
        self.urlq = gevent.queue.JoinableQueue()
        self.pageq = outq
        self.ntries = ntries
        self.count = count
        self.fetched = set()
        self.errored = set()

        self.fetchers = gevent.pool.Pool(size = count)
        self.network_tokens = Tokens(count)

        for i in xrange(count):
            self.fetchers.spawn(self.fetcher_proc)

    def set_size(self, count):
        self.network_tokens.resize(count)

        if count > self.count:
            for i in xrange(count-self.count):
                self.fetchers.spawn(self.fetcher_proc)

        self.count = count

    def fetch_page(self, br, url):
        try:
            with gevent.Timeout(self.page_timeout, TimeoutError):
                return br.open(url)
        except urllib2.HTTPError, err:
            if err.getcode() in (404, 403):
                log.debug("Got %d for %s", err.getcode(), url)
            else:
                log.error("Weird error %d for %s", err.getcode(), url)
            raise
        except urllib2.URLError, err:
            log.error("URLError: %r for %s", err, url)
            raise
        except TimeoutError:
            log.debug("Timeout fetching page: %s", url)
            raise
        except Exception, e:
            log.error("Unknown error %r for %s", e, url)
            raise

    def fetcher_proc(self):
        br = Browser()

        while True:
            url, tries = self.urlq.get()

            if url.upper() in self.fetched:
                log.debug("already fetched: %s", url)
                continue
            if url.upper() in self.errored:
                log.debug("errored out already: %s", url)

            log.info("fetching %s [%d]", url, tries)

            try:
                with self.network_token:
                    resp = self.fetch_page(br, url)

                if get_content_len(resp) > get_resp_len(resp):
                    raise ShortReadError("Expected %d bytes, but got %d" %
                               (get_content_len(resp), get_resp_len(resp)))

                log.debug("success: %s", url)
                self.fetched.add(url.upper())
                self.pageq.put((url, resp))

            except Exception, err:
                tries -= 1
                if tries >= 0:
                    self.urlq.put((url, tries))
                else:
                    log.info("Excessive failures, skipping: %s", url)
                    self.errored.add(url.upper())

            finally:
                self.urlq.task_done()

    def fetch(self, url):
        self.urlq.put((url, self.ntries))
        gevent.sleep(0)

    def fetch_all(self, urls):
        for url in urls:
            self.fetch(url)
    ifetch = fetch_all

    def join(self):
        return self.urlq.join()


class Processors(object):
    def __init__(self, count=20):
        self.pool = gevent.pool.Pool(size=count)
        self.queue = gevent.queue.JoinableQueue()

        self.md5sum = False
        self.output_dir = None
        self.clobber = True
        self.report = False
        self.outfp = None

        for i in xrange(count):
            self.pool.spawn(self._process_worker)

    def _process_worker(self):
        while True:
            url, response = self.queue.get()

            try:
                self._process(url, response)
                log.info("process success %s", url)
            except Exception, e:
                log.error("process fail %s : %r", url, e)
            finally:
                self.queue.task_done()

    def process(self, url, response):
        self.queue.put((url, response))
        gevent.sleep(0)

    def put(self, (url, response)):
        return self.process(url, response)

    def qsize(self):
        return self.queue.qsize()

    def get_output_fname(self, url, resp, data):
        path = urlparse.urlparse(resp.geturl()).path
        name = os.path.basename(path)

        if self.md5sum:
            fname = "%s" % hashlib.md5(data).hexdigest()
            root,ext = os.path.splitext(path)
            if ext is not None:
                fname += ext
        else:
            fname = name

        if self.output_dir:
            fname = os.path.join(self.output_dir, fname)

        return fname

    def _process(self, url, response):
        data = response.read()

        fname = self.get_output_fname(url, response, data)

        if os.path.exists(fname):
            if self.md5sum:
                return

            if not self.clobber:
                fn = fname
                i = 1
                while os.path.exists(fn):
                    fn = "%s.%d" % (fname, i)
                    i+= 1
                fname = fn

        log.info("saving (%s) to '%s'", url, fname)
        try:
            with open(fname, "wb") as fp:
                fp.write(data)
            self.log_success(url,fname,len(data))
        except:
            log.error("Saving file failed: %s", fname)
            return
        if self.report:
            sys.stdout.write("%s\n" % fname)

    def log_success(self, url, fname, fsize):
        if self.outfp is not None:
            self.outfp.write("%s,%s,%d\n" % (url,fname,fsize))

    def join(self):
        self.queue.join()
        self.outfp.close()
        return


def set_max_fds(maxfds=8180):
    nfd,tot = resource.getrlimit(resource.RLIMIT_NOFILE)

    # if we want fewer than the maximum num of FDs, then just return
    if maxfds < nfd:
        return maxfds

    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (maxfds, tot))
    except:
        pass
    nfd,tot = resource.getrlimit(resource.RLIMIT_NOFILE)

    return nfd

def launch_backdoor(address, locls):
    bckdoor = gevent.backdoor.BackdoorServer(address, locls)
    gevent.spawn(bckdoor.serve_forever)

def parse_args(args):
    parser = optparse.OptionParser()

    parser.add_option("-c", "--tries", help="tries before abandoning request",
                      dest="ntries", default=3)
    parser.add_option("-f", "--file", help="input file for URLS",
                      dest="fname", default=None)
    parser.add_option("-J", "--concurrent", help="number of fetcher threads",
                      dest="concurrent", default=1024)
    parser.add_option("-d", "--dir", help="output directory",
                      dest="output_dir", default=None)
    parser.add_option("--md5", help="rename files to md5sum of content",
                      dest="md5sum", action="store_true", default=False)
    parser.add_option("--noclobber", help="Don't overwrite existing files",
                      action="store_true", default=False)
    parser.add_option("--backdoor", dest="port", help="management via telnet",
                      action="store", default=None)
    parser.add_option("--timeout", help="timeout for each page (in SECS)",
                      default=600)
    parser.add_option("--stdout", dest="report", help="report files on STDOUT",
                      action="store_true", default=False)
    parser.add_option("--log", dest="output_log", help="keep download log",
                      default=None)

    opts,args = parser.parse_args(args)
    return opts,args

def main(args):
    opts, args = parse_args(args)

    processors = Processors()
    processors.output_dir = opts.output_dir

    if opts.output_log:
        processors.outfp = open(opts.output_log, "wb+")

    if opts.noclobber:
        processors.clobber = False
    if opts.md5sum:
        processors.md5sum = True
    if opts.report:
        processors.report = True

    nfds = set_max_fds(int(opts.concurrent))

    fetchers = Fetchers(processors, count=nfds)
    fetchers.ntries = int(opts.ntries)

    if opts.fname is None or opts.fname == "-":
        fp = fileinput.input(args[1:])
    else:
        fp = open(opts.fname)

    if opts.port:
        locls = {
                'fp': fp,
                 'fetchers': fetchers,
                 'processors': processors,
                 'opts': opts
                }
        launch_backdoor(('127.0.0.1', int(opts.port)), locls)

    fetchers.page_timeout = float(opts.timeout)
    fetchers.fetch_all(l.strip() for l in fp)

    fetchers.join()
    processors.join()

if __name__ == "__main__":
    main(sys.argv)
