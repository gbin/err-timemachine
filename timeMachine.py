# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import logging
from whoosh.query import DateRange

from errbot import botcmd, BotPlugin
import os
from whoosh.index import create_in, open_dir
from whoosh.fields import DATETIME, TEXT, ID, Schema
from config import BOT_DATA_DIR
from whoosh.qparser import QueryParser

SUBDIR = "timemachine_index"
ONE_DAY = timedelta(days = 1)
ONE_HOUR = timedelta(hours = 1)
FIVE_MINUTE = timedelta(minutes = 5)

class TimeMachine(BotPlugin):
    min_err_version = '1.6.0'
    active_poll = None

    def activate(self):
        folder = os.path.join(BOT_DATA_DIR, "timemachine_index")
        if os.path.exists(folder):
            logging.debug("Loading the index from %s" % folder)
            self.ix = open_dir(folder)
        else:
            schema = Schema(ts=DATETIME(stored=True), from_node=ID(stored=True), from_domain=ID(stored=True), from_resource=ID(stored=True),
                            to_node=ID(stored=True), to_domain=ID(stored=True), to_resource=ID(stored=True), body=TEXT(stored=True))
            os.mkdir(folder)
            logging.debug("Created a new index in %s" % folder)
            self.ix = create_in(folder, schema)
        self.parser = QueryParser("body", self.ix.schema) # body as the default field, can be overriden by the query itself
        super(TimeMachine, self).activate()

    def deactivate(self):
        super(TimeMachine, self).deactivate()
        self.writer = None
        self.ix.close()

    def search(self, q):
        with self.ix.searcher() as searcher:
            result = [dict(result) for result in searcher.search(q, limit=100)]
        return sorted(result, key=lambda d : d['ts'])


    @botcmd(template = 'query_results')
    def q(self, mess, args):
        """ Query the timemachine with a lucene query. Type !help q for examples.

 !q ts:20120826                         # returns all the messages of 2012-08-26
 !q ts:[20120826 TO 20120827]           # returns all the messages between 2012-08-26 and 2012-08-27
 !q blah                                # returns all the messages with the body "blah"
 !q blah AND ts:[20120826 TO 20120827]  # combinaison of the above

 The fields you can query on are : 'ts' as DATETIME, 'body' as TEXT, 'from_node' as ID, 'from_domain' as ID, 'from_resource' as ID,
                             'to_node' as ID, 'to_domain' as ID, 'to_resource' as ID

The bot tells you explicitely what he understood from your query at the top of the results.
"""
        q = self.parser.parse(args)
        return {'query': unicode(q), 'results' : self.search(q)}

    @botcmd(template = 'query_results')
    def lastday(self, mess, args):
        """ Return what was said within the 24 hours
        """

        now = datetime.now()
        q = DateRange('ts', now - ONE_DAY, now)
        return {'query': unicode(q), 'results' : self.search(q)}

    @botcmd(template = 'query_results')
    def lasthour(self, mess, args):
        """ Return what was said within the last hour
        """
        now = datetime.now()
        q = DateRange('ts', now - ONE_HOUR, now)
        return {'query': unicode(q), 'results' : self.search(q)}

    @botcmd(template = 'query_results')
    def justnow(self, mess, args):
        """ Return what was just said within the last 5 minutes
        """
        now = datetime.now()
        q = DateRange('ts', now - FIVE_MINUTE, now)
        return {'query': unicode(q), 'results' : self.search(q)}


    def callback_message(self, conn, mess):
        body = mess.getBody()
        if not body:
            return

        from_identity = mess.getFrom()
        to_identity = mess.getTo()
        logging.debug("Index message from %s to %s [%s]" % (from_identity, to_identity, body))
        self.writer = self.ix.writer()
        self.writer.add_document(ts=datetime.now(),
                            from_node=unicode(from_identity.getNode()),
                            from_domain=unicode(from_identity.getDomain()),
                            from_resource=unicode(from_identity.getResource()),
                            to_node=unicode(to_identity.getNode()),
                            to_domain=unicode(to_identity.getDomain()),
                            to_resource=unicode(to_identity.getResource()),
                            body=body
        )
        self.writer.commit()



