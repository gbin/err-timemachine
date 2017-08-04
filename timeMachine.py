# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from whoosh.query import DateRange

from errbot import botcmd, BotPlugin
import os
from whoosh.index import create_in, open_dir
from whoosh.fields import DATETIME, TEXT, ID, Schema
from config import BOT_DATA_DIR
from whoosh.qparser import QueryParser

SUBDIR = "timemachine_index"
ONE_DAY = timedelta(days=1)
ONE_HOUR = timedelta(hours=1)
FIVE_MINUTE = timedelta(minutes=5)


class TimeMachine(BotPlugin):
    """
    This is a plugin recording your chat history in a lucene index
    """
    min_err_version = '4.0.0'
    active_poll = None

    def activate(self):
        folder = os.path.join(BOT_DATA_DIR, "timemachine_index")
        if os.path.exists(folder):
            self.log.debug("Loading the index from %s" % folder)
            self.ix = open_dir(folder)
        else:
            schema = Schema(ts=DATETIME(stored=True), from_node=ID(stored=True), from_domain=ID(stored=True), from_resource=ID(stored=True),
                            to_node=ID(stored=True), to_domain=ID(stored=True), to_resource=ID(stored=True), body=TEXT(stored=True))
            os.mkdir(folder)
            self.log.debug("Created a new index in %s" % folder)
            self.ix = create_in(folder, schema)
        self.parser = QueryParser("body", self.ix.schema)  # body as the default field, can be overriden by the query itself
        super(TimeMachine, self).activate()

    def deactivate(self):
        super(TimeMachine, self).deactivate()
        self.ix.close()

    def search(self, q):
        with self.ix.searcher() as searcher:
            result = [dict(result) for result in searcher.search(q, limit=100)]
        return sorted(result, key=lambda d: d['ts'])

    @botcmd(template='query_results')
    def q(self, mess, args):
        """ Query the timemachine with a lucene query. Type !help q for examples.

 !q ts:20120826                         # returns all the messages of 2012-08-26
 !q ts:[20120826 TO 20120827]           # returns all the messages between 2012-08-26 and 2012-08-27
 !q blah                                # returns all the messages with the body "blah"
 !q blah AND ts:[20120826 TO 20120827]  # combinations of the above

 The fields you can query on are : 'ts' as DATETIME, 'body' as TEXT, 'from_node' as ID, 'from_domain' as ID, 'from_resource' as ID,
                             'to_node' as ID, 'to_domain' as ID, 'to_resource' as ID

The bot tells you explicitely what he understood from your query at the top of the results.
"""
        q = self.parser.parse(args)
        return {'query': q, 'results': self.search(q)}

    @botcmd(template='query_results')
    def lastday(self, mess, args):
        """ Return what was said within the 24 hours
        """

        now = datetime.now()
        q = DateRange('ts', now - ONE_DAY, now)
        return {'query': q, 'results': self.search(q)}

    @botcmd(template='query_results')
    def lasthour(self, mess, args):
        """ Return what was said within the last hour
        """
        now = datetime.now()
        q = DateRange('ts', now - ONE_HOUR, now)
        return {'query': q, 'results': self.search(q)}

    @botcmd(template='query_results')
    def justnow(self, mess, args):
        """ Return what was just said within the last 5 minutes
        """
        now = datetime.now()
        q = DateRange('ts', now - FIVE_MINUTE, now)
        return {'query': q, 'results': self.search(q)}

    def callback_message(self, mess):
        body = mess.body
        if not body:
            return

        from_identity = mess.frm
        to_identity = mess.to
        self.log.debug("Index message from %s to %s [%s]" % (from_identity, to_identity, body))
        with self.ix.writer() as writer:
            writer.add_document(ts=datetime.now(),
                                 from_node=from_identity.node,
                                 from_domain=from_identity.domain,
                                 from_resource=from_identity.resource,
                                 to_node=to_identity.node,
                                 to_domain=to_identity.domain,
                                 to_resource=to_identity.resource,
                                 body=body)
