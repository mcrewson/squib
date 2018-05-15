# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# Copyright 2012 Mark Crewson <mark@crewson.net>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import psycopg2
from psycopg2 import extras
import sys

from mccorelib.config     import ConfigError
from squib.oxidizers.base import PeriodicOxidizer

##############################################################################

class PGBouncerOxidizer (PeriodicOxidizer):


    def setup (self):
        super(PGBouncerOxidizer, self).setup()
        self.setup_pgbouncer_connection()

    def setup_pgbouncer_connection (self):
        self.connection_string = self.config.get('connection_string')
        if self.connection_string is None:
            raise ConfigError('%s::connection_string must be specified' % self.name) 
        self.conn = None

    def run_once (self):
        cursor = self.get_cursor()
        if cursor is None: return
        cursor.execute('SHOW POOLS')
        for row in [ dict(r) for r in cursor ]:
            if row['database'] == 'pgbouncer': continue
            prefix = '%s.%s.%s.' % (self.name, row['database'], row['user'])
            print prefix + 'cl_active gauge %d' % row['cl_active']
            print prefix + 'cl_waiting gauge %d' % row['cl_waiting']
            print prefix + 'sv_active gauge %d' % row['sv_active']
            print prefix + 'sv_idle gauge %d' % row['sv_idle']
            print prefix + 'sv_used gauge %d' % row['sv_used']
            print prefix + 'sv_tested gauge %d' % row['sv_tested']
            print prefix + 'sv_login gauge %d' % row['sv_login']
            print prefix + 'maxwait_us hist %d' % row['maxwait_us']

        cursor.execute('SHOW STATS_TOTALS')
        for row in [ dict(r) for r in cursor ]:
            if row['database'] == 'pgbouncer': continue
            prefix = '%s.%s.' % (self.name, row['database'])
            print prefix + 'xact_count derivmeter %d' % row['xact_count']
            print prefix + 'query_count derivmeter %d' % row['query_count']
            print prefix + 'bytes_received derivmeter %d' % row['bytes_received']
            print prefix + 'bytes_sent derivmeter %d' % row['bytes_sent']
            print prefix + 'xact_time derivmeter %d' % row['xact_time']
            print prefix + 'query_time derivmeter %d' % row['query_time']
            print prefix + 'wait_time derivmeter %d' % row['wait_time']

	sys.stdout.flush()

    def get_cursor (self):
        if self.conn is not None: return self.conn.cursor()
        try:
            self.conn = psycopg2.connect(self.connection_string, cursor_factory=extras.DictCursor)
            self.conn.autocommit = True
            return self.conn.cursor()            
        except psycopg2.OperationalError as error:
            sys.stderr.write("%s failed to obtain a cursor: %s\n" % (self.name, error))
            return None

##############################################################################

if __name__ == "__main__":
    PGBouncerOxidizer('pgbouncer', dict(period=5.0, connection_string='host=localhost port=5432 dbname=pgbouncer user=stats password=stats')).run()

##############################################################################
## THE END
