# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import pytest

from mo_dots import Null
from mo_logs import Log
from mo_threads import Thread, Till
from tuid.service import TUIDService
from mo_dots import Null, wrap

_service = None


@pytest.fixture
def service(config, new_db):
    global _service
    if new_db == "yes":
        return TUIDService(database=Null, start_workers=False, kwargs=config.tuid)
    elif new_db == "no":
        if _service is None:
            _service = TUIDService(kwargs=config.tuid, start_workers=False)
        return _service
    else:
        Log.error("expecting 'yes' or 'no'")


def test_caching(service):
    timeout_seconds = 60
    # Partition is to make sure threads are taken care by the caller itself
    partition_size = 5
    # Store all the revisions in all_revisions table
    # create table all_revisions(
    #     revision CHAR(12) NOT NULL,
    #     done CHAR(5),
    #     primary key(revision)
    # );
    # Initially do inserts: insert into all_revisions values('d63ed14ed622', 'false');
    # Once it is taken by a process update it: update all_revisions set done='true' where revision = 'd63ed14ed622';


    # with service.conn.transaction() as t:
    #     t.execute("INSERT OR IGNORE INTO all_revisions (revision, done) VALUES all the revisions")
    #
    # revision = service.conn.get_one("SELECT revision FROM all_revisions WHERE done=?", ('false',))[0]

    # Get revisions backward from the current revision
    service.clogger._fill_in_range(5, 'd63ed14ed622')

    query = {
        "_source": {"includes": ["revnum", "revision"]},
        "query": {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "done"
                    }
                }
            }
        },
        "sort": [{"revnum": {"order": "desc"}}],
        "size":100
    }
    result = service.clogger.csetlog.search(query)
    for r in result.hits.hits:
        revision = r._source.revision
        revnum = r._source.revnum
        # Testing
        if int(revnum/10) == 0:
            continue
        if revnum == 11:
            continue
        branch = service.config.hg.branch
        # Takes all the files changed in this revision
        files = service.hg_cache.get_revision(
            wrap({"changeset": {"id": revision}, "branch": {"name": branch}}),
            None,
            False,
            False,
        ).changeset.files
        num_of_files = len(files)
        if num_of_files == 0:
            continue
        num_of_threads = int(num_of_files/partition_size)+1

        # Call service on multiple threads at once
        tuided_files = [None] * num_of_threads
        threads = [
            Thread.run(
                str(i),
                service.mthread_testing_get_tuids_from_files,
                files[i*partition_size:(i+1)*partition_size],
                revision,
                tuided_files,
                i,
            )
            for i, a in enumerate(tuided_files)
        ]
        too_long = Till(seconds=timeout_seconds)
        for t in threads:
            t.join(till=too_long)

        assert not too_long

        # Update done = true
        updated_record = service.clogger._make_record_csetlog(revnum, revision, -1)
        updated_record["value"].update({"done": "true"})
        service.clogger.csetlog.add(updated_record)
        # with service.conn.transaction() as t:
        #     t.execute("UPDATE all_revisions SET done='true' WHERE revision = ?", (revision,))

        #revision = service.conn.get_one("SELECT revision FROM all_revisions WHERE done=?", ('false',))[0]
    assert True




    def mthread_caching_get_tuids_from_files(
        self,
        files,
        revision,
        results,
        res_position,
        going_forward=False,
        repo=None,
        please_stop=None,
    ):
        Log.note("Thread {{pos}} is running.", pos=res_position)
        # Interested to know only it completed or not
        _, results[res_position] = self.get_tuids_from_files(
            files, revision, going_forward=going_forward, repo=repo
        )
        Log.note("Thread {{pos}} is ending.", pos=res_position)
        return
