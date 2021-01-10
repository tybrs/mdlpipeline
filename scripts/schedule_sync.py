import json
import asyncio
from mdlpipeline.sync.enrollments import SyncEnrollments
from datetime import datetime
from sched import scheduler
from time import time, sleep
import pytest

def sync_daemon(local_handler):
    """Loads course mapping file, runs pull to find add/drops
    for webCampus's Conduit system, pushes file to SFTP to
    sftp02-na.blackboardopenlms.com/webcampus.uws.edu/conduit/.
    """
    with open('data/wc_pc_mapping_wi21.json', 'r') as f:
        MAPPING_JSON = json.load(f)
    # Data pull to find add/drops for webCampus's Conduit system
    se = asyncio.run(SyncEnrollments.pull(MAPPING_JSON))
    # Log Conduit file in log directory
    se.log_conduit()
    # Push file via SFTP
    se.push_conduit()
    local_handler.enter(15*60, 1, sync_daemon, (local_handler,))

if __name__ == '__main__':
    handler = scheduler(time, sleep)
    handler.enter(0, 1, sync_daemon, (handler,))
    handler.run()
    main()