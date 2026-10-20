"""Child process for the interrupted-purchase test: the simulator's own writer
(`Database.apply`, one transaction per purchase) applies one synthetic purchase.

The test holds a row lock on sim.sim_state, so the writer's last statement (the cursor
UPDATE) blocks after the customer/order/item/payment INSERTs, and the test SIGKILLs
this process there. Printing "applied" means the lock was not held: exit code 5.

argv: <conninfo json> <product_id> <seller_id> <tag>
"""

from __future__ import annotations

import json
import sys

from simulator.db import Database
from tests.helpers.olist_mini import synthetic_purchase


def main() -> int:
    conninfo = json.loads(sys.argv[1])
    product_id, seller_id, tag = sys.argv[2:5]
    purchase, renderer, _oid, _cid = synthetic_purchase(product_id, seller_id, tag)
    db = Database(conninfo)
    db.connect()
    print("connected", flush=True)
    db.apply([purchase], renderer)
    print("applied", flush=True)
    return 5


if __name__ == "__main__":
    sys.exit(main())
