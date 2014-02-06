xdzk
====
This is a prototype of XD distributed runtime clustering on ZooKeeper.

To start an Admin node, run: xdzk.Admin.main()

To start a Container node, run: xdzk.Container.main()

By default, both will expect to connect to ZooKeeper via port 2181 on localhost (set clientPort=2181 in zoo.cfg).

Either one will generate the /xd/containers znode if not already present.

Upon startup, each Container node will write its attributes to an ephemeral znode under /xd/containers using its generated UUID for the name of that node.

The Admin server has a watch on the /xd/containers znode, so it will receive and log an event for each new Container.

If a Container is stopped, then after a 15 second session timeout, the Admin will log its departure.

