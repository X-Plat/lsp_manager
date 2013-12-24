#!/user/bin/env python

import etcd


client = etcd.Client('10.36.166.46', 4001)

print client.get('/containers/conn2').key['value']



