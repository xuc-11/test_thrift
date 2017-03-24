# -*- coding: utf-8 -*-
#!/usr/bin/env python
import sys, glob
sys.path.append('gen-py')

from fbdata import FBDataService
# from fbdata.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class FBDataClient:
    def __init__(self):
        self.port=1212

    def open_transport(self):
        transport = TSocket.TSocket('localhost', self.port)
        self.transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FBDataService.Client(protocol)
        self.transport.open()
        return client

    def close_transport(self):
        self.transport.close()

if __name__ == '__main__':

    try:
        fbc=FBDataClient()
        fbclient=fbc.open_transport()
        ret1=fbclient.get_1or2hop_triple_by_mid('m.02j8rx')
        for item in ret1:
            print(item)
    except Exception as e:
        print e

# try:
#     transport = TSocket.TSocket('localhost', 1212)
#     transport = TTransport.TBufferedTransport(transport)
#     protocol = TBinaryProtocol.TBinaryProtocol(transport)
#     client = FBDataService.Client(protocol)
#
#     transport.open()

#     ret1=client.get_1hop_dict_by_mid('m.0100zby9')
#     print(ret1)
#     ret2=client.get_1hop_triple_by_mid('m.0100zby9')
#     for item in ret2:
#         print(item)
#
#     transport.close()
# except Exception as e:
#     print e
