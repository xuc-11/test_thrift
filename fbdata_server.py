# -*- coding: utf-8 -*-
#!/usr/bin/env python
import sys, glob
sys.path.append('gen-py')

from fbdata import FBDataService
from fbdata.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class FBDataServiceHandler:
    def __init__(self):
        # self.kbfile='ttt.txt'
        # self.kbfile='data/fb.triple.mini'
        self.kbfile='../data2/tripleInFb'
        self.mrfile='../data2/mediator-relations'
        self.kb=self.loadKB(self.kbfile)
        self.mrelations=self.load_mediator_relations(self.mrfile)

    def loadKB(self,file_kb):
        kb={}
        with open(file_kb,'rb') as fin:
            for line in fin:
                ll=line.decode('utf-8').strip().split('\t')
                s=ll[0].encode('utf-8')
                p=ll[1].encode('utf-8')
                o=ll[2].encode('utf-8')
                if not kb.has_key(s):
                    po={}
                    po[p]=[o]
                    kb[s]=po
                else:
                    po=kb[s]
                    if not po.has_key(p):
                        po[p]=[o]
                    else:
                        po[p].append(o)
                    kb[s]=po
        print('load kb succeed!')
        return kb

    def load_mediator_relations(self,file_mr):
        mrelations=set()
        with open(file_mr,'rb') as fin:
            for line in fin:
                ll=line.decode('utf-8').strip()
                mrelations.add(ll.encode('utf-8'))
        print('load mediator-relations succeed!')
        return mrelations

    # return a dict,{relation:[objects]}
    def get_1hop_dict_by_mid(self,mid):
        if mid not in self.kb:
            print(mid+' not find')
            return
        return self.kb[mid]

    # return a list<Triple>,subject in a Triple is mid
    def get_1hop_triple_by_mid(self,mid):
        if mid not in self.kb:
            print(mid+' not find')
            return
        ret=[]
        for k,v in self.kb[mid].iteritems():
            for item in v:
                ret.append(Triple(s=mid,p=k,o=item))
        return ret

    # return a subgraph list<list<Triple>>,if relation is mediator-relation,find 2 hop
    def get_1or2hop_triple_by_mid(self,mid):
        ret=[]
        if mid not in self.kb:
            print(mid+' not find')
            return
        for k,v in self.kb[mid].iteritems():
            if k not in self.mrelations:
                for item in v:
                    alist=[]
                    alist.append(Triple(s=mid,p=k,o=item))
                    ret.append(alist)
            else:
                for item in v:
                    if item not in self.kb:
                        continue
                    for k2,v2 in self.kb[item].iteritems():
                        for j in v2:
                            alist=[]
                            alist.append(Triple(s=mid,p=k,o=item))
                            alist.append(Triple(s=item,p=k2,o=j))
                            ret.append(alist)
        return ret

    # return paths between mid1 and mid2, if relation is mediator-relation,find 2 hop paths
    def get_paths_from_2mid(self,mid1,mid2):
        if mid1 not in self.kb:
            print(mid1+' not find')
            return
        ret=[]
        for k,v in self.kb[mid1].iteritems():
            if mid2 in v:
                alist=[]
                alist.append(Triple(s=mid1,p=k,o=mid2))
                ret.append(alist)
            if k in self.mrelations:
                for item in v:
                    if item not in self.kb:
                        continue
                    for k2,v2 in self.kb[item].iteritems():
                        if mid2 in v2:
                            alist=[]
                            alist.append(Triple(s=mid1,p=k,o=item))
                            alist.append(Triple(s=item,p=k2,o=mid2))
                            ret.append(alist)
        return ret
try:
    handler = FBDataServiceHandler()
    processor = FBDataService.Processor(handler)
    transport = TSocket.TServerSocket(port=1212)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print 'Starting the server...'
    server.serve()
    print 'done.'

except Exception as e:
    print e
