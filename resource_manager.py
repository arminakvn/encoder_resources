import redis
import io
import json
import math
import random
import time
from Queue import Queue
import httplib2
import os



class ResourceManager(object):
    def __init__(self,infile_path):
        print("init ResourceManager")

        self.application_state = dict()
        self.redis_client = redis.StrictRedis(host='redis', port=6379)
        self.agent_id = self.decideRoleGetAgentId()
        self.infile_path = infile_path
        self.fw_path = self.infile_path.replace("txt","csv")


    def decideRoleGetAgentId(self):
        if self.redis_client.get("nbatch") is None:
            # self.role = "manager"
            agent_id = 0

        else:
            # self.role = "agent"
            agent_id = self.redis_client.incr("agent_id")
        self.agent_id = agent_id
        return agent_id



    def setNbatch(self):
        # get the file, get the n rows
        m_lines = sum(1 for line in io.open("{}".format(self.infile_path), encoding='ascii', errors='ignore'))
        nbatch = (m_lines / 1000) + 1
        self.redis_client.set("nbatch", nbatch) 
        self.redis_client.expire("agent_id", 0)
        self.redis_client.expire("nbatch", 250)



    def getSource(self):
        from_line = (self.agent_id - 1) * 1000
        to_line = self.agent_id * 1000
        counter = 0
        line_pairs = list()
        with io.open('rm0to2000.txt', encoding='ascii', errors='ignore') as source:#bbox_fua_sub
            for line in source:
                if (counter >= from_line) and (counter < to_line):
                    line=line.replace("\n","").replace('"','')
            #         print line
                    split=line.split('\t')
                    address = split[1]
                    address_id = split[0]
                    line_pairs.append({'address': address, 'address_id': address_id})
                    counter = counter + 1
                else:
                    counter = counter + 1
        return line_pairs


    def process(self):

        # get dataset/batch work to do
        # geocodeall of it
#         e = EnCoderAgent()
        address_field_name = "address"
        zipcode_field_name = "ZIP_mmc"
        muni_field_name = "CITY"
        from_n = (self.agent_id - 1) * 1000
        to_n = self.agent_id * 1000
        self.fw=open("results{0}to{1}.csv".format(from_n,to_n),'a')
        self.fw.write("address_id,"+"ma_lp_id")
        self.fw.write("\n")
        source = self.getSource()
        for li in range(0, len(source)):
            line = source[li]
            address = line["address"]
            address_id = line["address_id"]
            print "address_id: ",address_id
            print "address: ", address
            try:
                print "e.process()"
#                 res = e.process(address)
#                 ma_lp_id = res.ma_lp_id.tolist()[0]
            except:
                ma_lp_id = "not found"
            ## save
#             self.fw.write("{0},".format(address_id)+"{0}".format(ma_lp_id))
#             self.fw.write("\n")
#             self.fw.flush()
            ## save
        self.fw.close()

    def manage(self):
        agent_id = self.decideRoleGetAgentId()
        if agent_id > 0:
            print "processing"
            self.process()
        else:
            print "administrating :: making batches"
            self.setNbatch()
        print "end of manage"





    