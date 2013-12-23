# -*- coding: iso-8859-1 -*-
import yaml, codecs, sys, os.path, optparse

from pyinotify import ProcessEvent
import etcd


class ContainerRegister(ProcessEvent):
    def __init__(self, logger, config):
       self.logger = logger
       self.container_base = config['monitor_dir']
       self.ins_file = config['instance_file']
       self.etcd = etcd.Etcd(config['registry_server_ip'], config['registry_server_port'])

    def process_default(self, event):
        """
        override default processing method
        """
        self.logger.debug('Process::process_default')
        # call base method
        super(Process, self).process_default(event)
    
    def process_IN_MOVED_TO(self, event):
        """
        process 'IN_MOVED_TO' events 
        """
        self.logger.debug('Process::process_IN_MOVED_TO')
        super(Process, self).process_default(event)
        fl = yaml.load(file(self.ins_file,'rb').read())
        self.register_with_etcd(fl)

    def register_with_etcd(self, ins):
        """
        process make bns_path 
        """
        bns_offset = 10000*int(ins['application_db_id'])+int(ins['instance_index'])*10+int(self.clusterid)
        bns_node = ins['tags']['bns_node']
        bnsset = bns_node.split('-')
        bns_name = '%s-%s-%s'%(bnsset[0],bnsset[1],bnsset[2])
        bns_path = "%s/%s.%s.%s" %(self.bns_base, bns_offset,bns_name,self.clustersuf)
        return bns_path

    def container_dict(self, instances):
        cdict = {}
        for ins in instances:
            handle = ins['warden_handle']
            cdict[handle] = {}
            cdict[handle]['state'] = ins['state']
            cdict[handle]['app_id'] = ins['application_db_id'] 
            cdict[handle]['app_name'] = ins['application_name']
            cdict[handle]['state_timestamp'] = ins['state_timestamp']
        return cdict

    def register_with_etcd(self, agent_data):
        """
        check the container information
        """
        self.logger.info('Starting checking the container.')
        containers = os.listdir(self.container_base)
        containers.delete('tmp')

        containers_in_dea = container_dict(agent_data['instances'])

        handles_in_dea = containers_in_dea.keys()
        
        extra = handles_in_dea - containers
        for handle in extra:
            #delete_from_etcd
            key = containers_in_dea[handle]['app_id'] + '/' + handle
            self.etcd.delete(key)
            self.logger.info('delete extra container %s' %handle)
        for handle in containers:
            #register_to_etcd(handle, json)
            state_key = containers_in_dea[handle]['app_id'] + '/' + handle + '/' + 'state'
            state_value = containers_in_dea[handle]['state']

            timestamp_key = containers_in_dea[handle]['app_id'] + '/' + handle + '/' + 'timestamp'
            timestamp_value = containers_in_dea[handle]['state_timestamp'] 
            self.etcd.set(state_key, state_value)
            self.etcd.set(timestamp_key, timestamp_value)

        
