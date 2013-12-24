# -*- coding: iso-8859-1 -*-
import yaml, codecs, sys, os.path, optparse

from pyinotify import ProcessEvent
import etcd


class ContainerRegister(ProcessEvent):
    def __init__(self, logger, config):
       self.logger = logger
       self.container_base = config['monitor_dir']
       self.ins_file = config['instance_file']
       print config['registry_server_ip']
       print config['registry_server_port']
       self.etcd = etcd.Client(host=config['registry_server_ip'], port=config['registry_server_port'])

    def process_default(self, event):
        """
        override default processing method
        """
        self.logger.debug('ContainerRegister::process_default')
        # call base method
        super(ContainerRegister, self).process_default(event)
    
    def process_IN_MOVED_TO(self, event):
        """
        process 'IN_MOVED_TO' events 
        """
        self.logger.debug('ContainerRegister::process_IN_MOVED_TO')
        super(ContainerRegister, self).process_default(event)
        fl = yaml.load(file(self.ins_file,'rb').read())
        self.register_with_etcd(fl)

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
        #get the container list in container base path
        containers_in_list = os.listdir(self.container_base)
        containers_in_list.remove('tmp')
        #containers = set(containers_in_list)
        containers = set(['war_c1', 'war_c2', 'dea_cm'])
     
        #get the containers managed by dea
        #containers_in_dea = self.container_dict(agent_data['instances'])
        containers_in_dea = {'dea_c1':{'app_id':'dea_c1','state':'CRASHED','ip':'dea_c1','state_timestamp':'dea_c1'},'dea_c2':{'app_id':'dea_c2','state':'RUNNING','ip':'dea_c2','state_timestamp':'dea_c2'}, 'dea_cm':{'app_id':'dea_cm','state':'RUNNING','ip':'dea_cm','state_timestamp':'dea_cm'}}
        handles_in_dea = set(containers_in_dea.keys())
        
       
        extra = handles_in_dea - containers
        print 'extra'
        print extra

        #delete handles that in dea while not in container base path
        for handle in extra:
            app_key = '/apps/' + str(containers_in_dea[handle]['app_id']) + '/' + handle
            container_key = '/containers/' + handle
            try:
                app_res = self.etcd.get(app_key)
            except:
                app_res = None
            print 'app_res in ex',app_res
            if app_res:
                try:
                    self.etcd.delete(app_key)
                    self.logger.info('Delete container %s succ' %handle)
                except:
                    self.logger.warn('Delete %s from etcd failed.'%app_key)

            try:
                con_res = self.etcd.get(container_key)
            except:
                con_res = None
            print 'con_res in e', con_res 
            if con_res:
                try:
                    self.etcd.delete(container_key)
                    self.logger.info('Delete container %s succ' %handle)
                except:
                    self.logger.warn('Delete %s from etcd failed.'%container_key)

        #Remove containers in warden
        missing = containers - handles_in_dea
        print 'missing', missing

        for handle in missing:
            container_key = '/containers/' + handle
            try:
                con_res = self.etcd.get(container_key)
            except:
                con_res = None
            print 'con_res in missing', con_res

            if con_res:
                try:
                    app_id = con_res.key['value']
                    try:
                       app_key = 'apps' + str(containers_in_dea[handle]['app_id']) + '/' + handle
                       self.etcd.delete(app_key)
                       self.logger.info('Delete container %s succ' %handle)
                    except:
                       self.logger.warn('Delete %s from etcd failed.'%app_key)

                    try:
                       self.etcd.delete(container_key)
                    except:
                       self.logger.warn('Delete %s from etcd failed.'%container_key)
                except:
                    self.logger.warn('Get %s from etcd failed.'%container_key)
        
            else:
                self.logger.warn('Get %s from etcd failed.'%container_key)

        #Update container in dea
        active_containers = containers & handles_in_dea
        print 'active , ', active_containers

        for handle in active_containers:
          #try:
            state_key = '/apps/' + str(containers_in_dea[handle]['app_id']) + '/' + handle + '/' + 'state'
            state_value = containers_in_dea[handle]['state']

            timestamp_key = '/apps/' + str(containers_in_dea[handle]['app_id']) + '/' + handle + '/' + 'timestamp'
            timestamp_value = containers_in_dea[handle]['state_timestamp'] 
            print 'state_key', state_key
            print 'state_value', state_value
            print 'timestamp_key', timestamp_key
            print 'timestamp_value', timestamp_value
            try:
               self.etcd.set(state_key, state_value)
            except:
               print 'error'
            container_key = '/containers/' + handle
            self.etcd.set(container_key, str(containers_in_dea[handle]['app_id']))
            self.etcd.set(timestamp_key, timestamp_value)
          #except:
            self.logger.warn('update conatiner failed')



        
