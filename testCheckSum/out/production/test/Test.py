import requests
import json
import re
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

filter_dict = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])


class AmbariCollector:
    cluster = ""

    def get_ansible_formatted_key(self, obj):
        for key in obj.keys():
            new_key = re.sub('[^0-9a-zA-Z-]', '_', key)
            if new_key != key:
                obj[new_key] = obj[key]
                del obj[key]
        return obj

    def __init__(self, proto, host, port, user, password, validate_ssl=False, timeout=60, max_retries=10):
        self.auth = HTTPBasicAuth(user, password)

        self.user = user
        self.password = password
        self.session = requests.Session()
        self.session.verify = validate_ssl
        self.timeout = timeout
        self.proto = proto
        self.host = host
        self.port = port
        self.main_url = proto + '://' + host + ':' + str(port)

        retries = Retry(total=max_retries,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        self.session.mount(proto + '://' + host + ':' + str(port), HTTPAdapter(max_retries=retries))

    def request_api(self, path):
        result = self.session.get(self.main_url + path, auth=self.auth, timeout=self.timeout)
        result.raise_for_status()
        return result.json()

    # Collect host information
    def get_hosts(self, cluster):
        result = {}
        # Collect host information
        url = "/api/v1/clusters/" + cluster + "/hosts?fields=Hosts/ip"
        for item in self.request_api(url)['items']:
            hostname = item['Hosts']['host_name']
            ip = item['Hosts']['ip']
            result.setdefault(hostname, {})
            result[hostname]['ip'] = ip
        return result

    # Collect root component/service information
    def get_root_components(self):
        result = {}
        url = "/api/v1/services/?fields=components/hostComponents/RootServiceHostComponents/service_name"
        for item in self.request_api(url)['items']:
            for component in item['components']:
                for host_components in component['hostComponents']:
                    hostname = host_components['RootServiceHostComponents']['host_name']
                    service_name = host_components['RootServiceHostComponents']['service_name']
                    component_name = host_components['RootServiceHostComponents']['component_name']
                    result.setdefault(component_name, {})
                    result[component_name].setdefault('hostname', [])
                    result[component_name].setdefault('services', service_name)
                    if hostname not in result[component_name]['hostname']:
                        result[component_name]['hostname'].append(hostname)
        return result

    # Collect component/service information
    def get_components(self, cluster):
        result = {}
        url = "/api/v1/clusters/" + cluster + "/components/?fields=host_components/HostRoles/service_name"
        for item in self.request_api(url)['items']:
            for component in item['host_components']:
                hostname = component['HostRoles']['host_name']
                service_name = component['HostRoles']['service_name']
                component_name = component['HostRoles']['component_name']
                result.setdefault(component_name, {})
                result[component_name].setdefault('hostname', [])
                result[component_name].setdefault('services', service_name)
                if hostname not in result[component_name]['hostname']:
                    result[component_name]['hostname'].append(hostname)
        return result

    # is cluster secure
    def is_secure_cluster(self, cluster):
        url = "/api/v1/clusters/" + cluster + "?fields=Clusters/security_type"
        data = self.request_api(url)
        if(data['Clusters']['security_type'] == 'KERBEROS'):
            return True
        else:
            return False

    # get current hdp version
    def get_hdp_version(self, cluster):
        url = "/api/v1/clusters/" + cluster + "/stack_versions?fields=ClusterStackVersions/state,repository_versions/RepositoryVersions/display_name"
        for item in self.request_api(url)['items']:
            if(item['ClusterStackVersions']['state'] == 'CURRENT'):
                return (item['repository_versions'][0]['RepositoryVersions']['display_name'])
        return ''

    # get list of all services
    def get_all_services(self, cluster):
        all_installed_service = []
        url = "/api/v1/clusters/" + cluster + "/services?fields=ServiceInfo/service_name"
        for item in self.request_api(url)['items']:
            all_installed_service.append(item['ServiceInfo']['service_name'])
        return all_installed_service

    # return all the config properties associated with a service
    def get_current_service_config(self, cluster, service_name):
        url = "/api/v1/clusters/" + cluster + "/configurations/service_config_versions?service_name.in(" + service_name + ")&is_current=true"
        response = self.request_api(url)['items'][0]
        str_response = json.dumps(response)
        #str_response = str_response.replace('}}','}}{% endraw %}')
        #str_response = str_response.replace("{{","{% raw %}{{")
        #new_str_response = re.sub('({\{[A-Za-z0-9_]+\}})', '{% raw %}\1{% endraw %}', str_response)
        ###str_response = str_response.replace("{{","/{/{")
        ###str_response = str_response.replace("{%","##")
        #str_response = (re.sub("({|%)({|%|})",'/\\1/\\2',str_response))
        formatted_response = json.loads(str_response, object_hook=self.get_ansible_formatted_key)
        response = {}
        for configuration in formatted_response['configurations']:
            type = configuration['type']
            type = re.sub('[^0-9a-zA-Z]', '_', type )
            for property in configuration['properties']:
                value = configuration['properties'][property]
                value = (re.sub("({|%)({|%|})",'/\\1/\\2',value))
                print property
                configuration['properties'][property] = value
                response[type]['properties'][property] = value

        return response



        # return config of a component within a service
    def get_current_component_config(self, cluster, service_name , type):
        result = {service_name: {type: {}}}
        url = "/api/v1/clusters/" + cluster + "/configurations/service_config_versions?service_name.in(" + service_name + ")&is_current=true"
        for config in self.request_api(url)['items'][0]['configurations']:
            if(config['type'] == type):
                result[service_name][type] = config['properties']
        return result

    # hack as of now -- ansible vars doesn't support this
    def get_property(self, cluster, service_name, type, property):
        url = "/api/v1/clusters/" + cluster + "/configurations/service_config_versions?service_name.in(" + service_name + ")&is_current=true"
        for config in self.request_api(url)['items'][0]['configurations']:
            if(config['type'] == type):
                return config['properties'][property]
        return  ''

    # get all service config
    def get_all_services_config(self, cluster):
        result = {'service_configuration': {}}
        for service_name in self.get_all_services(cluster):
            result['service_configuration'][service_name] = self.get_current_service_config(cluster, service_name)
        return result

    # Collect cluster information
    def cluster_info(self, cluster):
        result = {'vars': {}}
        # Ambari related details
        result['vars']['ambari_protocol'] = self.proto
        result['vars']['ambari_port'] = self.port
        result['vars']['ambari_user'] = self.user
        result['vars']['ambari_password'] = self.password
        # get all services configuration
        result['vars'] = self.get_all_services_config(cluster)
        # set current hdp stack version
        result['vars']['hdp_version'] = self.get_hdp_version(cluster)

        # is secure cluster
        if(self.is_secure_cluster(cluster)):
            result['vars']['is_secure'] = 'true'
        else:
            result['vars']['is_secure'] = 'false'
        return result
