import requests
import zeep
from retry import retry


class ESphereClient:
    TRANSPORT_SERVICE = 'EdiExpressTransportService'
    SEND_PORT = 'SendEndpointPort'

    def __init__(self, wsdl_path: str, username: str, password: str):
        self.wsdl_path = wsdl_path
        self.username = username
        self.password = password

    def get_relation_id(self, partner_gln: str, msg_type: str, msg_direction: str):
        client = zeep.Client(self.wsdl_path)
        relationships = client.service.process(Name=self.username, Password=self.password)
        relation_id = next(r['relation-id'] for r in relationships.Cnt['relation-response'].relation
                           if (r['partner-iln'], r['document-type'], r['direction']) == (
                           partner_gln, msg_type, msg_direction))
        return relation_id

    @retry(exceptions=(requests.exceptions.HTTPError,
                       zeep.exceptions.TransportError),
           tries=3, delay=2, backoff=2)
    def send_desadv(self, partner_gln: str, msg: str):
        relation_id = self.get_relation_id(partner_gln, 'DESADV', 'OUT')
        client = zeep.Client(self.wsdl_path, service_name=self.TRANSPORT_SERVICE, port_name=self.SEND_PORT)
        client.service.process(Name=self.username, Password=self.password, RelationId=relation_id,
                               DocumentContent=msg.encode('utf-8'))
