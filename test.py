from msci.bdt.context.ServiceClient import ServiceClient

url = 'https://www.barraone.com'
user_id = 'ykwon'
password = 'passive23456'
client_id = 'wu8cegzksh'

with ServieClient(url, user_id, password, client_id) as bdt_service_client:
    print(bdt_service_client.service.GetCurrentVersion())
