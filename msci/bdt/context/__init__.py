"""
The msci.bdt.context package contains a hierarchical implementation of the BDT Client.

Package contains an abstract root class called BDTClient and two child implementations for the service and the
interactive clients.

"""

URLS = {'PROD':'https://www.barraone.com',
        'UAT' :'https://uat.barraone.com',
        'US'  :'https://us.barraone.com',
        }