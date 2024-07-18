import pandas as pd
from msci.bdt import create_unique_export_set_name
from msci.bdt.context.ServiceClient import ServiceClient

url = 'https://www.barraone.com'
user_id = 'ykwon'
password = 'passive23456'
client_id = 'wu8cegzksh'

model = 'KRE3L'
port_name= '10003'
port_owner = 'ykwon'
date1 = '2024-07-12'

with ServiceClient(url, user_id, password, client_id) as bdt_service_client:
    name = create_unique_export_set_name(portfolio=port_name, model=model)
    portfolio_list = [(port_name, port_owner)]  # should be a list of (portfolio, owner) tuples
    report_list = [('Summary Report', 'SYSTEM')]
    bdt_service_client.createPorfolioExposureExportSet(name, model, portfolio_list, report_list)
    job_id = bdt_service_client.sendExportJob(name, date1, port_name + '-Exp')
    # output = bdt_service_client.getPortfolioAnalysisReports(job_id)

    # output = bdt_service_client.getPortfolioExposureReports(job_id, include_spec_risk=True)
    output = bdt_service_client.getPortfolioExposureReports(job_id)

    reports = output['Reports']
    status = output['Status']

    # reports_with_spec_risk = output_with_spec_risk['Reports']

    exposures = pd.DataFrame(reports[0]['Detail'])
    # exposures_with_spec_risk = pd.DataFrame(reports_with_spec_risk[0]['Detail'])

    print(exposures)
    # print(reports)
    # print(status)
    # print(exposures_with_spec_risk)
    
