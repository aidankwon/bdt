def import_mpc_case(service_bdt_client, port_list, case, market, currency):
    """

    :param service_bdt_client:
    :type service_bdt_client: msci.bdt.context.ServiceClient.ServiceClient
    :param port_list:
    :param case:
    :param market:
    :param currency:
    :return:
    """
    # Step 1 create new MPC
    b1case = service_bdt_client.factory.create('BarraOneCases')
    b1case._Owner = service_bdt_client.user_id

    b1mpc = service_bdt_client.factory.create('BarraOneMPCCase')
    b1mpc._MPCCaseName = case
    b1mpc._Market = market['Name']
    b1mpc._MarketOwner = market['Owner']
    b1mpc._Currency = currency

    ports = []
    for this_port in port_list:
        port = service_bdt_client.factory.create('MPCCaseValues')
        port._Portfolio = this_port['PortfolioName']
        port._PortfolioOwner = service_bdt_client.user_id
        port._BenchMark = 'CASH'
        ports.append(port)

    b1mpc.MPCCaseValues = ports
    b1case.BarraOneMPCCase = b1mpc

    # Step 2 submit the request to the service
    job_id = service_bdt_client.service.SubmitImportJob(JobName='BDTImportMPC', BarraOneCases=[b1case])

    # Step 3 check the import job status and wait for async job completion
    service_bdt_client.waitJob(job_id, 'Import')

