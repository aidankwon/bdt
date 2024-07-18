from msci.bdt.context.ServiceClient import ServiceClient

from msci.bdt import create_unique_export_set_name


def get_stress_test_report(service_bdt_client: ServiceClient, model: str,
                           portfolio_list, scenarios, date, model_owner='SYSTEM',
                           ccy="USD", stress_testing_views_list=[("MAC ST Portfolio Rpt", 'SYSTEM')]):
    """

    Synchronous execution of get stress test report flow

    :param service_bdt_client:
    :type service_bdt_client:  msci.bdt.context.ServiceClient.ServiceClient
    :param model:
    :type model:  str
    :param portfolio_list: Is a list of (Portfolio name, Portfolio owner, Aggregation Type) tuples, where the latter can be
                 NONE, LEAF_NODES_ONLY, ALL_NODES, AGGREGATE_NODES_ONLY, SPECIFIC_LEVELS
    :type portfolio_list: list of 3-tuples
    :param stress_testing_views_list: Is a list of (Report name, Report owner) tuples
    :type stress_testing_views_list: list of 2-tuples1
    :param scenarios Is a list of (Scenario name, Scenario owner, model name) tuples
    :type scenarios: list of 3-tuples
    :param ccy:  Optional currency (USD default)
    :type ccy:  str
    :param date:
    :type date:  str 'yyyy-mm-dd'
    :param model_owner: portfolio owner (SYSTEM default)
    :type model_owner:  str
    :return: stress test report
    """

    if stress_testing_views_list is None:
        stress_testing_views_list = [('Summary Report', "SYSTEM")]

    portfolio_str = ""
    for port_tuple in portfolio_list:
        portfolio_str = portfolio_str + port_tuple[0] + "_"
    export_set = create_unique_export_set_name(portfolio=portfolio_str, model=model)
    job_name = export_set

    # Step 1 create portfolio exposure export set

    # Obtain stress test report srt
    service_bdt_client.createShocksStressTestExportSet(name=export_set, portfolio_list=portfolio_list,
                                                       scenarios=scenarios, risk_model=model, ccy=ccy,
                                                       model_owner=model_owner,
                                                       stress_testing_views_list=stress_testing_views_list)

    # Step 2 submit the export job to the B1 server, hold the job id
    job_id = service_bdt_client.sendExportJob(export_set, date, job_name)

    # Step 3 synchronous waiting for job completion
    service_bdt_client.waitJob(job_id, 'Export')

    # Step 4 parse report into python data structure
    report = service_bdt_client.getShocksStressReport(job_id, True)

    # Remove export set
    service_bdt_client.service.DeleteExportSet(export_set)

    return report
