from msci.bdt import create_unique_export_set_name


def get_exposures_report(service_bdt_client, model, portfolio, date, data_rows=True, portfolio_owner='SYSTEM',
                         agg_type='LEAF_NODES_ONLY'):
    """

    Synchronous execution of get exposure report flow

    :param service_bdt_client:
    :type service_bdt_client:  msci.bdt.context.ServiceClient.ServiceClient
    :param model:
    :type model:  str
    :param portfolio: portfolio name
    :type portfolio:  str
    :param date:
    :type date:  str 'yyyy-mm-dd'
    :param data_rows: True(default) to return asset exposures; portfolio exposures otherwise
    :type data_rows:  Boolean
    :param portfolio_owner: portfolio owner (SYSTEM default)
    :type portfolio_owner:  str
    :param agg_type: Aggregation type can be NONE, LEAF_NODES_ONLY (Default), ALL_NODES, AGGREGATE_NODES_ONLY, SPECIFIC_LEVELS
    :type agg_type: str
    :return: portfolio exposures report
    """

    export_set_name = create_unique_export_set_name(portfolio=portfolio, model=model)
    job_name = export_set_name

    # Step 1 create portfolio exposure export set
    if not data_rows:
        # Create a single asset portfolio with itself
        # TODO Import with no validation to speed up
        portfolios = []
        port_positions = {'id': ['MLH0A0'],
                          'idtype': [None],
                          'Holdings': [1],
                          }

        # the export set name is unique so we can use it for the portfolio name too
        port_name = export_set_name
        port_owner = service_bdt_client.user_id
        portfolios = [{'PortfolioName': port_name, 'EffectiveStartDate': date, 'Positions': port_positions}]

        import_job_id = service_bdt_client.submitPortfolioImportJob(portfolios)

        import_log, import_detail = service_bdt_client.getImportJobLog(import_job_id)

        import_result_code = import_log['ResultCode'][0]
        if import_result_code != 'OK':
            # error importing portfolio, so raise exception
            raise Exception(f'Error importing portfolio: {import_result_code}')
    else:
        port_name = portfolio
        port_owner = portfolio_owner

    # create the export set
    service_bdt_client.createPorfolioExposureExportSet(export_set_name, model,
                                                       [(port_name, port_owner, agg_type)])

    # Step 2 submit the export job to the B1 server, hold the job id
    job_id = service_bdt_client.sendExportJob(export_set_name, date, job_name)

    # Step 3 synchronous waiting for job completion
    service_bdt_client.waitJob(job_id, 'Export')

    # Step 4 parse report into python data structure
    output = service_bdt_client.getPortfolioExposureReports(job_id)

    # Remove export set
    service_bdt_client.service.DeleteExportSet(export_set_name)

    return output
