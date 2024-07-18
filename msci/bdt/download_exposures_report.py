
from msci.bdt import create_unique_export_set_name


def download_exposures_report(service_bdt_client, model, portfolio, date, file_name, portfolio_owner='SYSTEM',
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
    :param file_name: output file name
    :type file_name:  str
    :param portfolio_owner: portfolio owner (SYSTEM default)
    :type portfolio_owner:  str
    :param agg_type: Aggregation type can be NONE, LEAF_NODES_ONLY (Default), ALL_NODES, AGGREGATE_NODES_ONLY, SPECIFIC_LEVELS
    :type agg_type: str
    :return:
    """


    # Step 1 create portfolio exposure export set
    service_bdt_client.logger.debug('Creating PorfolioExposureExportSet ...')
    export_set_name = service_bdt_client.createPorfolioExposureExportSet(None, model, [(portfolio, portfolio_owner, agg_type)])
    service_bdt_client.logger.debug(f'Created {export_set_name}')
    job_name = export_set_name
    service_bdt_client.logger.debug(f'Done creating {export_set_name} ')

    # Step 2 submit the export job to the B1 server, hold the job id
    service_bdt_client.logger.debug(f'Sending Job {job_name} ...')
    job_id = service_bdt_client.sendExportJob(export_set_name, date, job_name)
    service_bdt_client.logger.debug(f'Job {job_name} sent, id is f{job_id}')

    # Step 3 synchronous waiting for job completion
    service_bdt_client.logger.debug(f'Waiting for completion of job {job_id}')
    service_bdt_client.waitJob(job_id, 'Export')
    service_bdt_client.logger.debug('Done')

    # Step 4 parse report into python data structure
    service_bdt_client.logger.debug(f'Downloading job {job_id}')
    service_bdt_client.downloadExportJob(job_id, file_name)
    service_bdt_client.logger.debug(f'Job {job_id} downloaded')

    # Remove export set
    service_bdt_client.deleteExportSet(export_set_name)

    return 0
