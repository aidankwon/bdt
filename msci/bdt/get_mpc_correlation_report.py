import random

def get_mpc_correlation_report(service_bdt_client, model, case, report, report_owner, date):

    """

    Synchronous execution of get mpc correlation report flow

    :param service_bdt_client:
    :type service_bdt_client:  msci.bdt.context.ServiceClient.ServiceClient
    :param model:
    :type model:  str
    :param case: MPC case name
    :type case:  str
    :param report: MPC report name
    :type report:  str
    :param report_owner: MPC report owner
    :type report_owner:  str
    :param date:
    :type date:  str 'yyyy-mm-dd'
    :return: correlation report
    """


    # Step 1 create MPC export set
    export_set_name = service_bdt_client.createMPCExportSet(None, model, case, [(report, report_owner)])

    # Step 2 submit the export job to the B1 server, hold the job id
    job_name = 'MPCEJ_%s_%02i' % (date, random.randint(0,99))
    job_id = service_bdt_client.sendExportJob(export_set_name, date, job_name)

    # Step 3 synchronous waiting for job completion
    service_bdt_client.waitJob(job_id, 'Export')

    # Step 4 parse report into python data structure
    return service_bdt_client.getMPCReport(job_id)

    # Remove export set
    service_bdt_client.deleteExportSet(export_set_name)
