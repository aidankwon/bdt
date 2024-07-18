from warnings import warn
import base64
import time

from msci.bdt.context._CommonClient import _CommonClient


class BPMClient(_CommonClient):
    def submitHVRJob(self, job_name, port_name, port_owner, as_of_date):
        """
        Submit HVR job for existing job definition. Output format of HVR job must be Unformatted (CSV), otherwise
        job will fail. No checking is done on output format, user must ensure to only submit jobs for Unformatted HVR
        jobs.

        User should provide the name and owner of the HVR job definition, as well as the date for which the report is
        to be run.

        Function returns a

        :param job_name: name of existing HVR job definition. Out
        :type job_name:  str
        :param port_name: portfolio name to run HVR job for
        :type port_name:  str
        :param port_owner: portfolio owner
        :type port_owner:  str
        :param as_of_date: date to run report for in YYYY-MM-DD format

        :return: hvr report
        """

        hvr = self.factory.create('HvrDefinition')
        hvr._Name = job_name  # the Job Name as it appears in BPM
        #     hvr._Owner = self.user_id  # the user who created the job
        hvr._Owner = self.user_id

        hvr_port = self.factory.create('HvrPortfolio')

        hvr_port._Name = port_name
        hvr_port._Owner = port_owner
        hvr_ports = self.factory.create('HvrPortfolios')
        hvr_ports.Portfolio = hvr_port

        hvr_override = self.factory.create('OverrideHvrDefinitionParams')
        hvr_override.Portfolios = hvr_ports

        hvrParams = self.factory.create('HvrReportRequestParams')
        hvrParams.CycleDate = as_of_date
        hvrParams.HvrDefinition = hvr

        success = False
        retries = 0
        while not success and retries < 5:
            self.logger.info('Submitting HVR job ' + job_name + ' for ' + port_name + ' for ' + as_of_date)
            try:
                self.service.SubmitHVRJob(hvrParams, hvr_override)
            except Exception as e:
                err_message = e.args[0]
                retries += 1
                if err_message.find('in progress') > 0:
                    print('Job already running, waiting 30 seconds to submit')
                    time.sleep(30)
                else:
                    raise
            else:
                success = True

        if success:
            self.logger.info('Retrieving HVR job...')

            got_report = False
            try:
                attachment = self.service.GetHvrReports(hvrParams)
            except Exception as e:
                # first one's always free - we wait 30 seconds and try again, once only
                self.logger.info(e.args[0])
                time.sleep(30)
            else:
                got_report = True

            if not got_report:
                # if this fails we're done
                attachment = self.service.GetHvrReports(hvrParams)

            byte_array = base64.b64decode(attachment.BinaryData)
            results = self.readReport(byte_array, 'HVR')
            return results

        else:
            raise Exception('Error submitting job:'+err_message)

