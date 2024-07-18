import time
import zipfile
import csv
import base64
from collections import defaultdict
from suds.client import Client

try:
    # for Python 2.x
    from StringIO import StringIO
    from BytesIO import BytesIO
except ImportError:
    # for Python 3.x
    from io import StringIO
    from io import BytesIO

from msci.bdt.context.BDTClient import BDTClient, LogPlugin
from msci.bdt.context.exceptions import BDTError
from msci.bdt.context.transport import HttpAuthenticated


def parse_field(field_value):
    if field_value in ['N/A', None, '']:
        return None

    try:
        field_value = float(field_value)
    except (ValueError, TypeError):
        if field_value.endswith('%'):
            try:
                field_value = float(field_value[:-1]) / 100.0
            except ValueError:
                # We conclude it is not a numeric column, we leave it as is.
                pass

    return field_value

class _CommonClient(BDTClient):
    wsdl = '/axis2/services/BDTService?wsdl'

    def __init__(self, url, user_id, password, client_id, logger=None, timeout=50000, **kwargs):

        """
        Service BDT Client constructor with service overriding to handle credentials nicely

        :param url: web service url, example https://www.barraone.com, or a list of urls to try in order
        :type url:  str or list[str]
        :param user_id:
        :type user_id:  str
        :param password:
        :type password:  str
        :param client_id:
        :type client_id:  str
        :param logger: optional logger object
        :type logger:  logging
        :param timeout: optional timeout in seconds
        :type timeout:  int
        """

        BDTClient.__init__(self, url, user_id, password, client_id, logger, timeout, **kwargs)
        # Override service with helper
        self.service = _Service(self)

    def _get_client(self, **kwargs):
        """
        Return the python suds client

        """
        return Client(self.url, location=self.url, timeout=self.timeout, plugins=[LogPlugin(self.logger, False)],
                      transport=HttpAuthenticated(), **kwargs)

    def getJobStatus(self, job_id, job_type):
        """
        Return the job status in the server

        :param job_id:
        :type job_id:  str
        :param job_type: 'Import', 'Export', ...
        :type job_type:  str
        :return: 0 if successful, <0 if error (Error code)
        """
        # check_fun=getattr(self.service, "Get%sJobStatus" % job_type)
        check_fun = self._get_service_method("Get%sJobStatus" % job_type)
        status = check_fun(job_id)

        return status

    def waitJob(self, job_id, job_type):
        """
        Synchronously wait for job to be completed on the server

        :param job_id:
        :type job_id:  str
        :param job_type: 'Import', 'Export', ...
        :type job_type:  str
        :return: 0 if successful, <0 if error (Error code)
        """

        status = self.getJobStatus(job_id, job_type)
        while status > 0:
            self.logger.debug('Expected time to finish job: %i' % status)
            status = self.getJobStatus(job_id, job_type)
            if status > 0:
                time.sleep(status * 0.2)
            else:
                break

        if status:
            msg = 'Job id %s failed:%i' % (job_id, status)
            self.logger.error(msg)
            raise BDTError(msg, job_id, status)

    def removeUnderscores(self, input_list_of_tuples):
        output_dict = {}
        for old_key, value in input_list_of_tuples:
            if old_key[0] == '_':
                new_key = old_key[1:]
                output_dict[new_key] = value

        return output_dict

    def getImportJobLog(self, import_job_id):

        # wait till import is completed
        self.waitJob(import_job_id,'Import')

        job_log = self.service.GetImportJobLog(import_job_id)

        import_log = defaultdict(list)
        import_detail = defaultdict(list)

        for job_num, item in enumerate(job_log.LogGroups.ImportLogGroup, start=1):
            temp_dict = self.removeUnderscores(item)
            import_log['Job'].append(job_num)
            for this_key, this_value in temp_dict.items():

                if this_key == 'EffectiveDate':
                    import_log[this_key].append(this_value.strftime('%Y-%m-%d'))
                elif this_key != 'Details':
                    import_log[this_key].append(this_value)

            for this_detail in item.Details.ImportLogDetail:
                import_detail['Job'].append(job_num)
                temp_details = self.removeUnderscores(this_detail)
                for this_key, this_val in temp_details.items():
                    import_detail[this_key].append(this_val)

                # importLogDetail does not contain Detail2 for the first detail item
                # so we add 'Detail2': None to ensure that our dictionary is not jagged (same number of enties per key)
                # and can be converted to a Dataframe easily if the client wishes
                if 'Detail2' not in temp_details:
                    import_detail['Detail2'].append(None)


        return import_log, import_detail

    def readReport(self, byte_array, export_set_type, **kwargs):
        """
        Parse the ExportSet Binary data to extract the report data into python dictionary

        :param byte_array:
        :type byte_array:  binary
        :param export_set_type: Export set type from B1. Currently only 'PortfolioAnalysis', 'MPC', 'PortfolioExposure' are supported
        :type report_type:  str
        :return: reports

        ``**kwargs``: See below

        :param include_spec_risk: If True, include Specific Risk in PortfolioExposure Report. Default = False
        :type include_spec_risk: boolean
        """

        include_specific_risk = False

        if export_set_type == 'PortfolioExposure':
            delimiter = '|'
            include_specific_risk = kwargs.get('include_spec_risk', False)

        elif export_set_type in ['PortfolioAnalysis', 'MPC', 'HVR', 'STRESS']:
            delimiter = ','
        else:
            # Unrecognized report type. Warn user, but default to comma-delimited
            self.logger.warning('Unrecognized export set type ' + str(export_set_type))
            self.logger.warning('Defaulting to comma-delimited output')
            delimiter = ','

        reports = {'Reports': []}

        with zipfile.ZipFile(BytesIO(byte_array), "r") as myzip:
            zipfiles = myzip.infolist()
            for zipf in zipfiles:
                self.logger.info('Reading report from %s' % zipf.filename)
                with myzip.open(zipf.filename) as myfile:
                    header = {}
                    positions = {}
                    num_header_columns = 0
                    columns = None
                    summary = None

                    status_header_section = 0
                    lines = myfile.read()
                    lines = lines.decode("utf-8")
                    f = StringIO(lines)

                    # Here is a sample of the jobstatus file:
                    # note that this file is always CSV (with commas) even if the output file
                    # is pipe delimited - i.e. for the PortfolioExposure report type.

                    # See a sample below. Each line in the sample corresponds to a new line in the file:

                    # <start sample>
                    # Risk Model, Version, Covariance Matrix Date
                    # MAC.L, 400.00, 2020 / 06 / 26
                    # Name, Owner, Valid
                    # AllAssetCorrelations, System, true
                    # AllocationSelection - GICS, python, true
                    # Cashflow - Custom, python, true
                    # Risk - Decomp, python, true
                    #
                    # Portfolio, Analysis Date, Owner, Processed, Holdings Date, Bmk, Bmk Holdings Date, Market Value,
                    #   <cont on same line> Currency, Accept %, Directory, Full Tree Path, Stored Analytics Status,
                    # BA204, 2020/06/30, SYSTEM, Yes, 2020/06/30, CASH,, 1325289383537.18, USD, 97.97, BA204/, SYSTEM/BA204,
                    # SAP500D, 2020/06/30, SYSTEM, Yes, 2020/06/30, CASH,, 25636608236818.79, USD, 100.00, SAP500D/, SYSTEM/SAP500D,
                    # <end sample>

                    # There are three sections to  reading this file:

                    # 1:This first row of headers only has one corresponding row of values. This is always true
                    # regardless of how many portfolios or reports are in the export set

                    # 'Risk Model' = 'MAC.L', Version = 400.00, and Covariance Matrix Date = '2020/06/26',

                    # 2: The second section has one row per report definition specified in the job
                    # 'Name' = 'AllAssetCorrelations, 'Owner' = 'System, 'Valid' = true
                    # 'Name' = 'AllocationSelection - GICS, 'Owner' = 'python, 'Valid' = true
                    # 'Name' = 'Cashflow - Custom, 'Owner' = 'python, 'Valid' = true
                    # 'Name' = 'Risk - Decomp, 'Owner' = 'python, 'Valid' = true

                    # 3: The third section has one row per portfolio specified in the job:
                    # e.g. 'Portfolio' = 'BA204', 'Holdings Date' = '2020/06/30', and 'Full Tree Path' = 'SYSTEM/BA204' for the first row
                    # e.g. 'Portfolio' = 'SAP500D', 'Bmk' = 'CASH', and 'Market Value' = 25636608236818.79 for the second row

                    # the file is parsed like a standard report, with Header, Summary and Detail being returned.
                    # The first two sections are combined into the 'Header' dictionary, and the third section is returned
                    # in the 'Details' dictionary. 'Summary' is set to None

                    # I use 'status_header_section' to track which section I am on I read each line in the file

                    if 'jobstatus' in zipf.filename:
                        temp_delimiter = ','
                    else:
                        temp_delimiter = delimiter

                    for this_line in csv.reader(f, delimiter=temp_delimiter):

                        if not this_line:
                            continue  # skip over empty rows

                        if not num_header_columns:
                            num_header_columns = len(this_line)
                        header_row = this_line

                        if len(header_row) <= num_header_columns:
                            # still reading headers

                            if 'jobstatus' in zipf.filename:
                                # we are reading headers from the status file
                                # headers and values are row based
                                # so when we read the column headers
                                # the next line in the file will contain the corresponding values
                                # we look for either 'Risk Model' or 'Name' in the first column of the current line

                                # if first column = 'Risk Model', just the next row should be read as values
                                # if first column = 'Name', all following rows should be read as values

                                if status_header_section > 0:
                                    if status_header_section == 1:
                                        status_header_section = 0
                                        header['Risk Model'] = header_row[0]
                                        header['Version'] = header_row[1]
                                        header['Covariance Matrix Date'] = header_row[2]
                                    elif status_header_section == 2:

                                        if summary is None:
                                            summary = {'Name': [], 'Owner': [], 'Valid': []}

                                        summary['Name'].append(header_row[0])
                                        summary['Owner'].append(header_row[1])
                                        summary['Valid'].append(header_row[2])

                                else:
                                    if header_row[0] == 'Risk Model':
                                        status_header_section = 1

                                    if header_row[0] == 'Name':
                                        status_header_section = 2

                            elif num_header_columns == 2:
                                if len(header_row) > 1:
                                    header_values = header_row[1].strip()
                                else:
                                    header_values = header_row[-1]

                                header[header_row[0].replace(':', '')] = header_values

                        else:
                            # number of columns has changed
                            # the first time this happens we are looking at the column row
                            # subsequent rows will be the summary row (if it is present)
                            # and then the detail rows
                            if columns is None:

                                # get the list of columns from current line
                                columns = [x.strip() for x in this_line]

                                has_summary = False # assume no summary unless explicitly specified

                                # for the Correlations report, there may be duplicated columns if the portfolio
                                # contains positions with the same name (e.g. Class A and B shares)
                                # If we find  duplicates we rename the columns to address the problem
                                # this is inefficient code O(n^2) but fast relative to everything else :)
                                if len(columns) != len(set(columns)):
                                    # use max_duplicate_num dictionary to track any duplicate columns
                                    # will contain a duplicate count for any columns that are duplicate
                                    # contains an int that we append to the column name and then increment afterwards
                                    max_duplicate_num = defaultdict(int)
                                    new_cols = []
                                    for this_col in columns:
                                        if this_col not in new_cols:
                                            new_cols.append(this_col)
                                        else:
                                            # we increment before creating modified_col so that
                                            # first duplicate with end with '-1'
                                            duplicate_num = max_duplicate_num[this_col]+1
                                            modified_col = this_col + '-' + str(duplicate_num)
                                            new_cols.append(modified_col)
                                            max_duplicate_num[this_col] = duplicate_num
                                    columns = new_cols

                                # if report includes summary row, it is the first row after the headers
                                # we remove this row and return as a separate output,
                                # with the same columns as the details

                                # Positions Reports are required to have the Asset ID and Holdings columns in them
                                # So we can use this to test columns of a report to determine
                                # if it's a Positions Report
                                # Positions Report have a summary report which must be separated out

                                if export_set_type == 'PortfolioAnalysis':
                                    if {'Asset ID', 'Holdings'}.issubset(set(columns)):
                                        # Positions report - contains both 'Asset ID' and 'Holdings' columns
                                        has_summary = True
                                    elif {'Asset ID', 'Asset ID Type'}.issubset(set(columns)):
                                        # Cashflow report - contains Asset ID and ID Type, but not 'Holdings
                                        has_summary = True
                                    elif {'Risk Source'}.issubset(set(columns)):
                                        # this must be a Risk Decomposition or a Risk Delta Report
                                        has_summary = True
                                    elif 'jobstatus' in zipf.filename:
                                        has_summary = True
                                elif export_set_type == 'HVR':
                                    has_summary = True

                                # now go on to the next line in the file
                                # will be either summary or details depending on value of has_summary
                                for this_column in columns:
                                    positions[this_column] = []
                                continue

                            if has_summary and summary is None:

                                summary = {}
                                summary_row_fields = [x.strip() for x in this_line]

                                for col_name, summary_field in zip(columns, summary_row_fields):
                                    # if we can cast this to a float we will do so, otherwise leave as is
                                    # and strip '%' signs as well
                                    summary[col_name] = [parse_field(summary_field)]
                                # now go on to the next line in the file - detail rows
                                # ensure that we don't read the next line as a sumamry
                                has_summary = False
                                continue

                            detail_row_fields = [x.strip() for x in this_line]

                            for col_name, detail_field in zip(columns, detail_row_fields):
                                # if we can cast this to a float we will do so, otherwise leave as is
                                # and strip '%' signs as well
                                positions[col_name].append(detail_field)

                    # next we consider entire columns to determine whether to cast to float or not
                    # this has to be done at a column level since there may be castable values in columns
                    # that contain text information (e.g. a Barra ID or CUSIP that is entirely numeric)
                    # and those values should remain as text

                    for col_name, col_values in positions.items():
                        col_types = set(map(lambda x: type(parse_field(x)), col_values))
                        # if the column only contains floats or None or empty string (''), convert to float
                        # (parse_field will leave 'None' as is)
                        if col_types.issubset({float, type(None)}):
                            positions[col_name] = list(map(parse_field, col_values))
                        # else:
                        #     self.logger.info(col_name+ ' not coverted to float')

                    # reformat the 'wide' output for the exposure report to a 4 column narrow format
                    # does not need to be done for the specific covariance report though
                    # Specific Covariance does not have 'Asset ID' in keys, only 'Asset ID 1' and 'Asset ID 2'
                    if export_set_type == 'PortfolioExposure' and 'Asset ID' in positions.keys():
                        asset_id_lst = positions['Asset ID']
                        asset_id_type_lst = positions['Asset ID Type']
                        spec_risk_lst = positions['Specific Risk']

                        # note that our output dictionary will have multiple rows for each asset
                        asset_asset_lst = []
                        asset_asset_type_lst = []
                        asset_factor_lst = []
                        asset_exposure_lst = []

                        for this_factor in [i for i in positions.keys() if i not in
                                                                           ['Asset ID', 'Asset ID Type',
                                                                            'Specific Risk']]:

                            for (this_asset_id, this_assset_id_type, this_exposure) in zip(asset_id_lst,
                                                                                           asset_id_type_lst,
                                                                                           positions[this_factor]):
                                if this_exposure is not None:
                                    asset_asset_lst.append(this_asset_id)
                                    asset_asset_type_lst.append(this_assset_id_type)
                                    asset_factor_lst.append(this_factor)
                                    asset_exposure_lst.append(this_exposure)

                        # if requested, append the specific risk numbers as well as their own 'factor'
                        if include_specific_risk:
                            asset_asset_lst = asset_asset_lst + asset_id_lst
                            asset_asset_type_lst = asset_asset_type_lst + asset_id_type_lst
                            asset_factor_lst = asset_factor_lst + ['Specific Risk' for i in asset_id_lst]
                            asset_exposure_lst = asset_exposure_lst + [float(spec_risk) for spec_risk in
                                                                       spec_risk_lst]

                        # replace positions dictionary with condensed one for Portfolio Exposures
                        positions = {'Asset ID': asset_asset_lst,
                                     'Asset ID Type': asset_asset_type_lst,
                                     'Factor': asset_factor_lst,
                                     'Exposure': asset_exposure_lst}

                    if 'jobstatus' in zipf.filename:

                        # remove the column with empty column if it exists, in the jobstatus detail dictionary
                        positions.pop('', None)

                        reports['Status'] = ({'Header': header, 'Summary': summary,
                                              'Detail': positions, 'Filename': zipf.filename})
                    else:
                        reports['Reports'].append({'Header': header, 'Summary': summary,
                                                   'Detail': positions, 'Filename': zipf.filename})

        return reports

    def submitPortfolioImportJob(self, port_list, job_name='BDTImportUserPort'):
        """
        Import one or more portfolios. This function takes a list of dictionaries, each dictionary corresponding to a
        portfolio (or the same portfolio for a given analysis date).

        :param port_list: a list of dictionaries, one for each portfolio being imported. 'PortfolioName',
         'EffectiveStartDate' and 'Positions' are required - everything else is optional. The function will apply
         other keys from the dictionary to the portfolio upload, allowing for additional settings such as Benchmark,
         Store By Value, etc.
        :param job_name: optional name for the import job
        :type job_name: str

        :return: job id corresponding to submitted job
        :rtype: str
        """
        # Step 1 create Portfolios

        portfolios_for_import = []
        for this_port in port_list:
            # confirm that Portfolio, Owner and Date exist as keys in each dict
            # otherwise, skip this portfolio and raise a warning

            if 'PortfolioName' not in this_port:
                self.logger.warning('A portfolio dictionary is missing the PortfolioName key')
                self.logger.warning('This portfolio was not imported')
            elif 'EffectiveStartDate' not in this_port or 'Positions' not in this_port:
                self.logger.warning(f'Portfolio with name {this_port["PortfolioName"]} dictionary missing either EffectiveStartDate or Positions keys')
            else:

                port = self.factory.create('Portfolio')

                port._Owner = self.user_id
                port._PortfolioImportType = "BY_HOLDINGS"  # Set by default, may be overwritten

                for this_attr, this_value in this_port.items():
                    # Go through all inputs for this portfolio
                    # Assign any that are found on the Portfolio suds object (e.g. Benchmark)
                    # we will do 'Positions' separately to ensure we treat holdings / value / weights / ids correctly
                    if hasattr(port, '_' + this_attr) and this_attr != 'Positions':
                        setattr(port, '_' + this_attr, this_value)

                this_port_positions = this_port['Positions']

                # now iterate through items in Positions dictionary, which contains holdings / values / weights etc
                pos_list = self.factory.create('Positions')
                pos_list.Position = []
                # TODO: Support multiple ids for a given position (via Priority)
                for asset_num, (asset_id, asset_id_type) in enumerate(
                        zip(this_port_positions['id'], this_port_positions['idtype'])):
                    pos = self.factory.create('Position')
                    for col_name, col_values in this_port_positions.items():
                        if hasattr(pos, '_' + col_name):
                            setattr(pos, '_' + col_name, col_values[asset_num])

                    mid = self.factory.create('MID')
                    mid._ID = asset_id
                    mid._IDType = asset_id_type
                    pos.MID = mid
                    pos_list.Position.append(pos)

                port.Positions = pos_list
                portfolios_for_import.append(port)

        # Step 2 create SubmitImportJob request
        if not portfolios_for_import:
            # no valid portfolios found
            raise Exception('No valid portfolios found to import - check warnings for more details')

        job_id = self.service.SubmitImportJob(JobName=job_name,
                                              Portfolio=portfolios_for_import)

        return job_id

    def getExportJob(self, job_id):
        """
        Retrieve the Export Set binary data once status is complete

        :param job_id:
        :type job_id:  str
        :return: Binary Data
        """

        self.logger.info('Retrieving export set data for job id : %s' % job_id)
        # TODO should we do an automatic retry once only after waiting 30 seconds if this fails?
        attachment = self.service.GetExportJob(job_id)
        return base64.b64decode(attachment.BinaryData)

    def _get_report(self, report_type, job_id,  wait_for_completion, **kwargs):
        self.logger.info('Parsing %s report for job id : %s ' % (report_type, job_id))
        if wait_for_completion:
            self.waitJob(job_id, 'Export')
        byte_array = self.getExportJob(job_id)
        reports = self.readReport(byte_array, export_set_type=report_type, **kwargs)
        return reports

class _Service(object):
    """
    This is a helper object, used by the ServiceClient object, to simplify the syntax of the SOAP service calls.
    """

    def __init__(self, ES):
        self.ES = ES

    def __getattr__(self, item):
        def f(*args, **kwargs):
            return getattr(self.ES.client.service, item)(self.ES.user_id, self.ES.client_id, self.ES.password, *args,
                                                         **kwargs)

        return f


