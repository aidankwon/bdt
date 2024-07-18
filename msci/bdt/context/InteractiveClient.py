from suds.client import Client
from msci.bdt.context.BDTClient import BDTClient, LogPlugin
from msci.bdt.context.transport import HttpAuthenticated
from collections import defaultdict


class InteractiveClient(BDTClient):
    wsdl = '/bdti/BDTInteractive?wsdl'

    def __init__(self, url, user_id, password, client_id, logger=None, timeout=50000, **kwargs):

        """
        Interactive BDT Client constructor and login

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
        :type logger:  int
        """

        BDTClient.__init__(self, url, user_id, password, client_id, logger, timeout, **kwargs)
        self.service = self.client.service
        self.logger.debug('Interactive client login in progress...')
        try:
            self.service.Login(self.user_id, self.client_id, self.password)
        except Exception as e:
            msg = 'Unable to login to url: "%s"' % self.url
            detail = 'Error received: %s type:%s' % (e, type(e))
            raise Exception(msg, detail)
        self.logger.debug('... logged in.')

    def _get_client(self, **kwargs):
        """
        Return the python suds client

        """
        return Client(self.url, location=self.url, cache=None, timeout=self.timeout,
                      plugins=[LogPlugin(self.logger, True)], transport=HttpAuthenticated(), **kwargs)

    def terminate(self):
        """
        Override terminate method from parent to perform interactive client logout

        """
        self.logger.debug('Logging out ... ')
        self.service.Logout()
        self.logger.debug('... Done')

    def get_available_stored_analytics(self):
        """
        Return a dataframe of stored analytics that available for loading
        
        """
        salist = self.service.GetPortfolioTreeStoredAnalytics()
        return list(map(dict, salist))
        
    def get_stored_analytics_position_report(self, storedAnalyticsFolder, cols, data_rows=True):
        """
        Given a StoredAnalyticsFolder record, and a set of columns, return a dataframe of the portfolio
        
        :param storedAnalyticsFolder: an object with fields matching results of get_available_stored_analytics
        :type storedAnalyticsFolder: dict or dataframe row or StoredAnalyticsFolder object
        :param cols: dict of two series 'Name' and 'Owner'
        :type cols:  dict
        :param data_rows: set to True to get position level data, false to get just top-level results (faster)
        :type data_rows: boolean
        """
        
        # cast storedAnalyticsFolder to right type
        storedAnalyticsFolder = dict(storedAnalyticsFolder)
        saobj = self.factory.create('StoredAnalyticsFolder')
        for k,v in storedAnalyticsFolder.items():
            saobj[k] = v

        # open the stored analytics folder to the right session
        res = self.service.SetCurrentPortfolioStoredAnalytics(saobj)
        assert(res.IsDone)

        return self.download_pos_report(cols, data_rows)
        
        
    def download_pos_report(self, cols, data_rows=True, customize_settings=None):
        """
        shared code for downloading position report with either stored analytics or live portfolios
        :param cols: dict of two series 'Name' and 'Owner'
        :type cols:  dict
        :param data_rows: set to True to get position level data, false to get just top-level results (faster)
        :type data_rows: boolean
        """

        # set the view to the selected columns
        columns = []
        for name, owner in zip (cols['Name'], cols['Owner']):
            col = self.factory.create('Column')
            col._Name = name
            col._Owner = owner
            columns.append(col)

        if customize_settings is not None:
            settings = customize_settings
        else:
            settings = self.factory.create('CustomizeReportSetting')
        settings._DataRows = data_rows
        grouping = []
        sorting = []

        # get the position report for designed columns
        reportres = self.service.GetPositionsReport(columns, grouping, settings, sorting)

        # parse the report into a python dataframe
        report = reportres.Report[0]
        col_def = report.ReportDefinition.ColDefinition[0].ColDefData

        positions = defaultdict(list)
        col_names = [col._Name for col in col_def]

        data = report.ReportBody.Row

        # enable the user to select just the top-level (total row) or position-level data
        if data_rows:
            for row in data:
                if row._IsTotalRow:
                    continue
                for i, name in enumerate(col_names):
                    cell = row.Cell[i]
                    positions[name].append(cell._Val)
        else:
            for row in data:
                if row._IsTotalRow:
                    for i, name in enumerate(col_names):
                        cell = row.Cell[i]
                        positions[name].append(cell._Val)
                    break
                else:
                    continue

        # return back the results
        return positions

    def possible_report_column_names(self, report_name: str, owner='SYSTEM'):
        '''Display which columns are available for a report 

        :param report_name: may be one of 'Positions', 'RiskDecomposition', 'FactorExposureBreakdown', 
        'StressTestPositions', 'AllocationSelection', 'StressTestSummary'.
        :type report_name: str
        '''

        possible = ['Positions', 'RiskDecomposition', 'FactorExposureBreakdown', 
                    'StressTestPositions', 'AllocationSelection', 'StressTestSummary']
        if report_name not in possible:
            raise Exception(f'report_name ({report_name}) must be one of {possible}')


        cols = self.service.GetReportColumnName(report_name, owner)
        return(cols)

    def set_adhoc_portfolio(self, user_holds):
        '''Set an user-created adhoc portfolio in the interactive session
        
        :param user_hold: a dictionary with keys of asset ids and values of holdings
        :type user_hold: dict
        '''
        mypos = self.factory.create('Portfolio')
        mypos._Type = 'HOLDINGS'

        def create_pos(k, v):
            p = self.factory.create('Position')
            mid = self.factory.create('MID')
            mid._ID = k
            p.MID = [mid]
            p._Value = float( v )
            return p

        mypos.Pos = [create_pos(k,v) for k,v in user_holds.items()]
        rej = self.service.SetAdHocPortfolio(mypos)
        return rej

        
