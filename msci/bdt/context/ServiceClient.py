from warnings import warn
import zipfile
import csv
from io import StringIO, BytesIO

from msci.bdt import create_unique_export_set_name

from msci.bdt.context._CommonClient import _CommonClient

BOOL_STR = {True : "true", False : "false"}

class ServiceClient(_CommonClient):

    def portfolioSelection(self, portfolio_list, ccy='USD'):
        """

        Creates a PortfolioSelection complex type

        :param portfolio_list: Is a list of (Portfolio name, Portfolio owner, Aggregation Type) tuples, where the latter can be
         NONE, LEAF_NODES_ONLY, ALL_NODES, AGGREGATE_NODES_ONLY, SPECIFIC_LEVELS
        :type portfolio_list: list of 3-tuples
        :param ccy: Optional currency (USD default)
        :type ccy:  str
        :return: PortfolioSelection object
        """
        portfolio_selection = []
        for p in portfolio_list:
            port = self.factory.create('PortfolioSelection')
            port.PortfolioName = p[0]
            port.PortfolioOwner = p[1]
            port.IncludeAggregate = True
            if len(p) > 2:
                port.AggregationType = getattr(self.factory.create('NodeSelectionInfo'), p[2])
            port.Currency = ccy
            if len(p) > 3:
                extra_options = p[3]
                for k, v in extra_options.items():
                    setattr(port, k, v)

            portfolio_selection.append(port)
        return portfolio_selection

    def scenarioSelection(self, report_list):
        """
        Creates ScenarioSelection complex type, to be used in stress tests

        :param report_list: Is a list of (Scenario name, Scenario owner, model name) tuples
        :type report_list: list of 3-tuples
        :return: ReportSelection object
        """
        scenario_selection = []
        for name, owner, model in report_list:
            report = self.factory.create('ScenarioSelection')
            report.Name = name
            report.Owner = owner
            report.ModelName = model
            report.ModelOwner = 'SYSTEM'
            scenario_selection.append(report)
        return scenario_selection


    def reportSelection(self, report_list):
        """
        Creates ReportSelection complex type

        :param report_list: Is a list of (Report name, Report owner) tuples
        :type report_list: list of 2-tuples
        :return: ReportSelection object
        """
        report_selection = []
        for name, owner in report_list:
            report = self.factory.create('ReportSelection')
            report.Name = name
            report.Owner = owner
            report_selection.append(report)
        return report_selection

    def portfolioAE(self, model, portfolio_list, report_list, factory_type, ccy='USD', model_owner='SYSTEM'):
        """
        Create "PortfolioAnalysis", "PortfolioExposure" type, based on the value of factory_type

        :param model:
        :type model:  str
        :param portfolio_list: Is a list of (Portfolio name, Portfolio owner, Aggregation Type) tuples, where the latter can be
         NONE, LEAF_NODES_ONLY, ALL_NODES, AGGREGATE_NODES_ONLY, SPECIFIC_LEVELS
        :type portfolio_list: list of 3-tuples
        :param report_list: Is a list of (Report name, Report owner) tuples
        :type report_list: list of 2-tuples
        :param factory_type: "PortfolioAnalysis" or "PortfolioExposure"
        :type factory_type:  str
        :param ccy: Optional currency (USD default)
        :type ccy:  str
        :param model_owner: optional model owner
        :type model_owner:  str
        """
        pae = self.factory.create(factory_type)
        pae.PortfolioSelection = self.portfolioSelection(portfolio_list, ccy)
        pae.ReportSelection = self.reportSelection(report_list)
        pae._ModelName = model
        pae._ModelOwner = model_owner
        return pae

    def deleteExportSet(self, export_set_name):
        self.logger.info('Deleting Export Set:%s' % export_set_name)
        self.service.DeleteExportSet(export_set_name)

    def createExportSet(self, export_set_type, export_setup_args):
        """
        Creates ExportSet

        :param export_set_type: PORTFOLIO, PORTFOLIO_EXPOSURE or MPC
        :type export_set_type: ExportSetType complex type
        :param export_setup_args: export setup definition
        :type export_set_args: ExportSetupArgs complex type
        """

        result = self.service.CreateExportSet(True, export_set_type, export_setup_args)

    def createPorfolioAnalysisExportSet(self, name, model, portfolio_list, report_list, ccy='USD', format='.csv',
                                        model_owner='SYSTEM'):
        """
        Create PortfolioAnalysis ExportSet

        :param name: ExportSet name. If set to None, a name will automatically be created.
        :type name:  str
        :param model: Model name
        :type model:  str
        :param portfolio_list: Is a list of (Portfolio name, Portfolio owner, Aggregation Type) tuples, where the latter can be
         NONE, LEAF_NODES_ONLY, ALL_NODES, AGGREGATE_NODES_ONLY, SPECIFIC_LEVELS
        :type portfolio_list: list of 3-tuples
        :param report_list: Is a list of (Report name, Report owner) tuples
        :type report_list: list of 2-tuples
        :param ccy:  Optional currency
        :type ccy:  str
        :param format: Optional ExportSet file format
        :type format:  str
        :param model_owner: optional model owner
        :type model_owner:  str
        :return: The name of the export set
        :rtype: str
        """

        if not name:
            name = create_unique_export_set_name(portfolio=portfolio_list[0][0], model=model)

        self.logger.info('Creating PortfolioAnalysis Export Set: %s' % name)
        export_set_type = self.factory.create('ExportSetType').PORTFOLIO

        port_analysis = self.portfolioAE(model, portfolio_list, report_list, 'PortfolioAnalysis', ccy, model_owner)

        export_setup_args = self.factory.create('ExportSetupArgs')
        export_setup_args._ExportSetName = name
        export_setup_args._ExportSetOwner = self.user_id
        export_setup_args._OutputFormat = format
        export_setup_args.PortfolioAnalysis = port_analysis

        self.createExportSet(export_set_type, export_setup_args)
        return name

    def createPorfolioExposureExportSet(self, name, model, portfolio_list, report_list=[('Summary Report', 'SYSTEM')],
                                        ccy='USD', format='.csv', model_owner='SYSTEM', **kwargs):
        """
        Create PortfolioExposure ExportSet

        :param name: ExportSet name. If set to None, a name will automatically be created.
        :type name:  str
        :param model: Model name
        :type model:  str
        :param portfolio_list: Is a list of (Portfolio name, Portfolio owner, Aggregation Type) tuples, where the latter can be
         NONE, LEAF_NODES_ONLY, ALL_NODES, AGGREGATE_NODES_ONLY, SPECIFIC_LEVELS
        :type portfolio_list: list of 3-tuples
        :param report_list: Is a list of (Report name, Report owner) tuples
        :type report_list: list of 2-tuples
        :param ccy:  Optional currency
        :type ccy:  str
        :param format: Optional ExportSet file format
        :type format:  str
        :param model_owner: optional model owner
        :type model_owner:  str
        :return: The name of the export set
        :rtype: str
        """

        if not name:
            name = create_unique_export_set_name(portfolio=portfolio_list[0][0], model=model)

        self.logger.info('Creating PortfolioExposure Export Set:%s' % name)
        export_set_type = self.factory.create('ExportSetType').PORTFOLIO_EXPOSURE

        port_exp = self.portfolioAE(model, portfolio_list, report_list, 'PortfolioExposure', ccy, model_owner)

        for this_attr, this_value in kwargs.items():
            if hasattr(port_exp, '_' + this_attr):
                setattr(port_exp, '_' + this_attr, this_value)

        export_setup_args = self.factory.create('ExportSetupArgs')
        export_setup_args._ExportSetName = name
        export_setup_args._ExportSetOwner = self.user_id
        export_setup_args._OutputFormat = format
        export_setup_args.PortfolioExposure = port_exp

        self.createExportSet(export_set_type, export_setup_args)
        return name

    def createMPCExportSet(self, name, model, case_name, report_list, format='.csv', model_owner='SYSTEM'):
        """
        Create MPC Export Set

        :param name: ExportSet name. If set to None, a name will automatically be created.
        :type name:  str
        :param model: Model name
        :type model:  str
        :param case_name: MPC case name
        :type case_name:  str
        :param report_list: Is a list of (Report name, Report owner) tuples
        :type report_list: list of 2-tuples
        :param format: Optional ExportSet file format (.csv default)
        :type format:  str
        :param model_owner: optional model owner
        :type model_owner:  str
        :return: The name of the export set
        :rtype: str
        """

        if not name:
            name = create_unique_export_set_name(portfolio=report_list[0][0], model=model)

        self.logger.info('Creating MPC Export Set:%s' % name)
        export_set_type = self.factory.create('ExportSetType').MPC

        mpc_case = self.factory.create('MPCCaseSelection')
        mpc_case.Owner = self.user_id
        mpc_case.Name = case_name

        mpc = self.factory.create('MPC')
        mpc.ReportSelection = self.reportSelection(report_list)
        mpc.MPCCaseSelection = mpc_case
        mpc._ModelName = model
        mpc._ModelOwner = model_owner

        export_setup_args = self.factory.create('ExportSetupArgs')
        export_setup_args._ExportSetName = name
        export_setup_args._ExportSetOwner = self.user_id
        export_setup_args._OutputFormat = format
        export_setup_args.MPC = mpc

        self.createExportSet(export_set_type, export_setup_args)
        return name

    def sendExportJob(self, exp_set_name, analysis_date, job_name):
        """
        Submit the export job

        :param exp_set_name:
        :type exp_set_name:  str
        :param analysis_date:
        :type analysis_date: str 'yyyy-mm-dd'
        :param job_name:
        :type job_name:  str
        :return: JobId
        :rtype: str
        """

        self.logger.info('Sending Export job: %s for Export set: %s on %s' % (job_name, exp_set_name, analysis_date))
        return self.service.SubmitExportJob(exp_set_name, analysis_date, job_name, False)


    def sendAndWaitExportJob(self, exp_set_name, analysis_date, job_name, fn=None):
        """
        A synchronous wrapper around sendExportJob, which waits for the job to finish and returns or saves the result
        depending on whether a file name was given.

        :param exp_set_name: Name of the export set
        :param analysis_date:
        :type analysis_date: str 'yyyy-mm-dd'
        :param job_name:
        :param fn: If given, then the results will be saved into this file (as zip archive)
        :return: Byte array (if fn is None) else None

        """

        job_id = self.sendExportJob(exp_set_name, analysis_date, job_name)
        self.waitJob(job_id, 'Export')
        return self.downloadExportJob(job_id, fn)

    def createShocksStressTestExportSet(self, name, portfolio_list, scenarios, risk_model, style_format=".csv",
                                  ccy="USD", model_owner="SYSTEM", stress_testing_views_list=None):
        """
                Create PortfolioAnalysis StressTest

                :param name: ExportSet name
                :type name:  str
                :param risk_model: Model name
                :type risk_model:  str
                :param portfolio_list: Is a list of (Portfolio name, Portfolio owner, Aggregation Type) tuples, where the latter can be
                 NONE, LEAF_NODES_ONLY, ALL_NODES, AGGREGATE_NODES_ONLY, SPECIFIC_LEVELS
                :type portfolio_list: list of 3-tuples
                :param stress_testing_views_list: Is a list of (Report name, Report owner) tuples
                :type stress_testing_views_list: list of 2-tuples1
                :param scenarios Is a list of (Scenario name, Scenario owner, model name) tuples
                :type scenarios: list of 3-tuples
                :param ccy:  Optional currency (USD default)
                :type ccy:  str
                :param style_format: Optional ExportSet file format (.csv default)
                :type style_format:  str
                :param model_owner: optional model owner
                :type model_owner:  str
                :return:
                """
        port_selection = self.portfolioSelection(portfolio_list, ccy)

        # port_selection.AggregationType = self.factory.create(
        #    "NodeSelectionInfo").SPECIFIC_LEVELS  # "SPECIFIC_LEVELS"
        # port_selection.SpecificLevel = "1"

        if stress_testing_views_list is None:
            # The default views for SYSTEM
            stress_testing_views_list = [('Summary Report', "SYSTEM"),
                                         ('Portfolio Report', "SYSTEM"),
                                         ('Factor P&L Decomposition', "SYSTEM")
                                         ]

        report_selections = self.reportSelection(stress_testing_views_list)

        scenario_selections = self.scenarioSelection(scenarios)

        # Create in a separate function
        sim = self.stressSimulationSelection(port_selection, report_selections, scenario_selections,
                                             ccy=ccy, risk_model=risk_model, model_owner=model_owner)

        export_setup_args = self.factory.create('ExportSetupArgs')
        export_setup_args._ExportSetName = name
        export_setup_args._OutputFormat = style_format
        export_setup_args._ExportSetOwner = self.user_id  # This is assuming login id running the report is the owner
        export_setup_args.StressSimulation = sim

        export_set_type = self.factory.create('ExportSetType').STRESS
        result = self.service.CreateExportSet(True, export_set_type, export_setup_args)
        return result

    def stressSimulationSelection(self, port_selection, report_selections, scenario_selections,
                                  risk_model, ccy="USD", model_owner="SYSTEM"):
        sim = self.factory.create('StressSimulation')
        sim._Currency = ccy
        sim.PortfolioSelection = port_selection
        sim.ReportSelection = report_selections
        sim.ScenarioSelection = scenario_selections

        if "BIM" in risk_model:
            sim._StressTestType = self.factory.create('StressTestType').SIMULATION
        elif "MAC" in risk_model:
            sim._StressTestType = self.factory.create('StressTestType').STRESS_TEST
        else:
            print("Model not recognized...")

        sim._ModelName = risk_model
        sim._ModelOwner = model_owner
        return sim

    def submitShocksStressTestScenario(self, scenarios, risk_model, scenario_base_currency="USD",
                                          stress_type="MARKET", credit_market="Country",
                                          correlated_mode="false", floor_nominal_rates_to_zero="",
                                          interpolate_shocks="false",
                                          enable_full_valuation=""):
        '''
        Function to import a Stress Test scenario for the Stresstesting module

        :param risk_model:
        :type risk_model:  str
        :param scenarios: Is a dictionary with Scenario Name -> Dictionary of shock attributes
         For Market = _Type, _Market, _FilterAttribute, _FilterValue, _Shock _Variable, _Shock _Unit, _ShockAmount
         For Factor = _AssetClass, _FactorGroup, _FactorSubgroup, _Factor, _Unit, _Shock
        :type scenarios: dict multiple values
        :param scenario_base_currency: Base currency of scenarios
        :type scenario_base_currency: str
        :param stress_type: type of Stress (FACTOR or MARKET)
        :type stress_type: str
        :param credit_market: credit market type
        :type stress_type: str
        :param correlated_mode: correlated mode ('true' or 'false')
        :type stress_type: str
        :param floor_nominal_rates_to_zero: 'true' or 'false'
        :type stress_type: str
        :param interpolate_shocks: interpolate shocks ('true' or 'false')
        :type stress_type: str
        :param enable_full_valuation: enable full valuation ('true' or 'false')
        :type stress_type: str
        '''
        import_export_stress_test_scenario_list = []

        for scenario in scenarios.keys():
            list_of_shocks = scenarios[scenario]
            scenario_values = []
            scenario_params = {}
            scenario_type = None
            scenario_key = None
            for shock_params in list_of_shocks:
                if stress_type == "MARKET":
                    scenario_values_type = self.factory.create('ScenarioMarketValuesType')
                else:
                    scenario_values_type = self.factory.create('ScenarioFactorValuesType')
                for param in dict(scenario_values_type).keys():
                    if param in shock_params:
                        scenario_values_type[param] = shock_params[param]
                scenario_values.append(scenario_values_type)
                if stress_type == "MARKET":
                    scenario_params = {"ScenarioMarketValues": scenario_values,
                                       "_RiskModel": risk_model,
                                       "_ScenarioType": "MARKET",
                                       "_ScenarioName": scenario,
                                       "_ScenarioBaseCurrency": scenario_base_currency,
                                       "_CorrelatedMode": "{0}".format(correlated_mode),
                                       "_FloorNominalRatesToZero": "{0}".format(floor_nominal_rates_to_zero),
                                       "_CreditMarket": credit_market,
                                       "_InterpolateRateShocks": "{0}".format(interpolate_shocks),
                                       "_EnableFullValuation": "{0}".format(enable_full_valuation),
                                       "_BetaFilter": ""
                                       }
                    scenario_type = self.factory.create('ScenarioMarketType')
                    scenario_key = 'ScenarioMarket'
                elif stress_type == 'FACTOR':
                    scenario_params = {"ScenarioFactorValues": scenario_values,
                                       "_RiskModel": risk_model,
                                       "_ScenarioType": "FACTOR",
                                       "_ScenarioName": scenario,
                                       "_ScenarioBaseCurrency": scenario_base_currency,
                                       "_CorrelatedMode": "{0}".format(correlated_mode),
                                       "_FloorNominalRatesToZero": "{0}".format(floor_nominal_rates_to_zero),
                                       "_CreditMarket": credit_market,
                                       "_InterpolateRateShocks": "",
                                       "_EnableFullValuation": "{0}".format(enable_full_valuation),
                                       }
                    scenario_type = self.factory.create('ScenarioFactorType')
                    scenario_key = 'ScenarioFactor'
                else:
                    raise ValueError("Invalid argument stress type: {0}".format(stress_type))
            for param in dict(scenario_type).keys():
                if param in scenario_params:
                    scenario_type[param] = scenario_params[param]

            import_export_strss_test_scenario = self.factory.create('ImportExportStressTestScenario')
            for ky in dict(import_export_strss_test_scenario).keys():
                if ky == scenario_key:
                    import_export_strss_test_scenario[ky] = scenario_type
                else:
                    import_export_strss_test_scenario[ky] = {}
            import_export_stress_test_scenario_list.append(import_export_strss_test_scenario)

        job_id = self.service.SubmitImportJob(JobName='BDT_ImportExportStressTestScenario',
                                              ImportExportStressTestScenario=import_export_stress_test_scenario_list)
        self.waitJob(job_id, 'Import')
        log_response = self.service.GetImportJobLog(job_id)
        return log_response

    def downloadExportJob(self, job_id, file_name=None):
        """
        Download the Export Set Binary data into a file

        :param job_id:
        :type job_id:  str
        :param file_name: Output path to save export file. If not given, file is not saved and byte_array is returned
        :type file_name:  str
        :return:
        """

        self.logger.info('Downloading results into: %s ' % file_name)
        byte_array = self.getExportJob(job_id)
        if file_name is None:
            return byte_array

        try:
            with open(file_name, 'wb') as f:
                f.write(byte_array)
        except Exception as e:
            msg = 'Unable to save report to : "%s"' % file_name
            detail = 'Error received: %s type:%s' % (e, type(e))
            raise Exception(msg, detail)

        return 0

    def getShocksStressReport(self, job_id, wait_for_completion=True):
        return self._get_report('STRESS',  job_id, wait_for_completion)

    def getMPCReport(self, job_id, *, wait_for_completion=True):
        return  self._get_report('MPC', job_id, wait_for_completion)

    def getPortfolioAnalysisReport(self, job_id, *, wait_for_completion=True):
        warn("The 'getPortfolioAnalysisReport' method has been renamed to 'getPortfolioAnalysisReportS'."+\
             "The singular version will be removed in a future release.", DeprecationWarning, 2)
        return self.getPortfolioAnalysisReports(job_id, wait_for_completion=wait_for_completion)

    def getPortfolioAnalysisReports(self, job_id, *, wait_for_completion=True):
        return self._get_report('PortfolioAnalysis', job_id, wait_for_completion)

    def getPortfolioExposureReports(self, job_id, *, wait_for_completion=True, include_spec_risk=False):
        return self._get_report('PortfolioExposure', job_id, wait_for_completion, include_spec_risk=include_spec_risk)

    def getPortfolioExposureReport(self, job_id, *, wait_for_completion=True, include_spec_risk=False):
        warn("The 'getPortfolioExposureReport' method has been renamed to 'getPortfolioExposureReports'" + \
             "The singular version will be removed in a future release.", DeprecationWarning, 2)
        return self.getPortfolioExposureReports(job_id, wait_for_completion=wait_for_completion, include_spec_risk=include_spec_risk)

    def getExposureReport(self, job_id, *, wait_for_completion=True, include_spec_risk=False):
        warn("The 'getExposureReport' method has been renamed to 'getPortfolioExposureReports'" + \
             "The legacy version will be removed in a future release.", DeprecationWarning, 2)
        return self.getPortfolioExposureReports(job_id, wait_for_completion=wait_for_completion, include_spec_risk=include_spec_risk)

# This may be a bit of an overkill, but this is how we could "automate" adding the get*Report methods
# Although this works, but is not complete yet, as, ideally, the signatures should also be made to match.
# So actually as long as we only have these 3 functions I would go with the manual implementation as above.

# for report_type in ['MPC', 'PortfolioAnalysis', 'HVR']:
#     setattr(ServiceClient, 'get%sReport' % report_type,
#             (lambda t1: lambda self, job_id, wait_for_completion=True : ServiceClient._get_report(self, t1, job_id, wait_for_completion))(report_type))

