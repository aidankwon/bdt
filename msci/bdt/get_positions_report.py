from collections import defaultdict

def get_positions_report(interactive_bdt_client, model, portfolio, date, cols, data_rows=True,
                         portfolio_owner='SYSTEM', model_owner='SYSTEM', current_settings=None, customize_settings=None):
    """

    Interactive call to get portfolio positions

    :param interactive_bdt_client:
    :type interactive_bdt_client:  msci.bdt.context.InteractiveClient.InteractiveClient
    :param model:
    :type model:  str
    :param portfolio: portfolio name
    :type portfolio:  str
    :param date:
    :type date:  str 'yyyy-mm-dd'
    :param cols: List of column names
    :type cols:  List(str)
    :param portfolio_owner: portfolio owner (SYSTEM default)
    :type portfolio_owner:  str
    :param model_owner: model owner (SYSTEM default)
    :type model_owner:  str
    :param current_settings: (None default)
    :type current_settings:  dict
    :return: portfolio positions
    """

    interactive_bdt_client.service.SetAnalysisDate(date)
    smodel = interactive_bdt_client.factory.create('Model')
    smodel._Name = model
    smodel._Owner = model_owner
    interactive_bdt_client.service.SetModel(smodel)
    interactive_bdt_client.service.SetCurrentPortfolio(portfolio, portfolio_owner)
    if current_settings is not None:
        csettings = interactive_bdt_client.factory.create('CurrentSettings')
        for k, v in current_settings.items():
            csettings['_' + k] = v

        interactive_bdt_client.service.SetCurrentSettings(csettings)

    return interactive_bdt_client.download_pos_report(cols, data_rows, customize_settings=customize_settings)

