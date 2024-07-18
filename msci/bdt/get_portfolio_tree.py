def get_portfolio_tree(interactive_bdt_client, filter_tag):

    """

    Interactive call to get the list of portfolios matching the filter tag

    :param interactive_bdt_client:
    :type interactive_bdt_client:  msci.bdt.context.InteractiveClient.InteractiveClient
    :param filter_tag: portfolio owner or work-group
    :type filter_tag:  str
    :return: portfolio tree and portfolio list
    """

    folders = interactive_bdt_client.service.GetPortfolioTree(filter_tag)
    tree = {}
    full_port_list = []
    for folder in folders:
        path = folder._Path
        nodes = folder.PfNode
        ports = []
        for node in nodes:
            ports.append(node._Name)
            full_port_list.append(node._Name)
        tree[path] = ports

    return tree, full_port_list
