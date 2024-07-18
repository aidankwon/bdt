from suds.plugin import MessagePlugin
from suds.bindings import binding

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class BDTClient(object):
    # A list of the numeric and text columns available in BarraOne position reports (not comprehensive)
    POS_REPORT_COLS = {
        "Numeric": [
            'Holdings', 'Mkt Value','Price','Weight (%)', 'FX Conversion Rate',
            'Total Risk', 'Term Structure Risk', 'Term Structure Correlation', 'Term Structure Contribution', 
            'Effective Duration', 'Spread Risk', 'Spread Correlation', 'Spread Contribution',
            'Credit Spread Factor Exposure', 'Spread Duration', 'OAS (bp)', 'Specific Residual Risk', 
            'Specific Residual Contribution', 'Specific Residual Correlation', 'Currency Risk', 
            'Currency Risk Correlation', 'Currency Risk Contribution', 'KRD 1-month', 'KRD 6-month', 
            'KRD 1-year', 'KRD 2-year', 'KRD 5-year', 'KRD 10-year', 'KRD 20-year', 'KRD 30-year', 
            ],
        "Text": [
            'BARRAID', 'Asset Name', 'Inst. Type', 'Sector', 'Rating', 'Price Currency'
        ]
    }
    
    def __init__(self, url, user_id, password, client_id, logger=None, timeout=5000, **kwargs):

        """
        Abstract BDT Client constructor

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
        :param kwargs: kwargs for the underlying suds client
        """

        binding.envns = ('SOAP-ENV', 'http://schemas.xmlsoap.org/soap/envelope/')

        if logger is None:
            from logging import getLogger
            self.logger = getLogger(__name__)
        else:
            self.logger = logger

        self.user_id = user_id
        self.password = password
        self.client_id = client_id
        self.timeout = timeout

        if not isinstance(url, list):
            url = [url]

        connection_errors = []
        for url0 in url:
            self.url = url0 + self.wsdl
            self.logger.info('Connecting to %s ...' % url0)
            try:
                self.client = self._get_client(**kwargs)
            except Exception as e:
                msg = 'Unable to open connection to url: "%s"' % self.url
                detail = 'Error received: %s type:%s' % (e, type(e))
                self.logger.warning(msg + '\n' + detail)
                connection_errors.append(msg)
                connection_errors.append(detail)
            else:
                self.logger.info('... success')
                break
        else:
            msg = "\n".join(connection_errors)
            raise Exception(msg)

        self.factory = self.client.factory

    def terminate(self):
        """
        Children can override this method to add extra logic during client shutdown.

        """
        pass

    def _get_client(self, **kwargs):
        """
        Abstract _get_client method. To be implemented by children

        """
        raise NotImplemented

    def _get_service_method(self, name):
        """
        Return the handler to the service method

        :param name: The service method name
        :type name:  str
        """
        return getattr(self.service, name)

    def __enter__(self):
        """
        Return the resource object to the with call

        """
        return self

    def __exit__(self, *args, **kwargs):
        """
        Terminate the with block, free the resource

        """
        self.terminate()


class LogPlugin(MessagePlugin):
    """
    Message plugin implementation to read SOAP envelop removing MIME type wrappers

    """

    def __init__(self, logger, do_filter):
        """

        """
        self.logger = logger
        self.do_filter = do_filter

    def sending(self, context):
        self.logger.debug(str(context.envelope))

    def received(self, context):
        if self.do_filter:
            reply = context.reply
            tmp = reply.decode("utf-8")
            start = tmp.find('<?xml version')
            end = tmp.find('</S:Envelope>')
            tmp = tmp[start:end + 13]
            context.reply = tmp.encode("utf-8")
        self.logger.debug(str(context.reply))
