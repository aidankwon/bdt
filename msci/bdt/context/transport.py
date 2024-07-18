from suds.transport.https import HttpAuthenticated as HttpAuthenticatedSuds
from urllib.request import ProxyHandler


class HttpAuthenticated(HttpAuthenticatedSuds):
    """HttpAuthenticated which properly obeys the ``*_proxy`` environment variables.

       The root cause of the problem is that in suds the default value of self.proxy
       is {} instead of None. This solution improves on
       https://stackoverflow.com/questions/12414600/suds-ignoring-proxy-setting
       by keeping the proxy setting functionality, if needed.
    """

    def u2handlers(self):
        return [ProxyHandler(self.proxy if self.proxy else None)]
