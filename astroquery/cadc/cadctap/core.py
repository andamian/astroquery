# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
TAP plus
=============

"""
from astroquery.utils.tap.core import TapPlus
from astroquery.cadc.cadctap.tapconn import TapConnCadc
from astroquery.cadc.cadctap.jobSaxParser import JobSaxParserCadc
import requests
import os.path
from astropy.extern.six.moves.urllib.parse import urlencode

try:
    # python 3
    import http.client as httplib
except ImportError:
    # python 2
    import httplib

__all__ = ['TapPlusCadc']

VERSION = "1.0.1"
TAP_CLIENT_ID = "aqtappy-" + VERSION


class TapPlusCadc(TapPlus):
    """TAP plus class
    Provides TAP and TAP+ capabilities
    """

    def __init__(self, url=None, host=None, server_context=None,
                 tap_context=None, port=80, sslport=443,
                 default_protocol_is_https=False, connhandler=None,
                 verbose=True):
        """Constructor
        Parameters
        ----------
        url : str, mandatory if no host is specified, default None
            TAP URL
        host : str, optional, default None
            host name
        server_context : str, optional, default None
            server context
        tap_context : str, optional, default None
            tap context
        port : int, optional, default '80'
            HTTP port
        sslport : int, optional, default '443'
            HTTPS port
        default_protocol_is_https : bool, optional, default False
            Specifies whether the default protocol to be used is HTTPS
        connhandler connection handler object, optional, default None
            HTTP(s) connection hander (creator). If no handler is provided, a
            new one is created.
        verbose : bool, optional, default 'True'
            flag to display information about the process
        """
        if url is not None:
            protocol, host, port, server_context, \
                tap_context = self._Tap__parseUrl(url)
            if protocol == "http":
                connHandler = TapConnCadc(False,
                                          host,
                                          server_context,
                                          tap_context,
                                          port,
                                          sslport)
            else:
                # https port -> sslPort
                connHandler = TapConnCadc(True,
                                          host,
                                          server_context,
                                          tap_context,
                                          port,
                                          port)
        else:
            connHandler = TapConnCadc(default_protocol_is_https,
                                      host,
                                      server_context,
                                      tap_context,
                                      port,
                                      sslport)
        if connhandler is not None:
            connHandler = connhandler
        self.__certificate = None
        super(TapPlusCadc, self).__init__(url, host, server_context,
                                          tap_context, port, sslport,
                                          default_protocol_is_https,
                                          connHandler, verbose)

    def load_table(self, table, verbose=False):
        """Loads the specified table
        Parameters
        ----------
        table : str, mandatory
            full qualified table name (i.e. schema name + table name)
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        A table object
        """
        print("Retrieving table '"+str(table)+"'")
        tables = self._Tap__load_tables(only_names=False,
                                        include_shared_tables=False,
                                        verbose=verbose)
        for tab in tables:
            if tab.get_qualified_name() == table:
                return tab
        return None

    def _Tap__launchJobMultipart(self, query, uploadResource, uploadTableName,
                                 outputFormat, context, verbose, name=None):
        uploadValue = str(uploadTableName) + ",param:" + str(uploadTableName)
        args = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "FORMAT": str(outputFormat),
            "tapclient": str(TAP_CLIENT_ID),
            "QUERY": str(query),
            "UPLOAD": ""+str(uploadValue)}
        f = open(uploadResource, "r")
        chunk = f.read()
        f.close()
        files = [[uploadTableName, uploadResource, chunk]]
        contentType, body = self._Tap__connHandler.encode_multipart(args,
                                                                    files)
        response = self._Tap__connHandler.execute_post(context,
                                                       body,
                                                       contentType)
        if verbose:
            print(response.status, response.reason)
            print(response.getheaders())
        if 'async' in context:
            # Check for error
            location = self._Tap__connHandler.find_header(
                response.getheaders(),
                "location")
            jobid = self._Tap__getJobId(location)
            runresponse = self.__runAsyncQuery(jobid, verbose)
            return runresponse
        return response

    def _Tap__launchJob(self, query, outputFormat,
                        context, verbose, name=None):
        args = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "FORMAT": str(outputFormat),
            "tapclient": str(TAP_CLIENT_ID),
            "QUERY": str(query)}
        data = self._Tap__connHandler.url_encode(args)
        response = self._Tap__connHandler.execute_post(context, data)
        if verbose:
            print(response.status, response.reason)
            print(response.getheaders())
        if 'async' in context:
            location = self._Tap__connHandler.find_header(
                response.getheaders(),
                "location")
            jobid = self._Tap__getJobId(location)
            runresponse = self.__runAsyncQuery(jobid, verbose)
            return runresponse
        return response

    def __runAsyncQuery(self, jobid, verbose):
        args = {
            "PHASE": "RUN"}
        data = self._Tap__connHandler.url_encode(args)
        response = self._Tap__connHandler.execute_post('async/'+jobid+'/phase',
                                                       data)
        if verbose:
            print(response.status, response.reason)
            print(response.getheaders())
        return response

    def load_async_job(self, jobid=None, verbose=False):
        """Loads an asynchronous job
        Parameters
        ----------
        jobid : str, mandatory if no name is provided, default None
            job identifier
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        A Job object
        """
        if jobid is None:
            if verbose:
                print("No job identifier found")
            return None
        subContext = "async/" + str(jobid)
        response = self._Tap__connHandler.execute_get(subContext)
        if verbose:
            print(response.status, response.reason)
            print(response.getheaders())
        isError = self._Tap__connHandler.check_launch_response_status(response,
                                                                      verbose,
                                                                      200)
        if isError:
            if verbose:
                print(response.reason)
            raise requests.exceptions.HTTPError(response.reason)
            return None
        # parse job
        jsp = JobSaxParserCadc(async_job=True)
        job = jsp.parseData(response)[0]
        job.connHandler = self._Tap__connHandler
        suitableOutputFile = self._Tap__getSuitableOutputFile(
            job.async_,
            None,
            response.getheaders(),
            isError,
            job.parameters['FORMAT'])
        job.outputFile = suitableOutputFile
        job.parameters['format'] = job.parameters['FORMAT']
        # load results
        job.get_results(verbose)
        return job

    def save_results(self, job, filename, verbose=False):
        """Saves job results
        Parameters
        ----------
        job : Job, mandatory
            job
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        job.save_results(filename, verbose)

    def login(self, user=None, password=None, certificate_file=None,
              verbose=False):
        """Performs a login.
        User and password can be used or a file that contains user name and
        password
        (2 lines: one for user name and the following one for the password)
        Parameters
        ----------
        user : str, mandatory if 'file' is not provided, default None
            login name
        password : str, mandatory if 'file' is not provided, default None
            user password
        certificate_file : str, mandatory if no 'user' & 'password' is provided
            location of the certificate
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        if certificate_file is None and user is None:
            print('Input user/password or certificate file path')
            self._TapPlus__isLoggedIn = True
            return
        if certificate_file is not None and user is not None:
            print('Choose one form of authentication only')
            return
        if certificate_file is not None:
            if self._TapPlus__getconnhandler().cookies_set():
                print('Already logged in with user/password')
                return
            if not os.path.isfile(certificate_file):
                print('File does not exist - '+certificate_file)
                return
            else:
                self._TapPlus__getconnhandler()._TapConn__connectionHandler. \
                    _ConnectionHandlerCadc__certificate = certificate_file
                self.__certificate = certificate_file
                return
        if user is not None and password is not None:
            if self.__certificate is not None:
                print('Already logged in with certificate')
                return
            self._TapPlus__user = user
            self._TapPlus__pwd = password
            self.__dologin(verbose)
        else:
            if password is None:
                print("Invalid password")
                return

    def __dologin(self, verbose=False):
        self._TapPlus__isLoggedIn = False
        response = self.__execLogin(self._TapPlus__user,
                                    self._TapPlus__pwd,
                                    verbose)
        # check response
        connHandler = self._TapPlus__getconnhandler()
        isError = connHandler.check_launch_response_status(response,
                                                           verbose,
                                                           200)
        if isError:
            print("Login error: " + str(response.reason))
            raise requests.exceptions.HTTPError(
                "Login error: " + str(response.reason))
        else:
            # extract cookie
            cookie = response.read()
            c = cookie.decode()
            cookie = 'CADC_SSO=' + c
            if cookie is not None:
                self._TapPlus__isLoggedIn = True
                connHandler.set_cookie(cookie)

    def logout(self, verbose=False):
        """Performs a logout
        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        self._TapPlus__getconnhandler()._TapConn__connectionHandler. \
            _ConnectionHandlerCadc__certificate = None
        if self._TapPlus__getconnhandler().cookies_set():
            self._TapPlus__getconnhandler().unset_cookie()
        self.__certificate = None
        self._TapPlus__isLoggedIn = False

    def __execLogin(self, usr, pwd, verbose=False):
        args = {
            "username": str(usr),
            "password": str(pwd)}
        data = urlencode(args)
        url = 'http://www.canfar.phys.uvic.ca/ac/login'
        protocol, host, port, server_context, \
            tap_context = self._Tap__parseUrl(url)
        connHandler = httplib.HTTPConnection(host, 80)
        context = '/ac/login'
        header = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "text/plain"
        }
        connHandler.request("POST", context, data, header)
        response = connHandler.getresponse()
        if verbose:
            print(response.status, response.reason)
            print(response.getheaders())
        return response