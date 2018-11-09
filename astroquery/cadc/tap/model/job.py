# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
TAP plus
=============

"""
import time
import requests
import astroquery.cadc.tap.xmlparser

from astroquery.cadc.tap.xmlparser import utils

__all__ = ['Job']


class Job(object):
    """Job class
    """
    def __init__(self, async_job, query=None, connhandler=None):
        """Constructor

        Parameters
        ----------
        async_job : bool, mandatory
            'True' if the job is asynchronous
        query : str, optional, default None
            Query
        connhandler : TapConn, optional, default None
            Connection handler
        """
        self.__internal_init()
        self.__connHandler = connhandler
        self.__async = async_job
        self.__parameters['query'] = query

    def __internal_init(self):
        self.__connHandler = None
        self.__isFinished = None
        self.__jobid = None
        self.__remoteLocation = None
        self.__phase = None
        self.__async = None
        self.__outputFile = None
        self.__responseStatus = 0
        self.__responseMsg = None
        self.__results = None
        self.__resultInMemory = False
        self.__failed = False
        self.__runid = None
        self.__ownerid = None
        self.__startTime = None
        self.__endTime = None
        self.__creationTime = None
        self.__executionDuration = None
        self.__destruction = None
        self.__locationId = None
        self.__name = None
        self.__quote = None
        self.__parameters = {}
        self.__errMessage = None
        # default output format
        self.set_output_format('votable')

    def set_connhandler(self, connhandler):
        self.__connHandler = connhandler

    def set_jobid(self, jobid):
        """Sets job identifier

        Parameters
        ----------
        jobid : str, mandatory
            job identifier
        """
        self.__jobid = jobid

    def get_jobid(self):
        """Returns the job identifier

        Returns
        -------
        The job identifier
        """
        return self.__jobid

    def set_failed(self, failed=False):
        """Sets the job status to failed

        Parameters
        ----------
        failed : bool, optional, default 'False'
            failed status
        """
        self.__failed = failed

    def is_failed(self):
        """Returns the job status

        Returns
        -------
        'True' if the job is failed
        """
        return self.__failed

    def set_remote_location(self, location):
        """Sets the job remote location

        Parameters
        ----------
        location : str, mandatory
            job remote location
        """
        self.__remoteLocation = location

    def get_remote_location(self):
        """Returns the job remote location

        Returns
        -------
        The job remote location
        """
        return self.__remoteLocation

    def set_phase(self, phase):
        """Sets the job phase

        Parameters
        ----------
        phase : str, mandatory
            job phase
        """
        self.__phase = phase

    def get_phase(self, authentication=None, update=False):
        """Returns the job phase. May optionally update the job's phase.

        Parameters
        ----------
        authentication : AuthMethod object, mandatory, default 'None'
            authentication object to use
        update : bool
            if True, the phase will by updated by querying the server before
            returning.

        Returns
        -------
        The job phase
        """
        if update:
            if authentication.get_auth_method() == 'netrc':
                phase_request = "auth-async/"+str(self.get_jobid())+"/phase"
            else:
                phase_request = "async/" + str(self.get_jobid()) + "/phase"
            if authentication.get_auth_method() == 'certificate':
                response = self.__connHandler.execute_get_secure(
                    phase_request,
                    authentication=authentication)
            else:
                response = self.__connHandler.execute_get(
                    phase_request,
                    authentication=authentication)

            self.__last_phase_response_status = response.status
            if response.status != 200:
                raise Exception(response.reason)
            self.set_phase(str(response.read().decode('utf-8')))
        return self.__phase

    def set_output_file(self, output_file):
        """Sets the job output file

        Parameters
        ----------
        output_file : str, mandatory
            job output file
        """
        self.__outputFile = output_file

    def get_output_file(self):
        """Returns the job output file

        Returns
        -------
        The results output file
        """
        return self.__outputFile

    def set_response_status(self, status, msg):
        """Sets the HTTP(s) connection status

        Parameters
        ----------
        status : int, mandatory
            HTTP(s) response status
        msg : str, mandatory
            HTTP(s) response message
        """
        self.__responseStatus = status
        self.__responseMsg = msg

    def get_response_status(self):
        """Returns the HTTP(s) connection status

        Returns
        -------
        The HTTP(s) connection response status
        """
        return self.__responseStatus

    def get_response_msg(self):
        """Returns the HTTP(s) connection message

        Returns
        -------
        The HTTP(s) connection response message
        """
        return self.__responseMsg

    def set_output_format(self, output_format):
        """Sets the job output format

        Parameters
        ----------
        output_format : str, mandatory
            job results output format
        """
        self.__parameters['format'] = output_format

    def get_output_format(self):
        """Returns the job output format

        Returns
        -------
        The job results output format
        """
        return self.__parameters['format']

    def is_sync(self):
        """Returns True if this job was executed synchronously

        Returns
        -------
        'True' if the job is synchronous
        """
        return not self.__async

    def is_async(self):
        """Returns True if this job was executed asynchronously

        Returns
        -------
        'True' if the job is synchronous
        """
        return self.__async

    def get_query(self):
        """Returns the job query

        Returns
        -------
        The job query
        """
        return self.__parameters['query']

    def get_runid(self):
        """Returns the job run identifier

        Returns
        -------
        The job run identifier
        """
        return self.__runid

    def set_runid(self, runid):
        """Sets the job run identifier

        Parameters
        ----------
        runid : str, mandatory
            job run identifier
        """
        self.__runid = runid

    def get_ownerid(self):
        """Returns the job owner identifier

        Returns
        -------
        The job owner identifier
        """
        return self.__ownerid

    def set_ownerid(self, ownerid):
        """Sets the job owner identifier

        Parameters
        ----------
        ownerid : str, mandatory
            job owner identifier
        """
        self.__ownerid = ownerid

    def set_start_time(self, starttime):
        """Sets the job start time

        Parameters
        ----------
        starttime : str, mandatory
            job start time
        """
        self.__startTime = starttime

    def get_start_time(self):
        """Returns the job start time

        Returns
        -------
        The job start time
        """
        return self.__startTime

    def set_end_time(self, endtime):
        """Sets the job end time

        Parameters
        ----------
        endtime : str, mandatory
            job end time
        """
        self.__endTime = endtime

    def get_end_time(self):
        """Returns the job end time

        Returns
        -------
        The job end time
        """
        return self.__endTime

    def set_creation_time(self, creationtime):
        """Sets the job creation time

        Parameters
        ----------
        creationtime : str, mandatory
            job creation time
        """
        self.__creationTime = creationtime

    def get_creation_time(self):
        """Returns the job creation time

        Returns
        -------
        The job creation time
        """
        return self.__creationTime

    def set_execution_duration(self, executionduration):
        """Sets the job execution duration

        Parameters
        ----------
        executionduration : int, mandatory
            job execution duration
        """
        self.__executionDuration = executionduration

    def get_execution_duration(self):
        """Returns the job execution duration

        Returns
        -------
        The job execution duration
        """
        return self.__executionDuration

    def set_destruction(self, destruction):
        """Sets the job destruction value

        Parameters
        ----------
        destruction : int, mandatory
            job destruction
        """
        self.__destruction = destruction

    def get_destruction(self):
        """Returns the job destruction value

        Returns
        -------
        The job destruction value
        """
        return self.__destruction

    def set_locationid(self, locationid):
        """Sets the job location identifier

        Parameters
        ----------
        locationid : str, mandatory
            job location identifier
        """
        self.__locationId = locationid

    def get_locationid(self):
        """Returns the job location identifier

        Returns
        -------
        The job location identifier
        """
        return self.__locationId

    def set_name(self, name):
        """Sets the job name

        Parameters
        ----------
        name : str, mandatory
            job name
        """
        self.__name = name

    def get_name(self):
        """Returns the job name

        Returns
        -------
        The job name
        """
        return self.__name

    def set_quote(self, quote):
        """Sets the job quote

        Parameters
        ----------
        quote : int, mandatory
            job quote
        """
        self.__quote = quote

    def get_quote(self):
        """Returns the job quote

        Returns
        -------
        The job quote
        """
        return self.__quote

    def set_parameter(self, key, value):
        """Sets a job parameter

        Parameters
        ----------
        key : str, mandatory
            job parameter key
        value : str, mandatory
            job parameter value
        """
        self.__parameters[key] = value

    def get_parameter(self, key):
        """Returns a job parameter

        Parameters
        ----------
        key : str, mandatory
            job parameter key

        Returns
        -------
        The job parameter value
        """
        return self.__parameters[key]

    def get_parameters(self):
        """Returns the job parameters

        Returns
        -------
        The job parameters (a list)
        """
        return self.__parameters

    def set_errmessage(self, value):
        """Sets the job error message

        Parameters
        ----------
        value : str, mandatory
            job error message
        """
        self.__errMessage = value

    def get_errmessage(self):
        """Return the job error message

        Returns
        -------
        The job error message
        """
        return self.__errMessage

    def get_data(self):
        """Returns the job results (Astroquery API specification)
        This method will block if the job is asynchronous and the job has not
        finished yet.

        Returns
        -------
        The job results (astropy.table).
        """
        return self.get_results()

    def get_results(self, verbose=False, authentication=None):
        """Returns the job results
        This method will block if the job is asynchronous and the job has not
        finished yet.

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        authentication : AuthMethod object, mandatory, default 'None'
            authentication object to use

        Returns
        -------
        The job results (astropy.table).
        """
        if self.__results is not None:
            return self.__results
        # Try to load from server: only async
        if not self.__async:
            # sync: result is in a file
            return None
        else:
            # async: result is in the server once the job is finished
            self.__load_async_job_results(authentication)
            return self.__results

    def set_results(self, results):
        """Sets the job results

        Parameters
        ----------
        results : Table object, mandatory
            job results
        """
        self.__results = results
        self.__resultInMemory = True

    def save_results(self, verbose=False, authentication=None):
        """Saves job results
        If the job is asynchronous, this method will block until the results
        are available.

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        authentication : AuthMethod object, mandatory, default 'None'
            authentication object to use
        """
        output = self.get_output_file()
        output_format = self.get_output_format()
        if self.__resultInMemory:
            self.__results.write(output, format=output_format)
        else:
            if self.is_sync():
                # sync: cannot access server again
                if verbose:
                    print("No results to save")
            else:
                # Async
                self.wait_for_job_end(authentication, verbose)
                if authentication.get_auth_method() == 'netrc':
                    prefix = 'auth-async/'
                else:
                    prefix = 'async/'
                context = prefix + str(self.__jobid) + "/results/result"
                if authentication.get_auth_method() == 'certificate':
                    response = self.__connHandler.execute_get_secure(
                        context, authentication=authentication)
                else:
                    response = self.__connHandler.execute_get(
                        context, authentication=authentication)
                if verbose:
                    print('GET save results response',
                          response.status, response.reason)
                    print(response.getheaders())

                numberOfRedirects = 0
                while (response.status == 303 or response.status == 302) and \
                        numberOfRedirects < 20:
                    joblocation = self.__connHandler.find_header(
                        response.getheaders(), "location")
                    if authentication.get_auth_method() == 'certificate':
                        response = self.__connHandler.execute_get_secure(
                            context,
                            otherlocation=joblocation,
                            authentication=authentication)
                    else:
                        response = self.__connHandler.execute_get(
                            context,
                            otherlocation=joblocation,
                            authentication=authentication)
                    numberOfRedirects += 1
                    if verbose:
                        print('GET save results redirect response',
                              response.status, response.reason)
                        print(response.getheaders())
                isError = self.__connHandler.check_launch_response_status(
                    response,
                    verbose,
                    200)
                if isError:
                    if verbose:
                        print(response.reason)
                    raise Exception(response.reason)
                self.__connHandler.save_to_file(output, response)

    def wait_for_job_end(self, authentication=None, verbose=False):
        """Waits until a job is finished

        Parameters
        ----------
        authentication : AuthMethod object, mandatory, default 'None'
            authentication object to use
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        currentResponse = None
        responseData = None
        while True:
            responseData = self.get_phase(authentication, update=True)
            currentResponse = self.__last_phase_response_status

            lphase = responseData.lower().strip()
            if verbose:
                print("Job " + self.__jobid + " status: " + lphase)
            if "pending" != lphase and "queued" != lphase and \
                    "executing" != lphase:
                break
            # PENDING, QUEUED, EXECUTING, COMPLETED, ERROR, ABORTED, UNKNOWN,
            # HELD, SUSPENDED, ARCHIVED:
            time.sleep(0.5)
        return currentResponse, responseData

    def __load_async_job_results(self, authentication=None, debug=False):
        wjResponse, wjData = self.wait_for_job_end(authentication)
        if wjData != 'COMPLETED':
            if wjData == 'ERROR':
                if self.is_async() is True:
                    subcontext = 'async/' + self.get_jobid()
                else:
                    subcontext = 'sync/' + self.get_jobid()
                if authentication.get_auth_method() == 'certificate':
                    errresponse = self.__connHandler.execute_get_secure(
                        subcontext,
                        authentication=authentication)
                else:
                    errresponse = self.__connHandler.execute_get(
                        subcontext,
                        authentication=authentication)
                JobSaxParser = astroquery.cadc.tap.xmlparser.jobSaxParser. \
                    JobSaxParser
                # parse job
                jsp = JobSaxParser(async_job=False)
                errjob = jsp.parseData(errresponse)[0]
                errjob.set_connhandler(self.__connHandler)
                raise requests.exceptions.HTTPError(
                    errjob.get_errmessage())
            else:
                raise requests.exceptions.HTTPError(
                    'Error running query, PHASE: '+wjData)
        if authentication.get_auth_method() == 'netrc':
            subContext = "auth-async/" + str(self.__jobid) + "/results/result"
        else:
            subContext = "async/" + str(self.__jobid) + "/results/result"
        if authentication.get_auth_method() == 'certificate':
            resultsResponse = self.__connHandler.execute_get_secure(
                subContext,
                authentication=authentication)
        else:
            resultsResponse = self.__connHandler.execute_get(
                subContext,
                authentication=authentication)
        if debug:
            print(resultsResponse.status, resultsResponse.reason)
            print(resultsResponse.getheaders())

        numberOfRedirects = 0
        while (resultsResponse.status == 303 or resultsResponse.status == 302)\
                and numberOfRedirects < 20:
            joblocation = self.__connHandler.find_header(
                resultsResponse.getheaders(),
                "location")
            if authentication.get_auth_method() == 'certificate':
                resultsResponse = self.__connHandler.execute_get_secure(
                    subContext,
                    otherlocation=joblocation,
                    authentication=authentication)
            else:
                resultsResponse = self.__connHandler.execute_get(
                    subContext,
                    otherlocation=joblocation,
                    authentication=authentication)
            numberOfRedirects += 1
            if debug:
                print(resultsResponse.status, resultsResponse.reason)
                print(resultsResponse.getheaders())

        isError = self.__connHandler.check_launch_response_status(
            resultsResponse,
            debug,
            200)
        if isError:
            if debug:
                print(resultsResponse.reason)
            raise Exception(resultsResponse.reason)
        else:
            outputFormat = self.get_output_format()
            results = utils.read_http_response(resultsResponse, outputFormat)
            self.set_results(results)
            self.__phase = wjData

    def __str__(self):
        if self.__results is None:
            result = "None"
        else:
            result = self.__results.info()
        return "Jobid: " + str(self.__jobid) + \
            "\nPhase: " + str(self.__phase) + \
            "\nOwner: " + str(self.__ownerid) + \
            "\nOutput file: " + str(self.__outputFile) + \
            "\nResults: " + str(result)
