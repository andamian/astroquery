# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
TAP plus
=============

"""
import time
import requests
import astroquery.cadc.cadctap.jobSaxParser
from astroquery.utils.tap.model.job import Job
from astroquery.cadc.cadctap import utils

__all__ = ['Job']


class JobCadc(Job):
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
        super(JobCadc, self).__init__(async_job=async_job,
                                      query=query,
                                      connhandler=connhandler)
        self.errmessage = None

    def get_phase(self, update=False):
        """Returns the job phase. May optionally update the job's phase.

        Parameters
        ----------
        update : bool
            if True, the phase will by updated by querying the server before
            returning.

        Returns
        -------
        The job phase
        """
        if update:
            phase_request = 'async/' + str(self.jobid) + '/phase'
            response = self.connHandler.execute_get(phase_request)

            self._Job__last_phase_response_status = response.status
            if response.status != 200:
                raise Exception(response.reason)
            self._Job__phase = str(response.read().decode('utf-8'))
        return self._Job__phase

    def get_results(self, verbose=False):
        """Returns the job results
        This method will block if the job is asynchronous and the job has not
        finished yet.

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        The job results (astropy.table).
        """
        if self.results is not None:
            return self.results
        # Try to load from server: only async
        if not self.async_:
            # sync: result is in a file
            return None
        else:
            # async: result is in the server once the job is finished
            self.__load_async_job_results()
            return self.results

    def save_results(self, filename, verbose=False):
        """Saves job results
        If the job is asynchronous, this method will block until the results
        are available.

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        output = filename
        output_format = self.parameters['format']
        if self._Job__resultInMemory:
            if output_format == 'csv':
                output_format = 'ascii.csv'
            if "tsv" == output_format:
                output_format = "ascii.fast_tab"

            self.results.write(output, format=output_format)
        else:
            if not self.async_:
                # sync: cannot access server again
                print("No results to save")
            else:
                # Async
                self.wait_for_job_end(verbose)
                context = 'async/' + str(self.jobid) + "/results/result"
                response = self.connHandler.execute_get(context)
                if verbose:
                    print(response.status, response.reason)
                    print(response.getheaders())

                numberOfRedirects = 0
                while (response.status == 303 or response.status == 302) and \
                        numberOfRedirects < 20:
                    joblocation = self.connHandler.find_header(
                        response.getheaders(), "location")
                    response = self.connHandler.execute_get_other(joblocation)
                    numberOfRedirects += 1
                    if verbose:
                        print(response.status, response.reason)
                        print(response.getheaders())
                isError = self.connHandler.check_launch_response_status(
                    response,
                    verbose,
                    200)
                if isError:
                    if verbose:
                        print(response.reason)
                    raise Exception(response.reason)
                self.connHandler.dump_to_file(output, response)

    def wait_for_job_end(self, verbose=False):
        """Waits until a job is finished

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        currentResponse = None
        responseData = None
        while True:
            responseData = self.get_phase(update=True)
            currentResponse = self._Job__last_phase_response_status

            lphase = responseData.lower().strip()
            if verbose:
                print("Job " + self.jobid + " status: " + lphase)
            if "pending" != lphase and "queued" != lphase and \
                    "executing" != lphase:
                break
            # PENDING, QUEUED, EXECUTING, COMPLETED, ERROR, ABORTED, UNKNOWN,
            # HELD, SUSPENDED, ARCHIVED:
            time.sleep(0.5)
        return currentResponse, responseData

    def __load_async_job_results(self, debug=False):
        wjResponse, wjData = self.wait_for_job_end()
        if wjData != 'COMPLETED':
            if wjData == 'ERROR':
                subcontext = 'async/' + self.jobid
                errresponse = self.connHandler.execute_get(
                    subcontext)
                # parse job
                jsp = astroquery.cadc.cadctap.jobSaxParser. \
                    JobSaxParserCadc(async_job=False)
                errjob = jsp.parseData(errresponse)[0]
                errjob.connHandler = self.connHandler
                raise requests.exceptions.HTTPError(
                    errjob.errmessage)
            else:
                raise requests.exceptions.HTTPError(
                    'Error running query, PHASE: '+wjData)
        subContext = "async/" + str(self.jobid) + "/results/result"
        resultsResponse = self.connHandler.execute_get(subContext)
        if debug:
            print(resultsResponse.status, resultsResponse.reason)
            print(resultsResponse.getheaders())

        numberOfRedirects = 0
        while (resultsResponse.status == 303 or resultsResponse.status == 302)\
                and numberOfRedirects < 20:
            joblocation = self.connHandler.find_header(
                resultsResponse.getheaders(),
                "location")
            resultsResponse = self.connHandler.execute_get_other(joblocation)
            numberOfRedirects += 1
            if debug:
                print(resultsResponse.status, resultsResponse.reason)
                print(resultsResponse.getheaders())

        isError = self.connHandler.check_launch_response_status(
            resultsResponse,
            debug,
            200)
        if isError:
            if debug:
                print(resultsResponse.reason)
            raise Exception(resultsResponse.reason)
        else:
            outputFormat = self.parameters['format']
            results = utils.read_http_response(resultsResponse, outputFormat)
            self.set_results(results)
            self._Job__phase = wjData
