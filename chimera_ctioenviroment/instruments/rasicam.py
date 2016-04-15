import datetime
import threading

import xmltodict as xmltodict
from astropy import units
from chimera.core.exceptions import OptionConversionException
from chimera.instruments.weatherstation import WeatherBase
from chimera.interfaces.weatherstation import WeatherTransparency, WSValue
import requests


class Rasicam(WeatherBase, WeatherTransparency):
    """
    Instrument that gets information from RASICAM web page
    """


    __config__ = dict(model="RASICAM all sky camera")

    def __start__(self):
        """
        Start a thread that will be querying the rasicam forever and ever...
        """
        self.__stop = False
        self._results = None
        p = threading.Thread(target=self._watch)
        p.start()

    def __stop__(self):
        self.__stop = True

    def _update(self, data):
        """
        Updates with the RASICAM results
        """
        self._results = dict(stdev=data['StDev'],
                             transparency=100. * (
                                 float(data['StDev']['GlobalStDev']) <= float(data['StDev']['StDevThresh'])),
                             last_update=datetime.datetime.utcnow())
        self.log.debug('Updated RASICAM data: ' + self._results.__str__())

    def _watch(self):
        """
        Watches RASICAM for data
        """
        while 1:

            if self.__stop:
                return

            data = requests.get('http://rasicam.ctio.noao.edu/RASICAMWebService/vi/')

            self.log.debug('RASICAM.text >>>' + data.text)
            if not 'Error Updating Status' in data.text:
                aux_dict = xmltodict.parse(data.text)
                if 'ChartData' in aux_dict.keys():
                    aux_dict = aux_dict['ChartData']
                    self.log.debug('RASICAM.dict >>' + aux_dict.__str__())
                    if aux_dict['ResponseType'] == 'Chart' and 'StDev' in aux_dict:
                        self._update(aux_dict)
                else:
                    self.log.debug('Data is not Chart:' + aux_dict.__str__())
            else:
                self.log.debug('Skipping Error Updating Status...')

    def sky_transparency(self, unit_out=units.pct):
        """
        Returns, in percent, the sky transparency
        :param unit_out:
        """
        if unit_out not in self.__accepted_transparency_unit__:
            raise OptionConversionException("Invalid transparency unit %s." % unit_out)

        if self._results is not None:
            return WSValue(self._results['last_update'], self._results['transparency'], unit_out)
        else:
            return False
