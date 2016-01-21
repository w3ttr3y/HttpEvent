#!/usr/bin/env python

import json
import requests
import time

# Only used for default get hostname implementation
import socket

#Only used for default get time implementation
import time

class HttpEventCollector:
	"""Send events to Splunk's HttpEventCollector

	This class is meant to send data to an instance of Splunk's i
	HttpEventCollector for indexing.  It has been designed with the hope that it 
	will be flexible enough to allow it to function in diverse environments.

	Keyword Arguments:
	token - the event collector token provided by the Splunk administrator
	server - the server (hostname, fqdn, or ip as appropriate) to connect to Default: localhost
	port - the port to connect on Default:8088
	ssl - Boolean indicating if ssl should be used
	verify - Boolean or string Boolean indicates if the certificate should be validated.  If a string, it should be a path to the CA certificate to use for validation or to a directory of CA certs to use
	url_path - the path to the event collector's endpoint; this is hard coded in Splunk so shouldn't change unless a newer version changes it
	url_path_prefix - the prefix to use before url_path ; this is insert between the port and the url_path
	metadata - metadata fields (host, index, source, sourcetype, time) to add to events
	headers  - hash from which http headers are built
	get_host - a function which accepts one argument of an event which returns the hostname; only used if add_host is True at either the class or function level
	add_host - boolean indicating if the host value should be added to the event's metadata
	get_time - a function which accepts one argument of an event and which returns the time; only used if add_time is True at either the class or function level
	add_time - boolean indicating if a time value should be added to the event's metadata
	valid_metadata_fields - valid metadata fields ; used to filter the metadata
	"""

	def __init__(self, token, server="localhost", port=8088, ssl=True, verify=True, url_path='/services/collector/event', url_path_prefix='', metadata={}, headers={}, get_host = lambda e: socket.gethostname(), add_host = False, get_time = lambda e: str(int(time.time())), add_time = False, valid_metadata_fields = ['host', 'source', 'sourcetype', 'time']):
		self.server          = server
		self.port            = 8088
		self.ssl             = ssl
		self.url_path_prefix = url_path_prefix
		self.url_path        = url_path
		self.verify          = verify
		self._url()
		self._header(headers, token)
		self._get_host       = get_host
		self.add_host        = add_host
		self._get_time       = get_time
		self.add_time        = add_time

	def _header(self, headers, token):
		self._headers = headers
		if 'Authorization' not in headers:
			self._headers['Authorization'] = 'Splunk ' + token
		else:
			pass #TODO: log that we're not using token by defaulting to what was in headers

	def _url(self):
		try:
			return self._uri
		except AttributeError:
			self._uri = ''.join([
					'https' if self.ssl else 'http',
					'://',
					self.server,
					':',
					str(self.port),
					'/' if len(self.url_path_prefix) > 0 and self.url_path_prefix[0] != '/' else '',
					self.url_path_prefix if len(self.url_path_prefix) == 0 or self.url_path_prefix[-1] != '/' or self.url_path[0] != '/' else self.url_path_prefix[:-1],
					'/' if self.url_path[0] != '/' and len(self.url_path_prefix) > 0 and self.url_path_prefix[-1] != '/' else '',
					self.url_path,
			])
			return self._uri

	def send(self, data, metadata={}, headers={}, addHost = self.add_host, addTime = self.add_time, get_host = self._get_host, get_time = self._get_time):
		"""
    send data to the http event collector

		Arguments:
		data     - the event to send as a dictionary to be converted to JSON
		metadata - a dictionary of fields to add as metadata (e.g. time, source, sourcetype, or host)
		addHost  - boolean indicating if a hostname should be added to the metadata
		addTime  - boolean indicating if a timestamp should be added to the meatdata 
		"""
	 
		if addHost:
			headers.update({'host': get_host()})

		if addTime:
			headers.update({'time': get_time()})
		
		metadata.update({"event": data})
		self.sendEvent(metadata, headers=headers)

	def sendEvent(self, event, headers={}):
		"""
    send data to the http event collector

		Arguments:
		event   - the event to send to the event collector.  This must be a dictionary object... (todo: document this more)
		headers - http header to use when sending the data
		"""
		self.sendEventJSON(json.dumps(event), headers)

	def sendEventJSON(self, event, headers={}):
		"""
    send data to the http event collector

		Arguments:
		event   - the event to send to the event collector.  This must be a json object... (todo: document this more)
		headers - http header to use when sending the data
		"""
		headers = self._headers
		headers.update(headers)
		r = requests.post(self._url(), data = json.dumps(event), headers=headers, verify=self.verify)
		r.raise_for_status()

if __name__ == '__main__':
	TOKEN = "C4457A9C-E528-49FA-8382-D1837A24986C"
	eventtime = str(int(time.time()))
	payload = {'policy': '39', 'src_ip': '127.0.0.10'}
	HttpEventCollecter(token=TOKEN, server="localhost", port="8088", ssl=False, verify=False, 
			metadata={time: str(int(time.time()))}).send(payload)
