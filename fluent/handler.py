# -*- coding: utf-8 -*-

import logging
import socket
import cStringIO
import traceback

try:
    import simplejson as json
    from simplejson import JSONDecodeError 
except ImportError:
    import json

    class JSONDecodeError(ValueError):
        pass

from fluent import sender


class FluentRecordFormatter(object):
    def __init__(self, fmt=None):
        self.hostname = socket.gethostname()
        self.fmt = fmt

    def format(self, record):
        if self.fmt and isinstance(self.fmt, dict):
            data = self.format_data(record)
        else:
            data = {'sys_host': self.hostname,
                    'sys_name': record.name,
                    'sys_module': record.module,
                    # 'sys_lineno': record.lineno,
                    # 'sys_levelno': record.levelno,
                    # 'sys_levelname': record.levelname,
                    # 'sys_filename': record.filename,
                    # 'sys_funcname': record.funcName,
                    # 'sys_exc_info': record.exc_info,
                    }
            # if 'sys_exc_info' in data and data['sys_exc_info']:
            #    data['sys_exc_info'] = self.formatException(data['sys_exc_info'])

        self._structuring(data, record.msg)
        return data

    def _structuring(self, data, msg):
        if isinstance(msg, dict):
            self._add_dic(data, msg)
        elif isinstance(msg, str):
            try:
                self._add_dic(data, json.loads(str(msg)))
            except (ValueError, JSONDecodeError):
                pass

    def format_data(self, record):
        data = {}
        for k, i in self.fmt.iteritems():
            if i in record.__dict__.keys():
                if i == 'exc_info' and record.exc_info:
                    data[k] = self.format_exception(record.exc_info)
                else:
                    if i == 'msg' and isinstance(record.msg, dict):
                        pass
                    else:
                        data[k] = record.__dict__[i]
            else:
                data[k] = i
        return data

    @staticmethod
    def _add_dic(data, dic):
        for key, value in dic.items():
            if isinstance(key, basestring):
                data[str(key)] = value

    @staticmethod
    def format_exception(ei):
        """
        Format and return the specified exception information as a string.

        This default implementation just uses
        traceback.print_exception()
        """
        sio = cStringIO.StringIO()
        traceback.print_exception(ei[0], ei[1], ei[2], None, sio)
        s = sio.getvalue()
        sio.close()
        if s[-1:] == "\n":
            s = s[:-1]
        return s


class FluentHandler(logging.Handler):
    """
    Logging Handler for fluent.
    """
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 timeout=3.0,
                 verbose=False):

        self.tag = tag
        self.sender = sender.FluentSender(tag,
                                          host=host, port=port,
                                          timeout=timeout, verbose=verbose)
        logging.Handler.__init__(self)

    def emit(self, record):
        data = self.format(record)
        self.sender.emit(None, data)

    def close(self):
        self.acquire()
        try:
            self.sender._close()
            logging.Handler.close(self)
        finally:
            self.release()
