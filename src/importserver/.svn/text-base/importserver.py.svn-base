#!/usr/bin/python

import time
import SocketServer
import optparse
import threading
import logging
import pickle
import struct
import copy
import ConfigParser
import os
import sys
from subprocess import *

def R(cmd, local_vars):
  G = copy.copy(globals())
  G.update(local_vars)
  return cmd.format(**G)

def checkpid(pid_file):
  try:
    with open(pid_file, 'r') as fd:
      pid = int(fd.read().strip())
  except IOError:
    pid = None
  if pid:
    try:
      os.kill(pid, 0)
    except OSError:
      pid = None
  if pid:
    message = "program has been exist: pid={0}\n".format(pid)
    sys.stderr.write(message)
    sys.exit(1)

def writepid():
  try:
    pid = str(os.getpid())
    with open(pid_file, 'w') as fd:
      fd.write("{0}\n".format(pid))
  except Exception as err:
    logging.exception('ERROR: {0}'.format(err))

def Daemonize():
  try:
    pid = os.fork()
    if pid > 0:
      # exit first parent
      sys.exit(0)
  except OSError, e:
    sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
    sys.exit(1)

  # decouple from parent environment
  #os.chdir("/")
  os.setsid()
  os.umask(0)

  # do second fork
  try:
    pid = os.fork()
    if pid > 0:
      # exit from second parent
      sys.exit(0)
  except OSError, e:
    sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
    sys.exit(1)

  # redirect standard file descriptors
  sys.stdout.flush()
  sys.stderr.flush()
  si = file('/dev/zero', 'r')
  so = file('/dev/null', 'a+')
  se = file('/dev/null', 'a+', 0)
  os.dup2(si.fileno(), sys.stdin.fileno())
  os.dup2(so.fileno(), sys.stdout.fileno())
  os.dup2(se.fileno(), sys.stderr.fileno())

  writepid()

class ExecutionError(Exception): pass

class Shell:
  @classmethod
  def popen(cls, cmd, host=None, username=None):
    '''Execute a command locally, and return
    >>> Shell.popen('ls > /dev/null')
    ''
    '''
    if host is not None:
      if username is not None:
        cmd = "ssh {username}@{host} '{cmd}'".format(**locals())
      else:
        cmd = "ssh {host} '{cmd}'".format(**locals())
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    output = p.communicate()[0]
    err = p.wait()
    if err:
        output = 'Shell.popen({0})=>{1} Output=>"{2}"'.format(cmd, err, output)
        raise ExecutionError(output)
    return output
 
def SendPacket(sock, pkg_type, data):
  '''
  packet format
      ---------------------------------------------------
      |    Packet Type (8B)    |    Data Length (8B)    |
      ---------------------------------------------------
      |                      Data                       |
      ---------------------------------------------------
  '''
  buf = pickle.dumps(data)
  packetheader = struct.pack('!ll', pkg_type, len(buf))
  sock.sendall(packetheader)
  sock.sendall(buf)

def Receive(sock, length):
  result = ''
  while len(result) < length:
    data = sock.recv(length - len(result))
    if not data:
      break
    result += data
  if len(result) == 0:
    return None
  if len(result) < length:
    raise Exception('Do not receive enough data: '
        'length={0} result_len={1}'.format(length, len(result)))
  return result

def ReceivePacket(sock):
  '''
  receive a packet from socket
  return type is a tuple (PacketType, Data)
  '''
  packetformat = '!ll'
  packetheaderlen = struct.calcsize(packetformat)
  packetheader = Receive(sock, packetheaderlen)
  if packetheader is not None:
    (packettype, datalen) = struct.unpack(packetformat, packetheader)
    data = pickle.loads(Receive(sock, datalen))
    return (packettype, data)
  else:
    return (None, None)


class ImportServer(SocketServer.ThreadingMixIn,
    SocketServer.TCPServer):
  pass

class RequestHandler(SocketServer.StreamRequestHandler):
  GlobalLock = threading.Lock()

  def invoke_dispatch(self, inputs, options):
    try:
      import_list = []
      for i in inputs:
        input_dir = i[0]
        table_name = i[1]
        ic = R('{conf_dir}/{app_name}/{table_name}/sample.conf,{input_dir}', locals())
        import_list.append(ic)
      dpch_arg = ' '.join(import_list)
      logging.debug('dpch_arg is {0}'.format(dpch_arg))
      dispatch_cmd = './dispatch.sh {0} -b -c {1}'.format(options, dpch_arg)
      logging.debug('dispatch_cmd is {0}'.format(dispatch_cmd))
      output = Shell.popen(dispatch_cmd)
      ret = 0
    except Exception as err:
      output = 'ERROR: ' + str(err)
      logging.error(output)
      ret = 1
    return (output, ret)

  def overwrite_op(self, inputs):
    options = '-f -m -g gateway_host_file'
    (output, ret) = self.invoke_dispatch(inputs, options)
    logging.debug('dispatch_cmd output is {0}'.format(output))
    if ret == 0:
      SendPacket(self.connection, SUBMIT_OVERWRITE_TASK_RES_PKG, 'SUCCESSFUL')
    else:
      SendPacket(self.connection, SUBMIT_OVERWRITE_TASK_RES_PKG, 'FAILED')

  def import_op(self, inputs):
    options = ''
    (output, ret) = self.invoke_dispatch(inputs, options)
    logging.debug('dispatch_cmd output is {0}'.format(output))
    if ret == 0:
      SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, 'SUCCESSFUL')
    else:
      SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, 'FAILED')

  def handle(self):
    try:
      timer = 0
      while not self.GlobalLock.acquire(False):
        SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG,
            'Waiting for another import task, {0} seconds'.format(timer))
        time.sleep(10)
        timer += 10
      (packettype, data) = ReceivePacket(self.connection)
      if packettype == SUBMIT_OVERWRITE_TASK_PKG:
        self.overwrite_op(data)
      elif packettype == SUBMIT_IMPORT_TASK_PKG:
        self.import_op(data)
      else:
        SendPacket(self.connection, ERROR_PKG, 'FAILED')
        
    except Exception as err:
      logging.error("ERROR " + str(err))
    finally:
      try:
        if self.GlobalLock is not None:
          self.GlobalLock.release()
      except Exception:
        pass

def ParseArgs():
  try:
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config_file',
        help='Configuration file',
        dest='config_file', default='importserver.conf')
    (options, args) = parser.parse_args()
    config = ConfigParser.RawConfigParser()
    config.read(options.config_file)

    log_file    = config.get('public', 'log_file')
    log_level   = config.get('public', 'log_level')
    pid_file    = config.get('public', 'pid_file')
    listen_port = config.getint('importserver', 'port')
    app_name    = config.get('importserver', 'app_name')
    conf_dir    = config.get('importserver', 'conf_dir')
    rs_ip       = config.get('rootserver', 'vip')
    rs_port     = config.getint('rootserver', 'port')

    checkpid(pid_file)

    LEVELS = {'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL}
    level = LEVELS.get(log_level.lower(), logging.NOTSET)
    logging.basicConfig(filename=log_file, level=level,
        format='[%(asctime)s] %(levelname)s %(message)s')

    logging.info('pid_file    => ' + str(pid_file))
    logging.info('listen_port => ' + str(listen_port))
    logging.info('app_name    => ' + app_name)
    logging.info('conf_dir    => ' + conf_dir)
    logging.info('rs_ip       => ' + rs_ip)
    logging.info('rs_port     => ' + str(rs_port))
    globals()['pid_file']     = pid_file
    globals()['listen_port']  = listen_port
    globals()['app_name']     = app_name
    globals()['conf_dir']     = conf_dir
    globals()['rs_ip']        = rs_ip
    globals()['rs_port']      = rs_port
  except Exception as err:
    logging.exception('ERROR: ' + str(err))

def Main():
  try:
    server = ImportServer(("", listen_port), RequestHandler)
    server.serve_forever()
  except Exception as err:
    logging.exception("ERROR: {0}".format(err))
  finally:
    if server is not None:
      server.shutdown()

SUBMIT_IMPORT_TASK_PKG     = 100
SUBMIT_IMPORT_TASK_RES_PKG = 101
SUBMIT_OVERWRITE_TASK_PKG     = 102
SUBMIT_OVERWRITE_TASK_RES_PKG = 103
ERROR_PKG = 200

if __name__ == '__main__':
  ParseArgs()
  Daemonize()
  Main()

