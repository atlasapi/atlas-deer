#!/usr/bin/python

import argparse
import datetime
import dateutil.parser
import httplib
import json
import sys

arg_parser = argparse.ArgumentParser(description='bootstrap schedules.')

arg_parser.add_argument('-host', dest='host', metavar='host', help='host to bootstrap')
arg_parser.add_argument('-offset', dest='offset', type=int, nargs='?', metavar='offset', help='initial platform offset')
arg_parser.add_argument('-source', metavar='source', help='source of the schedules to bootstrap')
arg_parser.add_argument('-platform', metavar='platform', nargs='?', help='platform of channels to bootstrap')
arg_parser.add_argument('-start', metavar='start', help='start day of schedules to bootstrap')
arg_parser.add_argument('-end', metavar='end', nargs='?', help='end day of schedules to boostrap')
arg_parser.add_argument('channels', metavar='channels', nargs='*', help="channels to bootstrap, overrides platform")

args = arg_parser.parse_args();

source = args.source
platform = args.platform
start = dateutil.parser.parse(args.start).date()
end = start if args.end == None else dateutil.parser.parse(args.end).date()
host = args.host

v4_channel_prefix = "http://atlas.metabroadcast.com/4.0/channels/"

def color(c, val):
  if sys.stdout.isatty():
    return "\x1b[%sm%s\x1b[0m" % (c, val)
  return val

def days(start,end):
  cur = start
  while cur <= end:
    yield cur
    cur = cur + datetime.timedelta(1)

def param_str(params):
  return "&".join(["%s=%s"%(k,v) for k,v in params.iteritems() if not v == None])

class Struct:
  def __init__(self, **entries):
    self.__dict__.update(entries)

def get_channels(platform, limit, offset):
  channelHost = 'atlas.metabroadcast.com'
  params = {"platforms":platform,"limit":limit,"offset":offset}
  channelResource = '/3.0/channels.json?%s' % param_str(params)
  channelConn = httplib.HTTPConnection(channelHost)
  channelConn.request('GET', channelResource)
  print color("35", "GET http://%s%s\x1b[0m" % (channelHost, channelResource))
  channelResp = channelConn.getresponse()
  return Struct(**json.loads(channelResp.read())).channels

def cids(chan):
  als = [a for a in chan['aliases'] if a.startswith(v4_channel_prefix)]
  return [(chan['id'],a[len(v4_channel_prefix):]) for a in als]

def bootstrap_days(conn, v4id, v3id=None):
  print color("33","bootstrapping %s/%s" % (v4id,v3id))
  for day in days(start, end):
    bootstrap_day(conn, v4id, day)

def bootstrap_day(conn, cid, day):
  params = {"source":source,"channel":cid,"day":day}
  resource = "/system/bootstrap/schedule?%s" % param_str(params)
  conn.request('POST', resource)
  sys.stdout.write("%s %s " % (day, color("36", "POST http://%s%s\x1b[0m" % (host, resource))))
  sys.stdout.flush()
  resp = conn.getresponse()
  if resp.status >= 400:
    print color("41", "%s %s" % (resp.status, resp.reason))
    resp.read()
    return
  result = resp.read();
  print color("32" if "0 fail" in result else "31", result)

conn = httplib.HTTPConnection(host)

if len(args.channels) > 0:
  for cid in args.channels:
    bootstrap_days(conn,cid)
  sys.exit()

limit = 5
offset = 0 if args.offset == None else args.offset
channels = get_channels(platform, limit, offset)

if len(channels) == 0:
  print color("31", "no channels found")
  sys.exit()

while (len(channels) > 0):
  for chan in channels:
    for v3id, v4id in cids(chan):
      print ""
      bootstrap_days(conn, v4id, v3id)
  offset = offset + limit
  channels = get_channels(platform, limit, offset)
