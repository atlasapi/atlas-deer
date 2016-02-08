#!/usr/bin/python

import argparse
import datetime
import dateutil.parser
import httplib
import json
import sys
from tabulate import tabulate

arg_parser = argparse.ArgumentParser(description='Compare Atlas schedules.')
arg_parser.add_argument('-h1', dest='host1', default='atlas.metabroadcast.com', metavar='host1', help='Remote host for 1st schedule')
arg_parser.add_argument('-h2', dest='host2', default='atlas.metabroadcast.com', metavar='host2', help='Remote host for 2nd schedule')
arg_parser.add_argument('-p1', dest='port1', default=80, type=int, metavar='port1', help='Port for 1st schedule')
arg_parser.add_argument('-p2', dest='port2', default=80, type=int, metavar='port2', help='Port for 2nd schedule')
arg_parser.add_argument('-k', dest='key', help='API key to use')
arg_parser.add_argument('-platform', metavar='platform', nargs='?', help='platform of channels to compare')

arg_parser.add_argument('source', metavar='source', help='Source of the schedules to compare')
arg_parser.add_argument('start', metavar='start', help='Start time of schedules to compare')
arg_parser.add_argument('end', metavar='end', help='End time of scheduxles to compare')

args = arg_parser.parse_args();

args.start = dateutil.parser.parse(args.start)
args.end = dateutil.parser.parse(args.end)

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

class SimpleEntry:
    def __init__(self, id, title, bid, start, end):
        self.id = id
        self.title = title
        self.bid = bid
        self.start = start
        self.end = end

    def __str__(self):
        return "%s %s %s %s %s" % (self.id,self.title,self.bid,self.start,self.end)

    def __repr__(self):
        return self.__str__()

    def as_list(self):
        fmt = "%H:%M:%S"
        start = self.start.strftime(fmt)
        end = self.end.strftime(fmt)
        return [start, end, self.bid, self.id, self.title[:15]]

class Atlas:
    def __init__(self, host, port, version, key):
        self.host = host
        self.port = port
        self.version = version
        self.key = key

    def get_schedule(self, source, channel, start, end):
        resource = self.get_resource(source, channel, start, end)
        request, response = self.get(resource)
        return (request, self.simplify(response))

    def get(self, resource):
        conn = httplib.HTTPConnection(self.host, self.port)
        col = "32" if self.version == 3 else "33"
        request = color(col, "GET http://%s:%s%s" % (self.host, self.port, resource))
        conn.request('GET', resource)
        resp = conn.getresponse()
        if not resp.status == 200:
            if resp.status == 400:
                print "request failed for %s: %s" % (resource, resp.reason)
            if resp.status == 404:
                print "resource %s doesn't appear to exist" % (resource)
            if resp.status >= 500:
                print "problem with %s? %s %s" % (self.host, resp.status, resp.reason)
            resp.read()
            conn.close()
            sys.exit()
        body = resp.read()
        try:
            response = Struct(**json.loads(body))
        except Exception as e:
            print "couldn't decode response to %s: %s" % (request, e)
            print body
            sys.exit()
        return (request, response)

    def v4_channel_id(self, channel):
        prefix = 'http://atlas.metabroadcast.com/4/channels/'
        for a in channel.aliases:
            if a.startswith(prefix):
                return a[len(prefix):]
        raise Exception("couldn't find v4 channel id for %s" % channel.id)

    def join(self, params):
        return param_str(params)

    def get_resource(self, source, channel, start, end):
        if (self.version == 3):
            params = {
                'publisher':source,
                'channel_id' : channel['v3id'],
                'from':start.isoformat(),
                'to':end.isoformat(),
                'apiKey':self.key
            }
            return "/3.0/schedule.json?%s" % self.join(params)
        if (self.version == 4):
            params = {'source':source,'from':start.isoformat(),'to':end.isoformat(),'key':self.key}
            channel_id = channel['v4id']
            return "/4/schedules/%s.json?annotations=content.description&%s" %(channel_id, self.join(params))
        raise Exception("unexpected version %s" % self.version)

    def simplify(self, schedule):
        if self.version == 4:
            return self.simplify_v4(schedule)
        return self.simplify_v3(schedule)

    def simplify_v4(self, schedule):
        entries = []
        sched_entries = schedule.schedule['entries'];
        for sched_entry in sched_entries:
            i = sched_entry['item']
            b = sched_entry['broadcast']
            bid = b['aliases'][0]['value']
            start = dateutil.parser.parse(b['transmission_time'])
            end = dateutil.parser.parse(b['transmission_end_time'])
            entries.append(SimpleEntry(i['id'], i['title'], bid, start, end))
        return entries

    def simplify_v3(self, schedule):
        entries = []
        sched_entries = schedule.schedule[0]['items']
        for se in sched_entries:
            b = se['broadcasts'][0]
            start = dateutil.parser.parse(b['transmission_time'])
            end = dateutil.parser.parse(b['transmission_end_time'])
            bid = b['id']
            entries.append(SimpleEntry(se['id'],se['title'],bid[bid.index(':')+1:],start,end))
        return entries

headers = ['Title', 'ID', 'BID', 'End', 'Start']

def compare(left, right):
    table = [headers + ["|"] + headers[::-1]]
    difference = False
    l = None
    r = None
    while len(left) > 0 and len(right) > 0:
        l = l if l != None else left.pop(0)
        r = r if r != None else right.pop(0)
        if (l.start > r.start):
            difference = True
            table.append(mismatch(None, r))
            r = None
        elif (l.start < r.start):
            difference = True
            table.append(mismatch(l, None))
            l = None
        else:
            row, diff = matching_start(l, r)
            difference = difference or diff
            table.append(row)
            l = None
            r = None
    if len(left) > 0:
        for l in left:
            difference = True
            table.append(mismatch(l, None))
    else :
        for r in right:
            difference = True
            table.append(mismatch(None, r))
    return (table, difference)

highlight = lambda li: [color("31", v) if i in range(2,4) else v for (i,v) in enumerate(li)]

def matching_start(l, r):
    diff = False
    left = l.as_list()
    right = r.as_list()
    if (l.bid != r.bid):
        left[2] = color('31',left[2])
        right[2] = color('31',right[2])
        diff = True
    if (l.id != r.id):
        left[3] = color('45',left[3])
        right[3] = color('45',right[3])
    return (left[::-1] +["|"]+ right, diff)

red = lambda x : color("41",' ' * x)
missing_row = [red(8),red(8), red(11), red(6), red(15)]

def mismatch(l, r):
    vals_or_missing = lambda e: missing_row if e == None else e.as_list()
    return vals_or_missing(l)[::-1] +["|"]+ vals_or_missing(r)

v4_channel_prefix = "http://atlas.metabroadcast.com/4.0/channels/"

def v4_id(als):
    prfx = v4_channel_prefix
    return [a[len(prfx):] for a in als if a.startswith(prfx)][0]

##
# Currently there's only a v3 /channels resource so we may need to find the v4 id and we may as well
# check that the channel actually exists while were at it.
# There's no guarantee that the hosts specified in the args have a 3.0/channels resource so this is
# hard-coded.
##
def get_channel(cid):
    channelHost = 'atlas.metabroadcast.com'
    channelResource = '/3.0/channels/%s.json' % cid
    channelConn = httplib.HTTPConnection(channelHost)
    channelConn.request('GET', channelResource)
    print color("35","GET http://%s%s" % (channelHost, channelResource))
    channelResp = channelConn.getresponse()
    if not channelResp.status == 200:
        if channelResp.status == 400:
            print "request failed for %s: %s" % (args.channel, channelResp.reason)
        if channelResp.status == 404:
            print "channel %s doesn't appear to exist" % (args.channel)
        if channelResp.status >= 500:
            print "problem with %s? %s %s" % (channelHost, channelResp.status, channelResp.reason)
        channelResp.read()
        channelConn.close()
        sys.exit()
    channel = Struct(**json.loads(channelResp.read())['channels'][0])
    return channel_data(channel)

def channel_data(channel):
    return {'v3id': channel.id, 'v4id': v4_id(channel.aliases), 'title': channel.title}

def get_platform_channels(platform):
    limit = 5
    offset = 0 if args.offset == None else args.offset
    channels = get_channels(platform, limit, offset)
    if len(channels) == 0:
        print "no channels found"
    while (len(channels) > 0):
        for chan in channels:
            yield {'v3id': chan['id'], 'v4id': v4_id(chan['aliases']), 'title': chan['title']}
        offset = offset+limit
        channels = get_channels(platform, limit, offset)

def get_channels(platform, limit, offset):
    channelHost = 'atlas.metabroadcast.com'
    params = {"platforms":platform,"limit":limit,"offset":offset}
    channelResource = '/3.0/channels.json?%s' % param_str(params)
    channelConn = httplib.HTTPConnection(channelHost)
    channelConn.request('GET', channelResource)
    print color("35", "GET http://%s%s\x1b[0m" % (channelHost, channelResource))
    channelResp = channelConn.getresponse()
    return Struct(**json.loads(channelResp.read())).channels

print arg_parser.get_default("host1")

def dflt(p):
    return arg_parser.get_default(p)

def compare_params(channel, day):
    cmd = "./schedule-compare.py"
    if not args.host1 == dflt("host1"):
        cmd += " -h1 " + args.host1
    if not args.host2 == dflt("host2"):
        cmd += " -h2 " + args.host2
    if not args.port1 == dflt("port1"):
        cmd += " -p1 " + str(args.port1)
    if not args.port2 == dflt("port2"):
        cmd += " -p2 " + str(args.port2)
    if not args.version1 == dflt("version1"):
        cmd += " -v1 " + str(args.version1)
    if not args.version2 == dflt("version2"):
        cmd += " -v2 " + str(args.version2)
    if args.key:
        cmd += " -k " + args.key
    start = day.date()
    end = (day + datetime.timedelta(1)).date()
    cmd += " %s %s %s %s" % (args.source, start, end, channel['v3id'])
    return cmd

platform = args.platform
if platform:
    print "identifying channels for platform %s" % platform
    channels = get_platform_channels(platform)
else:
    print "identifying channels %s" % ', '.join(args.channel)
    channels = [get_channel(cid) for cid in args.channel]

atlas1 = Atlas(args.host1, args.port1, args.version1, args.key)
atlas2 = Atlas(args.host2, args.port2, args.version2, args.key)

source = args.source
start = args.start
end = args.end

if (end - start <= datetime.timedelta(1) and platform == None):
    channel = channels[0]
    print "comparing schedules on '%s' between %s and %s" % ("/".join(channel.values()), args.start, args.end)
    req1, schedule1 = atlas1.get_schedule(source, channel, start, end)
    req2, schedule2 = atlas2.get_schedule(source, channel, start, end)
    print req1
    print req2
    table, difference = compare(schedule1, schedule2)
    print tabulate(table, headers="firstrow")
else:
    for channel in channels:
        print " / ".join(channel.values())
        for day in days(start, end):
            s = day
            e = day+datetime.timedelta(1)
            sys.stdout.write("\t%s: " % day.date())
            sys.stdout.flush()
            req1, schedule1 = atlas1.get_schedule(source, channel, s, e)
            req2, schedule2 = atlas2.get_schedule(source, channel, s, e)
            table, difference = compare(schedule1, schedule2)
            if difference:
                print compare_params(channel, day)
            else:
                print "no differences"
