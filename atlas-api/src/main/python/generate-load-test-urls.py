#!/usr/bin/env python

# ./generate-load-test-urls.py  --number-of-urls=100 --atlas-url=stage.atlas.metabroadcast.com --target-host=host-to-test --api-key=api-key --source=pressassociation.com --num-channels-source=100 --num-channels=10 --platform=hkyn --start-date=2015-02-01 --end-date=2015-02-10
import argparse
import datetime
import dateutil.parser
import httplib
import json
import random

arg_parser = argparse.ArgumentParser(description='Generate URL for load testing')

arg_parser.add_argument('--number-of-urls', required=True, dest='n', type=int, metavar='n', help='Number of url to generate')
arg_parser.add_argument('--atlas-url', required=True, dest='atlas_url', metavar='atlas_url', help='Atlas host')
arg_parser.add_argument('--target-host', required=True, dest='target_host', metavar='target_host', help='Target host')
arg_parser.add_argument('--api-key', required=True, dest='api_key', metavar='api_key', help='Atlas API key')
arg_parser.add_argument('--num-channels-source', required=True, type=int, dest='num_channels_source', metavar='num_channels_source', help='Number of channels to choose from')
arg_parser.add_argument('--num-channels', required=True, type=int, dest='num_channels', metavar='num_channels', help='Number of channels to use in request')
arg_parser.add_argument('--platform', required=True, dest='platform', metavar='platform', help='platform')
arg_parser.add_argument('--source', required=True, metavar='source', help='source of the schedules to bootstrap')
arg_parser.add_argument('--start-date', required=True, metavar='start_date', help='Start date')
arg_parser.add_argument('--end-date', required=True, metavar='end_date', help='Start date')


args = arg_parser.parse_args()

args.start_date = dateutil.parser.parse(args.start_date)
args.end_date = dateutil.parser.parse(args.end_date)


class Atlas:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def get(self, resource):
        conn = httplib.HTTPConnection(self.host, self.port)
        request = "GET http://%s:%s%s" % (self.host, self.port, resource)
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
            response = json.loads(body)
        except Exception as e:
            print "couldn't decode response to %s: %s" % (request, e)
            print body
            sys.exit()
        return (request, response)
    

atlas = Atlas(args.atlas_url, 80)

req, platform = atlas.get("/4/channel_groups/%s.json?key=%s&annotations=channels" % (args.platform, args.api_key))
def get_days(start,end):
    ds = []
    cur = start
    while cur <= end:
        ds.append(cur)
        cur = cur + datetime.timedelta(1)
    return ds


channels = map((lambda c: c['channel']['id']),platform['channel_group']['channels'][:args.num_channels_source])
days = get_days(args.start_date, args.end_date)
for x in range(0, args.n):
    channels_string =  ",".join(random.sample(channels, args.num_channels))
    day = random.choice(days)
    print "/4/schedules.json?id=%s&annotations=channel,content_detail&from=%s&to=%s&key=%s&source=%s" % (
        # args.target_host,
        channels_string,
        day.isoformat(),
        (day + datetime.timedelta(1)).isoformat(),
        args.api_key,
        args.source
    )



