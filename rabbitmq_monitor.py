#!/usr/bin/env python

import json, urllib, socket
import commands
import base64
import sys
if sys.version_info[0] == 2:  # Python version
    from ConfigParser import ConfigParser, NoSectionError
    import httplib
    import urlparse
    from urllib import quote_plus
    def b64(s):
        return base64.b64encode(s)
else:
    from configparser import ConfigParser, NoSectionError
    import http.client as httplib
    import urllib.parse as urlparse
    from urllib.parse import quote_plus
    def b64(s):
        return base64.b64encode(s.encode('utf-8')).decode('utf-8')

#import monitor_constants as constants

rmqHost = '127.0.0.1'
rmqPort = 15672
rmqPath = '/api/'
#rmqName = 'admin'
#rmqPswd = 'Access4Rabbit'
rmqName = 'guest'
rmqPswd = 'guest'
NORM_STATE="OK"
IDLE_STATE="IDLE"
FAIL_STATE="NOK"
UNAC_STATE="DOWN"
MAX_HANGING_MSG = 50
#Access to the RabbitMQ Management HTTP API, 
# execute command and keep the result in the "result" variable
#
#@param CMD {STRING} - command that should be executed
#@return error code
#
def access_rabbitmq(command):
    ret = None
    err = 0
    auth = (rmqName + ":" + rmqPswd)
    headers = {"Authorization": "Basic " + b64(auth)}
    conn = httplib.HTTPConnection(rmqHost, rmqPort)
    try:
    conn.request('GET', rmqPath+command, headers=headers)
    resp = conn.getresponse()
    content = resp.read()
    conn.close()
        ret = content
        err = resp.status        
    except socket.error as e:
        ret = 'Could not connect: ' + str(e)
        err = 403
    return (err, ret)
    
#  Format a timestamp into the form 'x day hh:mm:ss'
#  @param TIMESTAMP {NUMBER} the timestamp in sec
def formatTimestamp(time):
    sec=time%60
    mins=(time/60)%60
    hr=(time/3600)%24
    da=time/86400
    s="%02u.%02u.%02u" % (hr, mins, sec)
    if da > 0:
        s=str(da)+'-'+str(s) 
    return s

def addAdditionalData(data, res_array): 
    if isinstance(data, basestring) and isinstance(res_array, list):
        details={'details':data}
        details = urllib.quote(json.dumps(details), '{}')
        res_array.append(details)

def convert(input):
    if isinstance(input, dict):
        return dict([(convert(key), convert(value)) for key, value in input.iteritems()])
    elif isinstance(input, list):
        return [convert(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input
    
errors=0
ad_res = []
MSG = {}
# get nodes info
#print('*** nodes ***')
try:
    (err, result) = access_rabbitmq('nodes')
#    print('==> nodes: '+str(result))
    if result != None:
        if err >= 400:
            raise Exception(result)
        nodes = json.loads(result)
        run=nodes[0]['running']
        pid=nodes[0]['os_pid']
        up=nodes[0]['uptime']
        upt=formatTimestamp(up / 1000)
        ofd=nodes[0]['fd_used']
        lfd=nodes[0]['fd_total']
        ofdp='%.2f' % (100.0*ofd/lfd)
        osd=nodes[0]['sockets_used']
        lsd=nodes[0]['sockets_total']
        osdp='%.2f' % (100.0*osd/lsd)
        proc=nodes[0]['proc_used']
        lproc=nodes[0]['proc_total']
        procp='%.2f' % (100.0*proc/lproc)
        #mem=nodes[0].mem_used
        #mem_mb=$(echo "scale=1;($mem/1024/1024)" | bc )
        #lmem=nodes[0].mem_limit
        #lmem_mb=$(echo "scale=1;($lmem/1024/1024)" | bc )
        #memp=$(echo "scale=1;(100.0*$mem/$lmem)" | bc )
        #dfree=nodes[0].disk_free
        #dfree_mb=$(echo "scale=1;($dfree/1024/1024)" | bc )
        #ldfree=nodes[0].disk_free_limit
        #ldfree_mb=$(echo "scale=1;($ldfree/1024/1024)" | bc )
        #dfreep=$(echo "scale=1;(100.0*$ldfree/$dfree)" | bc )
        cm = commands.getoutput('ps -p'+str(pid)+' -o %cpu,%mem  | grep -v % ') 
        rr = cm.strip().replace('  ',' ').split(' ', 2);
        cpu_pr=rr[0]
        mem_pr=rr[1]
#        print('cpu=%s, mem=%s' % (cpu_pr, mem_pr)) 
            
        if float(ofdp) > 90 or float(osdp) > 90:
            MSG[errors]="WARN - Too many open files descriptors"
            errors+=1
          
        if float(procp) > 90:
            MSG[errors]="WARN - Too many Erlang processes used ($proc / $lproc)"
            errors+=1        
          
        if float(mem_pr) > 95:
            MSG[errors]="WARN - Memory usage is critically big"
            errors+=1
           
        if float(cpu_pr) > 95:
            MSG[errors]="WARN - CPU usage is critically big"
            errors+=1         
except Exception as e:
    addAdditionalData('Cannot access to the rabbitmq engine - '+str(e), ad_res)
    status = UNAC_STATE
    param='status:'+str(status)+';additionalResults:'+str(ad_res).replace("'","")
    print param
    exit()
# # get overview info    
# access_rabbitmq "overview"
#print('*** overview ***')
try:
    (err, result) = access_rabbitmq('overview')
    if result != None:
        if err >= 400:
            raise Exception(result)
        overview = json.loads(result)
#        print(overview)
        rabbitmq_version = overview['rabbitmq_version'] if overview.has_key('rabbitmq_version') else '?'
        message_stats = overview['message_stats'] if overview.has_key('message_stats') else {} 
        v=message_stats['publish_details'] if message_stats.has_key('publish_details') else {} 
        v=v['rate'] if 'rate' in v else '0'
        pub_rate='%.2f' % float(v)
        v=message_stats['deliver_details'] if message_stats.has_key('deliver_details') else {}
        v=v['rate'] if 'rate' in v else '0'
        delivery_rate='%.2f' % float(v)
        v=message_stats['ack_details'] if message_stats.has_key('ack_details') else {}
        v=v['rate'] if 'rate' in v else '0'
        ack_rate='%.2f' % float(v)
        v=message_stats['deliver_no_ack_details'] if message_stats.has_key('deliver_no_ack_details') else {}
        v=v['rate'] if 'rate' in v else '0'
        deliver_no_ack_rate='%.2f' % float(v)
        v=message_stats['deliver_get_details'] if message_stats.has_key('deliver_get_details') else {}
        v=v['rate'] if 'rate' in v else '0'
        deliver_get_rate='%.2f' % float(v)
        
        queue_totals = overview['queue_totals'] if overview.has_key('queue_totals') else {} 
        msg=queue_totals['messages'] if queue_totals.has_key('messages') else '0'
        msg_ready=queue_totals['messages_ready'] if queue_totals.has_key('messages_ready') else '0'
        msg_unack=queue_totals['messages_unacknowledged'] if queue_totals.has_key('messages_unacknowledged') else '0'
#        msg_in_queues=int(msg) + int(msg_ready) + int(msg_unack)
        msg_in_queues=int(msg_ready) + int(msg_unack)
          
        if msg_in_queues > MAX_HANGING_MSG:
            MSG[errors]="WARN - some numbers of messages are left in queue"
            errors+=1

except Exception as e:
    addAdditionalData('Cannot access to the rabbitmq engine - '+str(e), ad_res)
    status = UNAC_STATE
    param='status:'+str(status)+';additionalResults:'+str(ad_res).replace("'","")
    print param
    exit()

# # get connections info
# access_rabbitmq "connections"
#print('*** connections ***')
conn=0
r_rate=0
w_rate=0
timeout=0
client=''
try:
    (err, result) = access_rabbitmq('connections')
    if result != None:
        if err >= 400:
            raise Exception(result)
        connections = json.loads(result)
        conn = len(connections)
        if conn > 0:
            for i in range(conn):
                l=len(connections[i])
                if l > 0: 
                    v=connections[i]['recv_oct_details'] if connections[i].has_key('recv_oct_details') else {}
                    r_rate=(v['rate'] if 'rate' in v else 0) + r_rate
                    v=connections[i]['send_oct_details'] if connections[i].has_key('send_oct_details') else {}
                    w_rate=(v['rate']  if 'rate' in v else 0) + w_rate
                    timeout=(connections[i]['timeout'] if connections[i].has_key('timeout') else 0) + timeout 
                    v=connections[i]['client_properties'] if connections[i].has_key('client_properties') else {}
                    client_=(v['product'] if 'product' in v else '')+'_'+(v['version'] if 'version' in v else '')+' '+(v['platform'] if 'platform' in v else '')
                    if len(client) <= 0:
                        client = client_
                    elif client.find(client_) < 0:
                        client+=';'+client_
        else:
            client="No any client establish connections yet" 
        recv_rate='%.2f' % (r_rate/1024.0)
        sent_rate='%.2f' % (w_rate/1024.0)
         
#        print('r_rate=%d, w_rate=%d, timeout=%d' % (r_rate, w_rate, timeout))
#        print('recv_rate=%s, sent_rate=%s' % (recv_rate, sent_rate))
#        print(client)
except Exception as e:
    addAdditionalData('Cannot access to the rabbitmq engine - '+str(e), ad_res)
    status = UNAC_STATE
    param='status:'+str(status)+';additionalResults:'+str(ad_res).replace("'","")
    print param
    exit()

# # get queue info
# access_rabbitmq "queues"
#print('*** queues ***')
queue_count = 0
consumers_count = 0
queue_without_consumer = []
queue_with_messages = {}
queue=''
try:
    (err, result) = access_rabbitmq('queues')
    if result != None:
        if err >= 400:
            raise Exception(result)
        queues = json.loads(result)
        queue_count = len(queues)
        if queue_count > 0:
            for i in range(queue_count):
                message_stats=queues[i]['message_stats'] if queues[i].has_key('message_stats') else {}
                consumers_count += queues[i]['consumers']
                if queues[i]['consumers'] == 0 :
                    queue_without_consumer.append(queues[i]['name'])

                if len(message_stats) > 0:
                    v=message_stats['publish_details'] if message_stats.has_key('publish_details') else {}
                    r_rate=v['rate'] if 'rate' in v else 0
                    v=message_stats['deliver_get_details'] if message_stats.has_key('deliver_get_details') else {}
                    w_rate=v['rate'] if 'rate' in v else 0
                else:
                    r_rate=0 ; w_rate=0
                queue_="'"+queues[i]['name']+"' ("+ ('%d' % (queues[i]['consumers']))+') pub: '+('%.2f' % (r_rate))+' msg/s; get: '+('%.2f' % (w_rate))+' msg/s'
                if len(queue) <= 0:
                    queue=queue_
                elif queue.find(queue_) < 0:
                    queue+='; '+queue_
                msg = queues[i]['messages_ready'] + queues[i]['messages_unacknowledged']
                if msg > MAX_HANGING_MSG :
                    queue_with_messages[queues[i]['name']] = msg
        else:
            queue="No any queues are created yet" 
except Exception as e:
    addAdditionalData('Cannot access to the rabbitmq engine - '+str(e), ad_res)
    status = UNAC_STATE
    param='status:'+str(status)+';additionalResults:'+str(ad_res).replace("'","")
    print param
    exit()
#print(queue)

if errors > 0:
    status = FAIL_STATE
    addAdditionalData('Problems in RabbitMQ' + str(rabbitmq_version) + ' (' + str(pid) + ') ' + FAIL_STATE, ad_res)
    for CNT in range(errors):
        addAdditionalData(str(MSG[CNT]), ad_res)
    if len(queue_with_messages) > 0 :
        addAdditionalData('Queues with hanging messages: ' + str(convert(queue_with_messages)), ad_res)
    if len(queue_without_consumer) > 0 :
        addAdditionalData('queues without consumers: ' + str(len(queue_without_consumer)), ad_res)
elif  (conn <= 0) or (queue_count <= 0):
    status=IDLE_STATE
    addAdditionalData('RabbitMQ' + str(rabbitmq_version) + ' (' + str(pid) + ') ' + IDLE_STATE, ad_res)   
else:
    status = NORM_STATE
    addAdditionalData('RabbitMQ' + str(rabbitmq_version) + ' (' + str(pid) + ') ' + NORM_STATE, ad_res)
    addAdditionalData(str(conn)+' connections are established', ad_res)
    addAdditionalData(str(queue_count)+' queues are created', ad_res)

param='status:'+str(status)+';osd:'+str(osdp)+';ofd:'+str(ofdp)+';cpu_usage:'+str(cpu_pr)+';mem_usage:'+str(mem_pr) \
    +';recv_mps:'+str(deliver_get_rate)+';sent_mps:'+str(pub_rate)+';msg_queue:'+str(msg_in_queues) \
    +';consumers:'+str(consumers_count)+';recv_kbps:'+str(recv_rate)+';sent_kbps:'+str(sent_rate)+';uptime:'+str(upt) \
    +';additionalResults:'+str(ad_res).replace("'","")

print param


