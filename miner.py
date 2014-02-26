#! /usr/bin/python

import collections
import asyncore
import copy
import os
import traceback
import random
import socket
import threading

import time
import json
import hashlib
import struct
import Queue
import binascii
import asynchat
import sys

VERSION = '1.4'
BASE_DIFFICULTY = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
HASHES_NUM = 0x800000
HASHES_SIZE = HASHES_NUM * 8 * 8
KEYS_NUM = HASHES_NUM * 8
KEYHASH_CAPACITY = KEYS_NUM * 2

KEYHASH_SIZE = KEYHASH_CAPACITY * 4
OUTPUT_SIZE = 8 * 0x100

def flipendian32(inmsg):
    return reduce(lambda x, y: x + inmsg[y:y+4][::-1], range(0, len(inmsg), 4), '')

def dblsha(data):
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


class Handler(asynchat.async_chat):
    def __init__(self, nsocket, map_, parent):
        asynchat.async_chat.__init__(self, nsocket, map_)
        self.parent = parent
        self.data = ''
        self.set_terminator('\n')

    def handle_close(self):
        self.close()
        self.parent.handler = None
        self.parent.socket = None

    def handle_error(self):
        import traceback
        traceback.print_exc()
        self.parent.stop()

    def collect_incoming_data(self, data):
        self.data += data

    def found_terminator(self):
        message = json.loads(self.data)
        self.parent.handle_message(message)
        self.data = ''


class StratumClient():
    def __init__(self, switch, host, port, st_username, st_password):
        self.host = host
        self.port = port
        self.username = st_username
        self.password = st_password
        self.handler = None
        self.socket = None
        self.channel_map = {}
        self.subscribed = False
        self.authorized = None
        self.submits = {}
        self.last_submits_cleanup = time.time()
        self.server_difficulty = BASE_DIFFICULTY
        self.jobs = {}
        self.current_job = None
        self.extranonce = ''
        self.extranonce2_size = 4
        self.send_lock = threading.Lock()
        self.switch = switch
        self.should_stop = False

    def run(self):
        print time.asctime(), "Starting stratum client to %s:%s@%s:%d" % (self.username, self.password, self.host, self.port)
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))

            self.handler = Handler(self.socket, self.channel_map, self)
            thread = threading.Thread(target=self.asyncore_thread)
            thread.daemon = True
            thread.start()

            if not self.subscribe():
                print time.asctime(), 'Failed to subscribe'
                self.stop()
                return
            elif not self.authorize():
                print time.asctime(), 'Failed to authorize'
                self.stop()
                return
        except socket.error:
            print time.asctime(), "Cound not start stratum client"
            self.stop()
            return

        while True:
            if self.should_stop:
                return
            result, bda, bdb = self.switch.submit_queue.get(block=True)
            self.submit(result, bda, bdb)

    def asyncore_thread(self):
        asyncore.loop(map=self.channel_map)
        print time.asctime(), "Stratum connection lost"
        self.stop()

    def stop(self):
        self.should_stop = True
        if self.handler:
            self.handler.close()

    def handle_message(self, message):
        if 'method' in message:
            if 'mining.notify' == message['method']:
                params = message['params']

                j = {'job_id': params[0], 'prevhash': params[1], 'coinbase1': params[2], 'coinbase2': params[3],
                     'merkle_branch': params[4], 'version': params[5], 'nbits': binascii.unhexlify(params[6]),
                     'ntime': binascii.unhexlify(params[7]), 'extranonce2': binascii.hexlify('1GH!')}

                j['clear_jobs'] = clear_jobs = params[8]
                if clear_jobs:
                    self.jobs.clear()
                cb1 = binascii.unhexlify(j['coinbase1'])

                coinbase = j['coinbase1'] + self.extranonce + j['extranonce2'] + j['coinbase2']
                merkle_root = dblsha(binascii.unhexlify(coinbase))

                for hash_ in j['merkle_branch']:
                    merkle_root = dblsha(merkle_root + binascii.unhexlify(hash_))

                j['block_start'] = ''.join([binascii.unhexlify(j['version']),
                                           binascii.unhexlify(j['prevhash']),
                                           merkle_root])
                j['time'] = time.time()
                j['target'] = self.server_difficulty

                self.jobs[j['job_id']] = j
                self.current_job = j

                self.switch.set_new_work(j)

            #mining.get_version
            if message['method'] == 'mining.get_version':
                self.send_message({"error": None, "id": message['id'], "result": '1GH PTS miner %s' % VERSION})

            #mining.set_difficulty
            elif message['method'] == 'mining.set_difficulty':
                print time.asctime(), "Setting new difficulty: %6.10f" % message['params'][0]
                self.server_difficulty = BASE_DIFFICULTY / message['params'][0]

        #responses to server API requests
        elif 'result' in message:

            #response to mining.subscribe
            #store extranonce and extranonce2_size
            if message['id'] == 's':
                self.extranonce = message['result'][1]
                self.extranonce2_size = message['result'][2]
                self.subscribed = True

            #check if this is submit confirmation (message id should be in submits dictionary)
            #cleanup if necessary
            elif message['id'] in self.submits:
                miner = self.submits[message['id']][0]
                if message['result']:
                    print time.asctime(), "Share accepted"
                    miner.accepted += 1
                else:
                    print time.asctime(), "Share rejected: %d %s" % (message['error'][0], message['error'][1])
                    miner.rejected += 1
                del self.submits[message['id']]
                if time.time() - self.last_submits_cleanup > 3600:
                    now = time.time()
                    for key, value in self.submits.items():
                        if now - value[1] > 3600:
                            del self.submits[key]
                    self.last_submits_cleanup = now

            #response to mining.authorize
            elif message['id'] == self.username:
                if not message['result']:
                    print('authorization failed with %s:%s@%s:%d', (self.username, self.password, self.host, self.port))
                    self.authorized = False
                else:
                    self.authorized = True

    def subscribe(self):
        self.send_message({'id': 's', 'method': 'mining.subscribe', 'params': []})
        for i in xrange(10):
            time.sleep(1)
            if self.subscribed: break
        return self.subscribed

    def authorize(self):
        self.send_message({'id': self.username, 'method': 'mining.authorize', 'params': [self.username, self.password]})
        for i in xrange(10):
            time.sleep(1)
            if self.authorized is not None: break
        return self.authorized

    def submit(self, result, bda, bdb):
        job_id = result['job_id']
        if not job_id in self.jobs:
            return True
        extranonce2 = result['extranonce2']
        ntime = result['ntime'].encode('hex') # struct.pack('<I', long(result['time'])).encode('hex')
        hex_nonce = struct.pack('<I', long(result['nonce'])).encode('hex')
        birthday_a = struct.pack('<I', bda).encode('hex')
        birthday_b = struct.pack('<I', bdb).encode('hex')
        id_ = job_id + hex_nonce + birthday_a + birthday_b
        self.submits[id_] = (result['miner'], time.time())
        return self.send_message({'params': [self.username, job_id, extranonce2, ntime, hex_nonce, birthday_a, birthday_b],
                                  'id': id_,
                                  'method': u'mining.submit'})

    def send_message(self, message):
        with self.send_lock:
            data = json.dumps(message) + '\n'
            # noinspection PyBroadException
            try:
                if not self.handler:
                    return False
                while data:
                    sent = self.handler.send(data)
                    data = data[sent:]
                return True
            except AttributeError:
                self.stop()
            except Exception:
                print time.asctime(), traceback.format_exc()
                self.stop()


class ConnectionManager(threading.Thread):
    def __init__(self, switch, cm_servers, cm_username, cm_password):
        super(ConnectionManager, self).__init__()
        self.setDaemon(True)
        self.switch = switch
        self.servers = cm_servers
        self.username = cm_username
        self.password = cm_password
        self.server_idx = random.randint(0, len(cm_servers) - 1)

    def get_next_client(self):
        self.server_idx += 1
        self.server_idx %= len(self.servers)
        return StratumClient(self.switch,
                             self.servers[self.server_idx][0],
                             self.servers[self.server_idx][1],
                             self.username,
                             self.password)

    def run(self):
        while True:
            self.get_next_client().run()
            self.switch.set_new_work(None)
            time.sleep(1.0)


class Submitter(threading.Thread):
    def __init__(self, switch):
        super(Submitter, self).__init__()
        self.setDaemon(True)
        self.switch = switch

    def run(self):
        while True:
            result = self.switch.result_queue.get(True)
            output = result['output']
            numpairs = struct.unpack('<Q', output[-8:])[0]
            colls = set()
            for p in xrange(numpairs):
                p = struct.unpack('<II', output[p * 8:p * 8 + 8])
                colls.add((p[0] % 0x100000000, p[1]))
                colls.add((p[1] % 0x100000000, p[0]))
            if len(colls) == 0:
                continue
            result['miner'].colls += len(colls)
            for coll in colls:
                data = result['block_head'] + struct.pack('<II', coll[0], coll[1])
                check_hash = int(binascii.hexlify(dblsha(data)[::-1]), 16)
                if check_hash > result['target']:
                    continue
                self.switch.submit_queue.put((result, coll[0], coll[1]))


class StatsPrinter(threading.Thread):
    def __init__(self, miners):
        super(StatsPrinter, self).__init__()
        self.setDaemon(True)
        self.miners = miners
        self.coll_counts = {m: collections.deque() for m in miners}
        self.hash_counts = {m: collections.deque() for m in miners}
        self.curr_colls = {m: 0 for m in miners}
        self.curr_hashes = {m: 0 for m in miners}
        self.start_time = 0

    def run(self):
        self.start_time = time.time()
        while True:
            time.sleep(15)
            runtime = time.time() - self.start_time
            total_cpm5m = total_hpm5m = total_cpm = total_hpm = total_a = total_r = 0
            for m in self.miners:
                miner = self.miners[m]
                name = "%s %s" % (miner.idstr, miner.device_name)
                colls_step = miner.colls - self.curr_colls[m]
                self.curr_colls[m] = miner.colls
                hashes_step = miner.hashes - self.curr_hashes[m]
                self.curr_hashes[m] = miner.hashes
                self.coll_counts[m].appendleft(colls_step)
                if len(self.coll_counts[m]) > 60:
                    self.coll_counts[m].pop()
                self.hash_counts[m].appendleft(hashes_step)
                if len(self.hash_counts[m]) > 60:
                    self.hash_counts[m].pop()
                cpm5m = reduce(lambda x, y: x + y, self.coll_counts[m], 0) / float(len(self.coll_counts[m])) * 4.0
                total_cpm5m += cpm5m
                hpm5m = reduce(lambda x, y: x + y, self.hash_counts[m], 0) / float(len(self.hash_counts[m])) * 4.0
                total_hpm5m += hpm5m
                cpm = miner.colls / runtime * 60.0
                total_cpm += cpm
                hpm = miner.hashes / runtime * 60.0
                total_hpm += hpm
                a = miner.accepted
                total_a += a
                r = miner.rejected
                total_r += r
                rpc = float(r) / (a + r) * 100.0 if a + r != 0 else 0.0
                print "Worker %s CPM (15m) %.2f HPM (15m) %.2f | CPM %.2f HPM %.2f | A/R %d/%d (%.2f%% rej)" % \
                      (name, cpm5m, hpm5m, cpm, hpm, a, r, rpc)
            total_rpc = float(total_r) / (total_a + total_r) * 100.0 if total_a + total_r != 0 else 0.0
            print "Total CPM (15m) %.2f HPM (15m) %.2f | CPM %.2f HPM %.2f | A/R %d/%d (%.2f%% rej)" % \
                  (total_cpm5m, total_hpm5m, total_cpm, total_hpm, total_a, total_r, total_rpc)


class Switch(threading.Thread):
    def __init__(self, switch_servers, cm_username, cm_pasword, miner_devs):
        super(Switch, self).__init__()
        self.setDaemon(True)

        self.work_cond = threading.Condition()
        self.work = None

        self.result_queue = Queue.Queue(len(miner_devs) * 4)
        self.submit_queue = Queue.Queue(4)

        self.miners = collections.OrderedDict()
        for (platform, dev_id, ws) in miner_devs:
            # noinspection PyBroadException
            try:
                self.miners['%d-%d' % (platform, dev_id)] = Miner(platform, dev_id, ws, self)
            except:
                print time.asctime(), "Worker %d-%d failed to initialize: %s" % (platform, dev_id, traceback.format_exc())

        self.miner_queue = Queue.Queue(len(self.miners))
        for miner in self.miners.values():
            miner.start()

        self.nonce = 0

        Submitter(self).start()

        ConnectionManager(self, switch_servers, cm_username, cm_pasword).start()
        StatsPrinter(self.miners).start()

    def gen_miner_job(self):
        w = copy.deepcopy(self.work)
        timediff = int(time.time()) - int(w['time'])
        ntime = struct.unpack('<I', w['ntime'])[0]
        ntime += timediff
        w['ntime'] = struct.pack('<I', ntime)
        self.nonce += 1
        w['nonce'] = self.nonce
        w['block_head'] = w['block_start'] + w['ntime'] + w['nbits'] + struct.pack('<I', self.nonce)
        w['midstate'] = dblsha(w['block_head'])
        return w

    def set_new_work(self, work):
        with self.work_cond:
            self.work = work
            if work is not None:
                if work['clear_jobs']:
                    self.nonce = 0
                self.work_cond.notify()
            else:
                self.nonce = 0

    def run(self):
        while True:
            with self.work_cond:
                if self.work is None:
                    self.work_cond.wait()
                self.miner_queue.put(self.gen_miner_job())


class Miner(threading.Thread):
    def __init__(self, platform, dev_id, ws, switch):
        super(Miner, self).__init__()
        self.setDaemon(True)
        self.id = dev_id
        self.idstr = "%d-%d" % (platform, dev_id)
        self.switch = switch
        self.hasher = PTSHasher(platform, dev_id, ws)
        self.device_name = self.hasher.device_name
        self.start_time = -1
        self.hashes = 0
        self.colls = 0
        self.accepted = 0
        self.rejected = 0

    def run(self):
        while True:
            work = self.switch.miner_queue.get(block=True)
            if self.start_time == -1:
                self.start_time = time.time()
            if not 'midstate' in work:
                continue
            work['miner'] = self
            work['output'] = self.hasher.process_midstate(work['midstate'])
            self.hashes += 1
            self.switch.result_queue.put(work, block=True)


class PTSHasher(object):
    def __init__(self, platform_num, device_num, ws):
        self.platform = cl.get_platforms()[platform_num]
        self.device = self.platform.get_devices()[device_num]
        self.device_mem = self.device.global_mem_size
        self.device_name = self.device.name

        if self.device_mem > 1024 * 1024 * 1024:
            self.keyhash_kapacity = KEYS_NUM * 2
        else:
            self.keyhash_kapacity = KEYS_NUM
        self.name = "PTSHasher:%d-%d" % (platform_num, device_num)
        self.ctx = cl.Context(devices=(self.device,))
        opts = '-DKEYHASH_CAPACITY=%d' % self.keyhash_kapacity
        initext = "%s initialized on platform %s, device %s, vendor: %s, mem: %d"
        with open('pts.cl') as clfile:
            cltext = clfile.read()
        self.prog = cl.Program(self.ctx, cltext).build(opts)
        pp = initext % (self.name, self.platform.name, self.device.name, self.device.vendor, self.device_mem)
        if ws != 0:
            pp += ', forced worksize %d' % ws
        print pp

        self.queue = cl.CommandQueue(self.ctx)

        self.sha512_fill = self.prog.pts_sha512_fill
        self.ksearch = self.prog.search_ht

        if ws != 0:
            self.sha512_fill_ws = self.ksearch_ws = ws
        else:
            self.sha512_fill_ws = self.sha512_fill.get_work_group_info(cl.kernel_work_group_info.WORK_GROUP_SIZE, self.device)
            self.ksearch_ws = self.ksearch.get_work_group_info(cl.kernel_work_group_info.WORK_GROUP_SIZE, self.device)

        self.hashes_buf = cl.Buffer(self.ctx, 0, size=HASHES_SIZE)
        self.keyhash_buf = cl.Buffer(self.ctx, 0, size=self.keyhash_kapacity * 4)
        self.output_buf = cl.Buffer(self.ctx, 0, size=OUTPUT_SIZE)

    def process_midstate(self, midstate):
        msg = flipendian32(midstate)

        for i in xrange(8):
            self.sha512_fill.set_arg(i, msg[i * 4:i * 4 + 4])
        self.sha512_fill.set_arg(8, self.hashes_buf)
        self.sha512_fill.set_arg(9, self.keyhash_buf)
        cl.enqueue_barrier(self.queue)
        cl.enqueue_nd_range_kernel(self.queue, self.sha512_fill, (HASHES_NUM,), (self.sha512_fill_ws,))
        self.queue.finish()

        output = bytearray(OUTPUT_SIZE)
        cl.enqueue_write_buffer(self.queue, self.output_buf, output)
        cl.enqueue_barrier(self.queue)

        self.ksearch.set_arg(0, self.hashes_buf)
        self.ksearch.set_arg(1, self.keyhash_buf)
        self.ksearch.set_arg(2, self.output_buf)
        cl.enqueue_nd_range_kernel(self.queue, self.ksearch, (KEYS_NUM,), (self.ksearch_ws,))
        cl.enqueue_barrier(self.queue)
        cl.enqueue_read_buffer(self.queue, self.output_buf, output)
        self.queue.finish()
        return str(output)


def find_amd_nvidia_platforms():
    rv = []
    plats = cl.get_platforms()
    for i in xrange(len(plats)):
        if 'amd accelerated' in plats[i].name.lower() or 'nvidia' in plats[i].name.lower():
            rv.append(i)
    return rv


def filter_devices(dset, platforms):
    rv = set()
    for p in platforms:
        devs = cl.get_platforms()[p].get_devices()
        if len(dset) == 0:
            dset.update([(p, i, 0) for i in range(len(devs))])
        for (p, d, ws) in dset:
            if device_suitable(p, devs[d], d):
                rv.add((p, d, ws))
    return rv


def device_suitable(platform, device, num):
    if device.type != 4:
        print "Skipping device %d-%d (%s): not a GPU" % (platform, num, device.name)
        return False
    if 'hd graphics' in device.name.lower():
        print "Skipping device %d-%d (%s): not a discrete GPU" % (platform, num, device.name)
        return False
    if device.global_mem_size < 1024 * 1024 * 896:
        print "Skipping device %d-%d (%s): GLOBAL_MEM_SIZE %d < required %d" % (platform, num, device.name, device.global_mem_size, 1024 * 1024 * 896)
        return False
    return True


def run():
    os.environ['PYOPENCL_NO_CACHE'] = '1'
    os.environ['GPU_MAX_ALLOC_PERCENT'] = '100'
    os.environ['GPU_MAX_HEAP_SIZE'] = '100'
    os.environ['GPU_USE_SYNC_OBJECTS'] = '1'
    if 'AMD_OCL_BUILD_OPTIONS_APPEND' in os.environ:
        del os.environ['AMD_OCL_BUILD_OPTIONS_APPEND']
    if 'AMD_OCL_BUILD_OPTIONS' in os.environ:
        del os.environ['AMD_OCL_BUILD_OPTIONS']
    if "DISPLAY" not in os.environ:
        os.environ["DISPLAY"] = ":0"

    print "1GH PTS miner v%s" % VERSION

    ocl_ok = False
    # noinspection PyGlobalUndefined
    global cl
    # noinspection PyBroadException
    try:
        import pyopencl as cl
        ocl_ok = True
    except:
        print "Error: could not initialize OpenCL"
        print

    platforms = find_amd_nvidia_platforms()

    if ocl_ok and len(platforms) == 0:
        ocl_ok = False
        print "Error: AMD or Nvidia OpenCL platform not detected in the system"
        print

    sample = 'PeanutsEeRvi8hFELjYf4dhKXtQ6kZCF2Q'
    if not ocl_ok or len(sys.argv) < 2:
        print "PTS GPU miner by reorder"
        print "Usage: %s YOUR-PTS-ADDRESS <platform1-device1> <platform2-device2> ..." % sys.argv[0]
        print "    <platxormX-deviceX> is OpenCL platform-device number (0-0, 1-0, 1-1 etc)"
        print "    By default, all suitable GPUs in the system will be used"
        print
        print "    You can try overriding default workgroup size by adding comma and desired ws size to device."
        print "    Valid values are 64, 128, 256, 512, 1024, 2048 (not all of them may work on your card!)"
        print "    Example: 0-0,256 0-1,64"
        print
        print "    Example using all available devices: %s %s " % (sys.argv[0], sample)
        print "    Example using only devices 0 and 1 of platform 0, device 1 worksize forced to 64: %s %s 0-0 0-1,64" % (sys.argv[0], sample)
        sys.exit(0)

    devs = set()
    if len(sys.argv) > 2:
        for i in xrange(len(sys.argv) - 2):
            try:
                ws = 0
                a = sys.argv[i + 2]
                if ',' in a:
                    a, ws = a.split(',')
                    try:
                        ws = int(ws)
                    except ValueError:
                        pass
                    if ws not in (64, 128, 256, 512, 1024, 2048):
                        ws = 0
                if '-' in a:
                    p, d = a.split('-')
                    devs.add((int(p), int(d), ws))
                else:
                    devs.add((0, int(a), ws))
            except ValueError:
                pass

    devices = filter_devices(devs, platforms)

    if len(devices) == 0:
        print "No suitable devices found. Only cards with 1G+ available VRAM supported."
        sys.exit(0)

    host = 'ptspool.1gh.com'
    servers = [(host, 13333), (host, 13334), (host, 13335), (host, 13336)]
    username = sys.argv[1]
    password = 'x'

    sw = Switch(servers, username, password, devices)

    print time.asctime(), "Miner Starts"
    sw.start()
    try:
        while True:
            raw_input()
    except KeyboardInterrupt:
        pass
    print time.asctime(), "Miner Stops"

if __name__ == '__main__':
    run()

