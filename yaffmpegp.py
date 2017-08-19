#!/usr/bin/env python3

import os
import re
import time
import logging
import contextlib
import subprocess

from enum import Enum, unique
from threading import Thread, Semaphore
from queue import Queue, Empty


@unique
class FFMpegStatus(Enum):
    IDLE = 0
    SENDING = 1
    SENT = 2
    COMPRESSING = 3
    COMPRESSED = 4
    DOWNLOADING = 5
    DOWNLOADED = 6
    FINISHED = 7
    BROKEN = 8
    RUNNING = 9
    DRAINING = 10


class RetryException(Exception):
    pass


class FFMpeg(Thread):
    def __init__(self, host, queue=None, qlimit=0, params=None):
        super(FFMpeg, self).__init__(daemon=True)
        
        if not params:
            params = []

        self._host = host
        self._main_queue = queue

        self._send_queue = Queue()
        self._compress_queue = Queue()
        self._download_queue = Queue()
        self._remove_queue = Queue()

        self._send_thread = Thread(target=self.send_file, daemon=True)
        self._compress_thread = Thread(target=self.compress_file, daemon=True)
        self._download_thread = Thread(target=self.download_file, daemon=True)

        self._send_status = FFMpegStatus.IDLE
        self._compress_status = FFMpegStatus.IDLE
        self._download_status = FFMpegStatus.IDLE

        self._send_fname = ""
        self._compress_fname = ""
        self._download_fname = ""

        self.params = params
        self.healthy = True
        self._qlimit = qlimit

        self._processed_files = []
        
        # scp stats
        self.scp_percent = 0
        self.scp_eta = ""

        self._retries = 0
        self._processed = 0
        self._last_contact = time.time()
        self._status = FFMpegStatus.IDLE
        self._fps = 0.0
        self._time = ""

        self._semaphore = Semaphore(2)
        
        #self.check_ffmpeg()

    @property
    def processed_files(self):
        return self._processed_files[:]

    def _unbuffered(self, proc, stream='stdout'):
        stream = getattr(proc, stream)
        with contextlib.closing(stream):
            while True:
                out = []
                last = stream.read(1)
                if type(last) is bytes:
                    last = last.decode()
                # Don't loop forever
                if last == '' and proc.poll() is not None:
                    break
                while last not in ["\n", "\r\n", "\r"]:
                    if last == '' and proc.poll() is not None:
                        break
                    out.append(last)
                    last = stream.read(1)
                    if type(last) is bytes:
                        last = last.decode()
                out = ''.join(out)
                yield out 

    def check_ffmpeg(self):
        check_ffmpeg = subprocess.Popen(
            ["ssh", self._host, "ffmpeg", "-version"],
            stdout=subprocess.PIPE
        )

        for line in self._unbuffered(check_ffmpeg):
            re_version = re.search('ffmpeg version ([0-9\.]*)', line)
            if not re_version:
                continue
            self.ffmpeg_version = re_version.group(1)

        if not hasattr(self, 'ffmpeg_version'):
            raise Exception("FFMpeg initialization for {} failed: can't get version nr".format(self._host))
        print("{} initialized with ffmpeg version {}".format(self._host, self.ffmpeg_version))

        check_ffmpeg.wait()

    def _scp(self, src, dest):
        with open("/dev/null", "w") as null:
            command = ["scp", "-o", "ConnectTimeout=30", "-B", src, dest]
            scp = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=null
            )
        for line in self._unbuffered(scp):
            re_percent = re.search(r"([0-9]*)%.*?([0-9:\-]*) ETA", line)
            if re_percent:
                self.scp_percent = int(re_percent.group(1))
                self.scp_eta = re_percent.group(2)
        if scp.returncode != 0:
            raise RetryException("[{}] scp failed for {}->{} ({})".format(scp.returncode, src, dest, ' '.join(command)))

    def send_file(self):
        while self.healthy:
            fpath = self._send_queue.get()
            if fpath == None:
                self._send_status = FFMpegStatus.FINISHED
                self._compress_queue.put(None)
                return

            self._send_status = FFMpegStatus.SENDING

            _, fname = os.path.split(fpath)
            self._send_fname = fname
            retries = 0
            while True:
                try:
                    self._scp(fpath, "{}:/tmp/{}".format(self._host, fname))
                    break
                except RetryException as e:
                    if retries >= 3:
                        raise e
                    retries += 1

            self._compress_queue.put(fname)
            self._send_queue.task_done()
            self._send_status = FFMpegStatus.SENT

    def compress_file(self):
        while self.healthy:
            fname = self._compress_queue.get()
            if fname == None:
                self._compress_status = FFMpegStatus.FINISHED
                self._download_queue.put(None)
                return
            self._compress_status = FFMpegStatus.COMPRESSING
            self._compress_fname = fname
            command = ["ssh", "-o", "ServerAliveCountMax=3", "-o", "ServerAliveInterval=10", self._host, "ffmpeg", "-stats", "-y", "-i", "/tmp/{}".format(fname)]
            command += self.params
            command += ["/tmp/{}.ts".format(fname)]
            ffmpeg = subprocess.Popen(
                command,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            for line in self._unbuffered(ffmpeg, 'stderr'):
                self._last_contact = time.time()
                if line is None:
                    break
                re_fps = re.search(r"fps=([0-9\.\s]+).*?(time=[0-9:\.]*)", line)
                if re_fps:
                    self._fps = float(re_fps.group(1))
                    self._time = re_fps.group(2)
            ffmpeg.wait()
            if ffmpeg.returncode != 0:
                raise RetryException("FFMpeg compress failed on {} for {}, returncode: {}".format(self._host, fname, ffmpeg.returncode))

            self._download_queue.put(fname)
            self._compress_queue.task_done()
            self._processed += 1
            self._semaphore.release()
            self._compress_status = FFMpegStatus.COMPRESSED

    def download_file(self):
        while self.healthy:
            fname = self._download_queue.get()
            if fname == None:
                self._download_status = FFMpegStatus.FINISHED
                return
            self._download_status = FFMpegStatus.DOWNLOADING
            self._download_fname = fname
            self._scp("{}:/tmp/{}.ts".format(self._host, fname), "./")

            self._download_queue.task_done()
            self._remove_queue.put(fname)
            self._download_status = FFMpegStatus.DOWNLOADED

            # so yeah, .ts is hardcoded for now
            self._processed_files.append(f"./{fname}.ts")

    def remove_files(self):
        command = ["ssh", "-o", "ServerAliveCountMax=3", "-o", "ServerAliveInterval=10"]
        fnames = []
        while not self._remove_queue.empty():
            try:
                fnames.append("/tmp/{}.ts".format(self._remove_queue.get(timeout=1)))
            except Empty:
                continue
        command += fnames
        print("Removing files: {}".format(str(command)))
        with open("/dev/null", "w") as null:
            subprocess.run(
                command,
                stdout=null,
                stderr=null
            )

    @property
    def host(self):
        return self._host.split('@')[-1]

    @property
    def status(self):
        return self._status.name

    @property
    def send_status(self):
        return self._send_status.name

    @property
    def compress_status(self):
        return self._compress_status.name

    @property
    def download_status(self):
        return self._download_status.name

    @property
    def send_queue(self):
        return self._send_queue.qsize()

    @property
    def compress_queue(self):
        return self._compress_queue.qsize()

    @property
    def download_queue(self):
        return self._download_queue.qsize()

    @property
    def stale(self):
        return self._status == FFMpegStatus.COMPRESSING and (time.time() - self._last_contact) > 30

    @property
    def fps(self):
        if self._compress_status == FFMpegStatus.COMPRESSING:
            return self._fps
        return 0.0

    @property
    def finished(self):
        return  self._status == FFMpegStatus.DRAINING and \
                self._send_status == FFMpegStatus.FINISHED and \
                self._compress_status == FFMpegStatus.FINISHED and \
                self._download_status == FFMpegStatus.FINISHED

    @property
    def time(self):
        if self._compress_status == FFMpegStatus.COMPRESSING:
            return self._time
        return ""

    def _send_repr(self):
        if self._send_status == FFMpegStatus.SENDING:
            return "SENDER(%s, %d, ->%s)" % (self.send_status, self.send_queue, self._send_fname)
        else:
            return "SENDER(%s, %d)" % (self.send_status, self.send_queue)

    def _compress_repr(self):
        if self._compress_status == FFMpegStatus.COMPRESSING:
            return "COMPRESS(%s, %d, *%s)" % (self.compress_status, self.compress_queue, self._compress_fname)
        else:
            return "COMPRESS(%s, %d)" % (self.compress_status, self.compress_queue)

    def _download_repr(self):
        if self._download_status == FFMpegStatus.DOWNLOADING:
            return "DOWNLOAD(%s, %d, <-%s)" % (self.download_status, self.download_queue, self._download_fname)
        else:
            return "DOWNLOAD(%s, %d)" % (self.download_status, self.download_queue)

    def _pipe_repr(self):
        return "%s -> %s -> %s" % (self._send_repr(), self._compress_repr(), self._download_repr())

    def __repr__(self):
        return "%s\t[p%02d]: ST: %s | %s | stale: %s | fps: %s time: %s" % (self.host, self._processed, self.status, self._pipe_repr(), self.stale, self.fps, self.time)

    def run(self):
        try:
            self._status = FFMpegStatus.RUNNING
            self._send_thread.start()
            self._compress_thread.start()
            self._download_thread.start()

            while self.healthy:
                if not self._semaphore.acquire(False):
                    time.sleep(1)
                else:
                    try:
                        filepath = self._main_queue.get(timeout=1)

                        if type(filepath) is bytes:
                            filepath = filepath.decode()

                        self._send_queue.put(filepath)
                        self._main_queue.task_done()
                    except Empty:
                        pass

                if self._main_queue.empty() or self._main_queue.qsize() < self._qlimit:
                    self._status = FFMpegStatus.DRAINING
                    break

            self._send_queue.put(None)

            while not self.finished:
                time.sleep(0.5)

            self._send_thread.join()
            self._compress_thread.join()
            self._download_thread.join()
            self.remove_files()

            self._status = FFMpegStatus.FINISHED

        except Exception as e:
            logging.getLogger().exception(e)
            self._status = FFMpegStatus.BROKEN
            self._main_queue.task_done()
            self._main_queue.put(filepath)
            thread.exit()

if __name__ == "__main__":
    from glob import glob
    from tempfile import TemporaryDirectory
    
    import sys
    import getpass
    import argparse

    parser = argparse.ArgumentParser(
        description = 'Yet Another FFmpeg paralleizer'
    )
    parser.add_argument(
        '--hostlist',
        nargs='*',
        help='SSH endpoints with ffmpeg installed',
        required=True
    )
    parser.add_argument(
        '--ssh-user',
        dest='ssh_user',
        nargs='?',
        help='SSH user to use',
        required=False,
        default=getpass.getuser()
    )
    parser.add_argument(
        '--segment-length',
        dest='segment_length',
        nargs='?',
        type=int,
        help='Movie segment length',
        required=False,
        default=False
    )
    parser.add_argument(
        '--file',
        nargs='?',
        help='File to be yaffmpegpalized',
        required=True
    )
    args, ffmpegargs = parser.parse_known_args()

    print(f"hostlist: {args.hostlist}")
    print(f"file: {args.file}")
    print(f"ssh_user: {args.ssh_user}")
    print(f"ffmpeg args: {ffmpegargs}")

    with TemporaryDirectory() as tmp:
        subprocess.run(
            ['ffmpeg', '-i', args.file, '-c', 'copy', '-f', 'segment', '-reset_timestamps', '1', '-segment_time', str(args.segment_length), f"{tmp}/output%4d.mp4"]
        )
        files = glob(f"{tmp}/*.mp4")

        queue = Queue()
        for f in files:
            print("Adding {}".format(f))
            queue.put(f)
        print("Creating threads structures...")
        ffmpegs = [
            FFMpeg(f"{args.ssh_user}@{host}", queue, params=ffmpegargs) for host in args.hostlist
        ]

        print("Starting threads...")
        for f in ffmpegs:
            f.start()
        
        overall_fps = [0.0]
        while any(map(lambda x: x.status != FFMpegStatus.FINISHED.name, ffmpegs)):
            print("Qsize: %d" % queue.qsize())
            fps = 0.
            for f in ffmpegs:
                print("%s" % f)
                fps += f.fps
            overall_fps.append(fps)
            if len(overall_fps) > 100:
                overall_fps.pop(0)
            print("AVG: %.02f fps | CURR: %.02f fps" % (sum(overall_fps)/len(overall_fps), fps))
            print()
            time.sleep(1)

        processed_files = sorted([x for y in ffmpegs for x in y.processed_files])

        # this is horrible flacky part, hacked this way just to get shit 
        subprocess.run(
            ["ffmpeg", "-i", "concat:{}".format("|".join(processed_files)), "-c", "copy", "output.mp4"]
        )
        time.sleep(1)
