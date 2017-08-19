# Yet Another FFmpeg parallelizer 

### Hello! My name is Łukasz!
Purely for the needs of the open source community I'm giving you this code. It was written only for my needs, I only made sure that it actually works (for me) and flake8 is not overly complaining about it.

### Cool, what is this
This is a way to paralleize ffmpeg run across fleet of computers, if you have one. Some time ago I was dumping old VHS tapes to preserve them indefinitely and I really missed something like this.

### But there are already different solutions!
Yea, but I had fun writing this. Plus it is more elastic (at least to me).

Moreover since some of the hosts where I was sending the parts of video were quite far away and had poor connectivity I implemented prefeching. Each queue has upload buffer and files are pre-uploaded while remote host is already compressing chunk of the video. Same for downloading.

Internally this is threaded pipeline based on simple state machine (I hope it's finite).

### Usage

```
usage: yaffmpegp.py [-h] --hostlist [HOSTLIST [HOSTLIST ...]]
                    [--ssh-user [SSH_USER]]
                    [--segment-length [SEGMENT_LENGTH]] --file [FILE]

Yet Another FFmpeg paralleizer

optional arguments:
  -h, --help            show this help message and exit
  --hostlist [HOSTLIST [HOSTLIST ...]]
                        SSH endpoints with ffmpeg installed
  --ssh-user [SSH_USER]
                        SSH user to use
  --segment-length [SEGMENT_LENGTH]
                        Movie segment length
  --file [FILE]         File to be yaffmpegpalized
```

### Example
```
./yaffmpegp.py --hostlist 10.0.0.2 10.0.0.3 --ssh-user lsiudut --segment-length 50 --file /dev/shm/whoa.mp4 -c:a copy -c:v libx264 -crf 25 -preset veryfast
```
At this point ffmpeg will split big file into chunks 50s each (approx). There will be a lot of logs printed out to stderr. Once the proper job started output will look like this:
```
Creating threads structures...
Starting threads...
Qsize: 14
10.66.0.2       [p00]: ST: RUNNING | SENDER(IDLE, 0) -> COMPRESS(IDLE, 0) -> DOWNLOAD(IDLE, 0) | stale: False | fps: 0.0 time: 
10.66.0.3       [p00]: ST: RUNNING | SENDER(IDLE, 0) -> COMPRESS(IDLE, 0) -> DOWNLOAD(IDLE, 0) | stale: False | fps: 0.0 time: 
AVG: 0.00 fps | CURR: 0.00 fps

Qsize: 10
10.66.0.2       [p00]: ST: RUNNING | SENDER(SENDING, 1, ->output0013.mp4) -> COMPRESS(IDLE, 0) -> DOWNLOAD(IDLE, 0) | stale: False | fps: 0.0 time: 
10.66.0.3       [p00]: ST: RUNNING | SENDER(SENDING, 1, ->output0011.mp4) -> COMPRESS(IDLE, 0) -> DOWNLOAD(IDLE, 0) | stale: False | fps: 0.0 time: 
AVG: 0.00 fps | CURR: 0.00 fps
```

What does it mean?
```
Qsize: 10
```
Remained queue length (files that are to be dispatched).

```
10.66.0.2       [p00]: ST: RUNNING | SENDER(SENDING, 1, ->output0013.mp4) -> COMPRESS(IDLE, 0) -> DOWNLOAD(IDLE, 0) | stale: False | fps: 0.0 time: 
```
Status of each pipeline in format:
```
<host> [pXX] ST: <status> | <worker_name>(<worker_status>, <queue_len>, [<filename>]) -> ... | stale: <is_it_stale> | fps: <worker_fps> | time: <ffmpeg_time_progress>
```
* host - host IP address
* pXX - how many files were processed by given host
* worker_name - SENDER, COMPRESS, DOWNLOAD - parts of the pipeline
* queue_len - how many files are currenlty queued for given worker
* filename - name of the currently processed file
* stale - do we received ffmpeg output in last few seconds - if it's stale you probably want to restart the job (yea, I didn't implement retries)
* fps - fps of given worker
* time - how many seconds of given file was processed

```
AVG: 0.00 fps | CURR: 0.00 fps
```

Average fps and current fps.

### Output

Files are (currently) split to files name `outputXXXX.mp4`. Results of compression is being downloaded to $PWD and final file is named output.mp4. Definitely not elegant, yet sufficient.
