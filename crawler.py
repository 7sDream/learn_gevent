import gevent.monkey
import gevent.queue
import gevent.pool
import gevent.server

gevent.monkey.patch_all()

import os
import sqlite3
import socket
import redis
import logging.handlers

import zhihu_oauth

from settings import (
    DB_FILENAME, TOKEN_FILENAME, LOG_FILENAME, ERROR_LOG_FILENAME,
    LOG_FORMATTER, REDIS_KEY_SET, REDIS_KEY_QUEUE, REDIS_KET_TIME,
    COMMAND_STATE, COMMAND_FINISH, COMMAND_REMAIN, COMMAND_WORKER,
    COMMAND_PAUSE,
    COMMAND_RUN, COMMAND_STOP,
    RESPONSE_STATE, RESPONSE_FINISH, RESPONSE_WORKER, RESPONSE_REMAIN,
    RESPONSE_NOT_UNDERSTAND, RESPONSE_PAUSE, RESPONSE_PAUSE_FINISH,
    RESPONSE_RUN, RESPONSE_STOP, RESPONSE_WAIT_DB
)
from createdb import create_or_get
from stopwatch import stop_watch

# ---------- Logger setting ------------

logging.basicConfig(level=logging.NOTSET, handlers=[])

L = logging.getLogger(__name__)
L.setLevel(logging.NOTSET)
formatter = logging.Formatter(LOG_FORMATTER)
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
fh = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024000,
                                          backupCount=5)
fh.setLevel(logging.NOTSET)
fh.setFormatter(formatter)
error_fh = logging.handlers.RotatingFileHandler(ERROR_LOG_FILENAME,
                                                maxBytes=1024000, backupCount=2)
error_fh.setFormatter(formatter)
error_fh.setLevel(logging.WARN)
L.addHandler(sh)
L.addHandler(fh)
L.addHandler(error_fh)


# --------- utils function --------


def get_user_date(user):
    answer = user.answer_count
    question = user.question_count
    vote = user.voteup_count
    thank = user.thanked_count
    following = user.following_count
    follower = user.follower_count

    school = major = ''
    for education in user.educations:
        if 'school' in education:
            school = education.school.name
        if 'major' in education:
            major = education.major.name
        break

    address = ''
    for location in user.locations:
        address = location.name
        break

    industry = user.business.name if user.business else ''

    company = job = ''
    for employment in user.employments:
        if 'company' in employment:
            company = employment.company.name
        if 'job' in employment:
            job += employment.job.name
        break

    return user.id, user.name, user.headline, user.gender, answer, \
        question, vote, thank, following, follower, school, major, \
        address, industry, company, job,


def tobytes(*data):
    return tuple(str(x).encode('utf-8') for x in data)


def is_command(data, cmd):
    return data.startswith(cmd)


# --------- main crawler class -----

class StopByClientException(Exception):
    pass


class ZhihuUserCrawler(object):
    def __init__(self, worker_num=12, root_user_id=None, db_name=DB_FILENAME):

        # build client
        self.client = zhihu_oauth.ZhihuClient()
        # login logic
        if os.path.isfile(TOKEN_FILENAME):
            self.client.load_token(TOKEN_FILENAME)
        else:
            success = False
            while not success:
                success, _ = self.client.login_in_terminal()
                if success:
                    self.client.save_token(TOKEN_FILENAME)

        self.r = redis.StrictRedis(decode_responses=True)

        # add root user if not in set
        if not self.r.exists(REDIS_KEY_SET):
            self.r.sadd(REDIS_KEY_SET, root_user_id)
        if not self.r.exists(REDIS_KEY_QUEUE):
            self.r.sadd(REDIS_KEY_QUEUE, root_user_id)

        # create db writer queue
        self.db_q = gevent.queue.Queue()

        # create db connection
        self.conn = create_or_get(db_name)

        # db writer will be create at start method
        self.writer = None

        # create gevent pool
        self.pool = gevent.pool.Pool(worker_num)

        # start sockets server
        self.server = gevent.server.StreamServer(('127.0.0.1', 9981),
                                                 self._handler)
        self.pool.spawn(self._server)

        # create stopwatch
        self.watch = stop_watch(self.r.incrbyfloat(REDIS_KET_TIME, 0))

        # state is stop now, will be running in start method
        self.state = 'stop'

    def start(self):
        self.watch.start()
        self.state = 'running'
        L.info("Crawler start!")
        self.writer = self.pool.spawn(self._db_writer)

        self.new_worker()
        gevent.sleep(5)

        # main loop
        while self.r.scard(REDIS_KEY_QUEUE) != 0:
            while self.state != 'running':
                if self.state == 'stop':
                    raise StopByClientException
                gevent.sleep(1)
            self.pool.wait_available()
            self.new_worker()

        # wait for worker stop when stop by client
        while self._worker_count > 0:
            gevent.sleep(1)

        self.state = 'stop'
        self.writer = None
        L.info("Finished!")

    def _work(self, user):
        L.debug("Start work for " + user.id)
        if self.state != 'running':
            self.r.sadd(REDIS_KEY_QUEUE, user.id)
            return
        try:
            for following in user.followings:
                if self.r.sadd(REDIS_KEY_SET, following.id) == 1:
                    self.r.sadd(REDIS_KEY_QUEUE, following.id)
                    L.debug(' '.join(
                        ['get info of', user.name, "'s following",
                         following.name]))
                    self.db_q.put(get_user_date(following))
            return 0
        except BaseException as e:
            L.warn('worker died! Reason: ' + str(e))
            self.r.sadd(REDIS_KEY_QUEUE, user.id)
            raise e

    def new_worker(self):
        user = self.client.people(self.r.spop(REDIS_KEY_QUEUE))

        # for root user
        if self.r.sadd(REDIS_KEY_SET, user.id) == 1:
            self.db_q.put(get_user_date(user))
            self.r.sadd(REDIS_KEY_SET, user.id)

        self.pool.spawn(self._work, user)

    def _server(self):
        L.info("Server start!")
        while True:
            try:
                self.server.serve_forever()
            except OSError:
                L.warn('Server address be used.')
                exit(1)
            except Exception as e:
                L.warn('Error happened in server: ' + str(e))

    def _handler(self, sock, _):
        data = sock.recv(1024)
        L.info("received a command {0}.".format(data.decode('utf-8').strip('\n')))
        if is_command(data, COMMAND_STATE):
            data = RESPONSE_STATE % tobytes(self.state)
        elif is_command(data, COMMAND_FINISH):
            s = self.r.scard(REDIS_KEY_SET)
            t = self.watch.elapsed
            v = s / t
            data = RESPONSE_FINISH % tobytes(s, t, v)
        elif is_command(data, COMMAND_REMAIN):
            data = RESPONSE_REMAIN % tobytes(self.r.scard(REDIS_KEY_QUEUE))
        elif is_command(data, COMMAND_WORKER):
            data = RESPONSE_WORKER % tobytes(self._worker_count)
        elif is_command(data, COMMAND_PAUSE):
            if self.state == 'running':
                self._pause(sock)
            data = RESPONSE_PAUSE_FINISH
        elif is_command(data, COMMAND_RUN):
            if self.state == '_pause':
                self.watch.start()
                self.state = 'running'
                data = RESPONSE_RUN
            else:
                data = RESPONSE_STATE % tobytes(self.state)
        elif is_command(data, COMMAND_STOP):
            if self.state == 'running':
                self._pause(sock)
            if self.state == 'pause':
                self._stop(sock)
                data = RESPONSE_STOP
            else:
                data = RESPONSE_STATE % tobytes(self.state)
        else:
            data = RESPONSE_NOT_UNDERSTAND
        sock.send(data)
        sock.shutdown(socket.SHUT_RDWR)

    def _pause(self, sock):
        self.state = 'pausing'
        sock.send(RESPONSE_PAUSE)
        L.info('Try to pause crawler...')
        while self._worker_count > 0:
            sock.send((RESPONSE_WORKER.replace(b'\n', b'\r')) %
                      tobytes(self._worker_count))
            gevent.sleep(10)
        self.watch.stop()
        self.save_time()
        self.state = 'pause'
        L.info('Crawler pause.')

    def _stop(self, sock):
        L.info('Try to stop crawler.')
        if self.state != 'pause':
            self._pause(sock)
        sock.send(RESPONSE_WAIT_DB)
        L.info('Waiting database writer finish...')
        while not self.db_q.empty():
            gevent.sleep(1)
        L.info('Database writer finish.')
        if self.writer:
            self.writer.kill()
            self.writer = None
        self.state = 'stop'
        L.info('Crawler stopped.')

    def _db_writer(self):
        while True:
            try:
                with self.conn:
                    while not self.db_q.empty():
                        data = self.db_q.get()
                        try:
                            self.conn.execute(
                                'INSERT INTO user VALUES ('
                                '?, ?, ?, ?, ?, ?, ?, ?, '
                                '?, ?, ?, ?, ?, ?, ?, ?)',
                                data
                            )
                            L.debug('write ' + data[1] + 'to db')
                        except sqlite3.Error as e:
                            L.warn('database error!' + str(e))
            except Exception as e:
                L.warn('some error happeded!' + str(e))
            gevent.sleep(10)

    def save_time(self):
        self.r.set(REDIS_KET_TIME, self.watch.elapsed)

    @property
    def _worker_count(self):
        return self.pool.size - self.pool.free_count() - 2


# ----------------------------------

if __name__ == '__main__':
    crawler = ZhihuUserCrawler(root_user_id='7sdream')
    try:
        crawler.start()
    except StopByClientException:
        L.info("Crawler stop by client command.")
    except Exception:
        crawler.save_time()
