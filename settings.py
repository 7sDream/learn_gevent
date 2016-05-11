DB_FILENAME = 'db.sqlite3'
TOKEN_FILENAME = 'token.pkl'
LOG_FILENAME = 'log.txt'
ERROR_LOG_FILENAME = 'error.log'
REDIS_KEY_SET = 'ZHIHU_CRAWLER_SET'
REDIS_KEY_QUEUE = 'ZHIHU_CRAWLER_QUEUE'
REDIS_KET_TIME = 'ZHIHU_CRAWLER_TIME'
LOG_FORMATTER = '[%(asctime)s] %(levelname)s ' \
                '[%(name)s.%(funcName)s:%(lineno)d] %(message)s'

COMMAND_STATE = b'state'
RESPONSE_STATE = b"I'm %s.\n"

COMMAND_FINISH = b'crawled'
RESPONSE_FINISH = b"I crawled %s people in %s, speed %s, I'm great!\n"

COMMAND_REMAIN = b'left'
RESPONSE_REMAIN = b'I have %s people to crawl. Oh, I hate work!\n'

COMMAND_PAUSE = b'pause'
RESPONSE_PAUSE = b'Ok, But please wait a moment...\n'
RESPONSE_PAUSE_FINISH = b'Pause finish.\n'

COMMAND_WORKER = b'worker'
RESPONSE_WORKER = b'There are %2s worker(s) crawling now.\n'

COMMAND_RUN = b'run'
RESPONSE_RUN = b'I will try my best to work for you!\n'

COMMAND_STOP = b'stop'
RESPONSE_STOP = b'Stopped(file dumped).\n'

RESPONSE_NOT_UNDERSTAND = b"I can't understand your words.\n"

RESPONSE_WAIT_DB = b'Waiting for database writer quit...\n'
RESPONSE_DUMP_QUEUE = b'Dumping task queue...\n'
RESPONSE_DUMP_SET = b'Dumping set...\n'
