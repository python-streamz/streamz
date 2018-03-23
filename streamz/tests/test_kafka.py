import atexit
from contextlib import contextmanager
import os
import pytest
import random
import requests
import shlex
import subprocess
from time import sleep
from tornado import gen

from ..core import Stream
from ..dask import DaskStream
from streamz.utils_test import gen_test, wait_for
pytest.importorskip('distributed')
from distributed.utils_test import gen_cluster  # flake8: noqa

KAFKA_FILE = 'kafka_2.11-1.0.0'
LAUNCH_KAFKA = os.environ.get('STREAMZ_LAUNCH_KAFKA', '') == 'true'
TOPIC = 'test'
ck = pytest.importorskip('confluent_kafka')


def download_kafka(target):
    r = requests.get('http://apache.mirror.globo.tech/kafka/1.0.0/'
                     '%s.tgz' % KAFKA_FILE, stream=True)
    with open(target, 'wb') as f:
        for chunk in r.iter_content(2 ** 20):
            f.write(chunk)
    subprocess.check_call(['tar', 'xzf', KAFKA_FILE],
                          cwd=os.path.dirname(target))


def stop_docker(name='streamz-kafka', cid=None, let_fail=False):
    """Stop docker container with given name tag

    Parameters
    ----------
    name: str
        name field which has been attached to the container we wish to remove
    cid: str
        container ID, if known
    let_fail: bool
        whether to raise an exception if the underlying commands return an
        error.
    """
    try:
        if cid is None:
            print('Finding %s ...' % name)
            cmd = shlex.split('docker ps -q --filter "name=%s"' % name)
            cid = subprocess.check_output(cmd).strip().decode()
        if cid:
            print('Stopping %s ...' % cid)
            subprocess.call(['docker', 'rm', '-f', cid])
    except subprocess.CalledProcessError as e:
        print(e)
        if not let_fail:
            raise


def launch_kafka():
    stop_docker(let_fail=True)
    cmd = ("docker run -d -p 2181:2181 -p 9092:9092 --env "
           "ADVERTISED_HOST=127.0.01 --env ADVERTISED_PORT=9092 "
           "--name streamz-kafka spotify/kafka")
    print(cmd)
    cid = subprocess.check_output(shlex.split(cmd)).decode()[:-1]

    def end():
        if cid:
            stop_docker(cid=cid)
    atexit.register(end)

    def predicate():
        try:
            out = subprocess.check_output(['docker', 'logs', cid],
                                      stderr=subprocess.STDOUT)
            return b'kafka entered RUNNING state' in out
        except subprocess.CalledProcessError:
            pass
    wait_for(predicate, 10, period=0.1)
    return cid


def create_kafka_topic(cid, topic, partitions):
    cmd = ("docker exec {cid} /opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh "
           "--create --zookeeper localhost:2181 --replication-factor 1 "
           "--partitions {parts} --topic {topic}".format(
                cid=cid, parts=partitions, topic=topic))
    out = subprocess.check_output(shlex.split(cmd))
    return b'Created topic' in out


_kafka = [None]


@contextmanager
def kafka_service():
    if _kafka[0] is None:
        if LAUNCH_KAFKA:
            launch_kafka()
        else:
            raise pytest.skip.Exception(
                "Kafka not available. "
                "To launch kafka use `export STREAMZ_LAUNCH_KAFKA=true`")

        producer = ck.Producer({'bootstrap.servers': 'localhost:9092'})
        producer.produce('test-start-kafka', b'test')
        out = producer.flush(10)
        if out > 0:
            raise RuntimeError('Timeout waiting for kafka')
        _kafka[0] = producer
    yield _kafka[0]


@gen_test(timeout=60)
def test_from_kafka():
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j}
    with kafka_service() as kafka:
        stream = Stream.from_kafka([TOPIC], ARGS, asynchronous=True)
        out = stream.sink_to_list()
        stream.start()
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        # it takes some time for messages to come back out of kafka
        startlen = len([o for o in out if o])
        wait_for(lambda: len([o for o in out if o]) == startlen + 10, 10,
                 period=0.1)  # list may include some test data
        assert out[-1] == b'value-9'

        kafka.produce(TOPIC, b'final message')
        kafka.flush()
        wait_for(lambda: out[-1] == b'final message', 10, period=0.1)

        stream._close_consumer()
        kafka.produce(TOPIC, b'lost message')
        kafka.flush()
        # absolute sleep here, since we expect output list *not* to change
        yield gen.sleep(1)
        assert out[-1] == b'final message'


@gen_test(timeout=60)
def test_from_kafka_thread():
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j}
    with kafka_service() as kafka:
        stream = Stream.from_kafka([TOPIC], ARGS)
        out = stream.sink_to_list()
        stream.start()
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        # it takes some time for messages to come back out of kafka
        startlen = len([o for o in out if o])
        wait_for(lambda: len([o for o in out if o]) == startlen + 10, 10,
                 period=0.1)

        assert out[-1] == b'value-9'
        kafka.produce(TOPIC, b'final message')
        kafka.flush()
        wait_for(lambda: out[-1] == b'final message', 10, period=0.1)

        stream._close_consumer()
        kafka.produce(TOPIC, b'lost message')
        kafka.flush()
        # absolute sleep here, since we expect output list *not* to change
        sleep(1)
        assert out[-1] == b'final message'


def test_kafka_batch():
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j}
    with kafka_service() as kafka:
        stream = Stream.from_kafka_batched(TOPIC, ARGS)
        out = stream.sink_to_list()
        stream.start()
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        # out may still be empty or first item of out may be []
        wait_for(lambda: any(out) and out[-1][-1] == b'value-9', 10, period=0.2)
        stream.stopped = True


@gen_cluster(client=True, timeout=60)
def test_kafka_dask_batch(c, s, w1, w2):
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j}
    with kafka_service() as kafka:
        stream = Stream.from_kafka_batched(TOPIC, ARGS, asynchronous=True,
                                           dask=True)
        stream.start()
        assert isinstance(stream, DaskStream)
        out = stream.gather().sink_to_list()
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        timeout = 10
        while not any(out):
            yield gen.sleep(0.2)
            timeout -= 0.2
            assert timeout > 0, "Timeout"
        assert out[0][-1] == b'value-9'
        stream.stopped = True
