import asyncio
import atexit
from contextlib import contextmanager
from flaky import flaky
import os
import pytest
import random
import shlex
import subprocess
import time
from tornado import gen


from ..core import Stream
from ..dask import DaskStream
from streamz.utils_test import gen_test, wait_for, await_for
pytest.importorskip('distributed')
from distributed.utils_test import gen_cluster  # flake8: noqa

KAFKA_FILE = 'kafka_2.11-1.0.0'
LAUNCH_KAFKA = os.environ.get('STREAMZ_LAUNCH_KAFKA', '') == 'true'
ck = pytest.importorskip('confluent_kafka')


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
    subprocess.call(shlex.split("docker pull spotify/kafka"))
    cmd = ("docker run -d -p 2181:2181 -p 9092:9092 --env "
           "ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 "
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


_kafka = [None]


@contextmanager
def kafka_service():
    TOPIC = "test-%i" % random.randint(0, 10000)
    if _kafka[0] is None:
        if LAUNCH_KAFKA:
            launch_kafka()
        else:
            raise pytest.skip.Exception(
                "Kafka not available. "
                "To launch kafka use `export STREAMZ_LAUNCH_KAFKA=true`")

        producer = ck.Producer({'bootstrap.servers': 'localhost:9092',
                                'topic.metadata.refresh.interval.ms': '5000'})
        producer.produce('test-start-kafka', b'test')
        out = producer.flush(10)
        if out > 0:
            raise RuntimeError('Timeout waiting for kafka')
        _kafka[0] = producer
    yield _kafka[0], TOPIC


def split(messages):
    parsed = []
    for message in messages:
        message = message.decode("utf-8")
        parsed.append(int(message.split('-')[1]))
    return parsed


@flaky(max_runs=3, min_passes=1)
@gen_test(timeout=60)
def test_from_kafka():
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        stream = Stream.from_kafka([TOPIC], ARGS, asynchronous=True)
        out = stream.sink_to_list()
        stream.start()
        yield gen.sleep(1.1)  # for loop to run
        for i in range(10):
            yield gen.sleep(0.1)  # small pause ensures correct ordering
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        # it takes some time for messages to come back out of kafka
        wait_for(lambda: len(out) == 10, 10, period=0.1)
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
        stream._close_consumer()


@flaky(max_runs=3, min_passes=1)
@gen_test(timeout=60)
def test_to_kafka():
    ARGS = {'bootstrap.servers': 'localhost:9092'}
    with kafka_service() as kafka:
        _, TOPIC = kafka
        source = Stream()
        kafka = source.to_kafka(TOPIC, ARGS)
        out = kafka.sink_to_list()

        for i in range(10):
            yield source.emit(b'value-%d' % i)

        source.emit('final message')
        kafka.flush()
        wait_for(lambda: len(out) == 11, 10, period=0.1)
        assert out[-1] == b'final message'


@flaky(max_runs=3, min_passes=1)
@gen_test(timeout=60)
def test_from_kafka_thread():
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        stream = Stream.from_kafka([TOPIC], ARGS)
        out = stream.sink_to_list()
        stream.start()
        yield gen.sleep(1.1)
        for i in range(10):
            yield gen.sleep(0.1)
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        # it takes some time for messages to come back out of kafka
        yield await_for(lambda: len(out) == 10, 10, period=0.1)

        assert out[-1] == b'value-9'
        kafka.produce(TOPIC, b'final message')
        kafka.flush()
        yield await_for(lambda: out[-1] == b'final message', 10, period=0.1)

        stream._close_consumer()
        kafka.produce(TOPIC, b'lost message')
        kafka.flush()
        # absolute sleep here, since we expect output list *not* to change
        yield gen.sleep(1)
        assert out[-1] == b'final message'
        stream._close_consumer()


def test_kafka_batch():
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j,
            'auto.offset.reset': 'latest'}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        # These messages aren't read since Stream starts reading from latest offsets
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i, b'%d' % i)
        kafka.flush()
        stream = Stream.from_kafka_batched(TOPIC, ARGS, max_batch_size=4, keys=True)
        out = stream.sink_to_list()
        stream.start()
        wait_for(lambda: stream.upstream.started, 10, 0.1)
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i, b'%d' % i)
        kafka.flush()
        # out may still be empty or first item of out may be []
        wait_for(lambda: any(out) and out[-1][-1]['value'] == b'value-9', 10, period=0.2)
        assert out[-1][-1]['key'] == b'9'
        # max_batch_size checks
        assert len(out[0]) == len(out[1]) == 4 and len(out) == 3
        stream.upstream.stopped = True


@gen_cluster(client=True, timeout=60)
async def test_kafka_dask_batch(c, s, w1, w2):
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        stream = Stream.from_kafka_batched(TOPIC, ARGS, keys=True,
                                           asynchronous=True, dask=True)
        out = stream.gather().sink_to_list()
        stream.start()
        await asyncio.sleep(5)  # this frees the loop while dask workers report in
        assert isinstance(stream, DaskStream)
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        await await_for(lambda: any(out), 10, period=0.2)
        assert {'key': None, 'value': b'value-1'} in out[0]
        stream.stop()
        await asyncio.sleep(0)
        stream.upstream.upstream.consumer.close()


def test_kafka_batch_npartitions():
    j1 = random.randint(0, 10000)
    ARGS1 = {'bootstrap.servers': 'localhost:9092',
             'group.id': 'streamz-test%i' % j1,
             'enable.auto.commit': False,
             'auto.offset.reset': 'earliest'}
    j2 = j1 + 1
    ARGS2 = {'bootstrap.servers': 'localhost:9092',
             'group.id': 'streamz-test%i' % j2,
             'enable.auto.commit': False,
             'auto.offset.reset': 'earliest'}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka

        TOPIC = "test-partitions"
        subprocess.call(shlex.split("docker exec streamz-kafka "
                                    "/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh "
                                    "--create --zookeeper localhost:2181 "
                                    "--replication-factor 1 --partitions 2 "
                                    "--topic test-partitions"))

        for i in range(10):
            if i % 2 == 0:
                kafka.produce(TOPIC, b'value-%d' % i, partition=0)
            else:
                kafka.produce(TOPIC, b'value-%d' % i, partition=1)
        kafka.flush()

        with pytest.raises(ValueError):
            stream1 = Stream.from_kafka_batched(TOPIC, ARGS1,
                                                asynchronous=True,
                                                npartitions=0)
            stream1.gather().sink_to_list()
            stream1.start()

        stream2 = Stream.from_kafka_batched(TOPIC, ARGS1,
                                            asynchronous=True,
                                            npartitions=1)
        out2 = stream2.gather().sink_to_list()
        stream2.start()
        wait_for(lambda: stream2.upstream.started, 10, 0.1)
        wait_for(lambda: len(out2) == 1 and len(out2[0]) == 5, 10, 0.1)
        stream2.upstream.stopped = True

        stream3 = Stream.from_kafka_batched(TOPIC, ARGS2,
                                            asynchronous=True,
                                            npartitions=4)
        out3 = stream3.gather().sink_to_list()
        stream3.start()
        wait_for(lambda: stream3.upstream.started, 10, 0.1)
        wait_for(lambda: len(out3) == 2 and (len(out3[0]) + len(out3[1])) == 10, 10, 0.1)
        stream3.upstream.stopped = True


def test_kafka_refresh_partitions():
    j1 = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j1,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        TOPIC = "test-refresh-partitions"
        subprocess.call(shlex.split("docker exec streamz-kafka "
                                    "/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh "
                                    "--create --zookeeper localhost:2181 "
                                    "--replication-factor 1 --partitions 2 "
                                    "--topic test-refresh-partitions"))

        for i in range(10):
            if i % 2 == 0:
                kafka.produce(TOPIC, b'value-%d' % i, partition=0)
            else:
                kafka.produce(TOPIC, b'value-%d' % i, partition=1)
        kafka.flush()

        stream = Stream.from_kafka_batched(TOPIC, ARGS,
                                           asynchronous=True,
                                           refresh_partitions=True,
                                           poll_interval='2s')
        out = stream.gather().sink_to_list()
        stream.start()
        wait_for(lambda: stream.upstream.started, 10, 0.1)
        wait_for(lambda: len(out) == 2 and (len(out[0]) + len(out[1])) == 10, 10, 0.1)

        subprocess.call(shlex.split("docker exec streamz-kafka "
                                    "/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh "
                                    "--alter --zookeeper localhost:2181 "
                                    "--topic test-refresh-partitions --partitions 4"))
        time.sleep(5)
        for i in range(10,20):
            if i % 2 == 0:
                kafka.produce(TOPIC, b'value-%d' % i, partition=2)
            else:
                kafka.produce(TOPIC, b'value-%d' % i, partition=3)
        kafka.flush()

        wait_for(lambda: len(out) == 4 and (len(out[2]) + len(out[3])) == 10
                         and out[3][4] == b'value-19', 10, 0.1)
        stream.upstream.stopped = True


def test_kafka_batch_checkpointing_sync_nodes():
    '''
    Streams 1 and 3 have different consumer groups, while Stream 2
    has the same group as 1. Hence, Stream 2 does not re-read the
    data that had been finished processing by Stream 1, i.e. it
    picks up from where Stream 1 had left off.
    '''
    j1 = random.randint(0, 10000)
    ARGS1 = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j1,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    j2 = j1 + 1
    ARGS2 = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j2,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        stream1 = Stream.from_kafka_batched(TOPIC, ARGS1)
        out1 = stream1.map(split).filter(lambda x: x[-1] % 2 == 1).sink_to_list()
        stream1.start()
        wait_for(lambda: any(out1) and out1[-1][-1] == 9, 10, period=0.2)
        stream1.upstream.stopped = True
        stream2 = Stream.from_kafka_batched(TOPIC, ARGS1)
        out2 = stream2.map(split).filter(lambda x: x[-1] % 2 == 1).sink_to_list()
        stream2.start()
        time.sleep(5)
        assert len(out2) == 0
        stream2.upstream.stopped = True
        stream3 = Stream.from_kafka_batched(TOPIC, ARGS2)
        out3 = stream3.map(split).filter(lambda x: x[-1] % 2 == 1).sink_to_list()
        stream3.start()
        wait_for(lambda: any(out3) and out3[-1][-1] == 9, 10, period=0.2)
        stream3.upstream.stopped = True


@gen_cluster(client=True, timeout=60)
async def test_kafka_dask_checkpointing_sync_nodes(c, s, w1, w2):
    '''
    Testing whether Dask's scatter and gather works in conformity with
    the reference counting checkpointing implementation.
    '''
    j1 = random.randint(0, 10000)
    ARGS1 = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j1,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    j2 = j1 + 1
    ARGS2 = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j2,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        for i in range(10):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()
        stream1 = Stream.from_kafka_batched(TOPIC, ARGS1, asynchronous=True,
                                            dask=True)
        out1 = stream1.map(split).gather().filter(lambda x: x[-1] % 2 == 1).sink_to_list()
        stream1.start()
        await await_for(lambda: any(out1) and out1[-1][-1] == 9, 10, period=0.2)
        stream1.upstream.stopped = True
        stream2 = Stream.from_kafka_batched(TOPIC, ARGS1, asynchronous=True,
                                            dask=True)
        out2 = stream2.map(split).gather().filter(lambda x: x[-1] % 2 == 1).sink_to_list()
        stream2.start()
        time.sleep(5)
        assert len(out2) == 0
        stream2.upstream.stopped = True
        stream3 = Stream.from_kafka_batched(TOPIC, ARGS2, asynchronous=True,
                                            dask=True)
        out3 = stream3.map(split).gather().filter(lambda x: x[-1] % 2 == 1).sink_to_list()
        stream3.start()
        await await_for(lambda: any(out3) and out3[-1][-1] == 9, 10, period=0.2)
        stream3.upstream.stopped = True


def test_kafka_batch_checkpointing_async_nodes_1():
    '''
    In async nodes like partition & sliding window, data is checkpointed only after
    the pipeline has finished processing it.
    '''
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j,
            'enable.auto.commit': False}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        stream1 = Stream.from_kafka_batched(TOPIC, ARGS)
        out1 = stream1.partition(2).sliding_window(2, return_partial=False).sink_to_list()
        stream1.start()
        for i in range(0,2):
            kafka.produce(TOPIC, b'value-%d' % i)
            kafka.flush()
            time.sleep(2)
        assert len(out1) == 0
        #Stream stops before data can finish processing, hence no checkpointing.
        stream1.upstream.stopped = True
        stream1.destroy()
        stream2 = Stream.from_kafka_batched(TOPIC, ARGS)
        out2 = stream2.partition(2).sliding_window(2, return_partial=False).sink_to_list()
        stream2.start()
        wait_for(lambda: stream2.upstream.started, 10, 0.1)
        for i in range(2,6):
            kafka.produce(TOPIC, b'value-%d' % i)
            kafka.flush()
            time.sleep(2)
        assert len(out2) == 1
        assert out2 == [(([b'value-0', b'value-1'], [b'value-2']), ([b'value-3'], [b'value-4']))]
        #Some data gets processed and exits pipeline before the stream stops, hence checkpointing complete.
        stream2.upstream.stopped = True
        stream2.destroy()
        stream3 = Stream.from_kafka_batched(TOPIC, ARGS)
        out3 = stream3.sink_to_list()
        stream3.start()
        wait_for(lambda: stream3.upstream.started, 10, 0.1)
        #Stream picks up from where it left before, i.e., from the last committed offset.
        wait_for(lambda: len(out3) == 1 and out3[0] == [b'value-3', b'value-4', b'value-5'], 10, 0.1)
        stream3.upstream.stopped = True
        stream3.destroy()


def test_kafka_batch_checkpointing_async_nodes_2():
    '''
    In async nodes like zip_latest, zip, combine_latest which involve multiple streams,
    checkpointing in each stream commits offsets after the datum in that specific stream
    is processed completely and exits the pipeline.
    '''
    CONSUMER_ARGS1 = {'bootstrap.servers': 'localhost:9092',
                      'group.id': 'zip_latest',
                      'enable.auto.commit': False}
    CONSUMER_ARGS2 = {'bootstrap.servers': 'localhost:9092',
                      'group.id': 'zip',
                      'enable.auto.commit': False}
    CONSUMER_ARGS3 = {'bootstrap.servers': 'localhost:9092',
                      'group.id': 'combine_latest',
                      'enable.auto.commit': False}
    TOPIC1 = 'test1'
    TOPIC2 = 'test2'
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        stream1 = Stream.from_kafka_batched(TOPIC1, CONSUMER_ARGS1, asynchronous=True)
        stream1.start()
        stream2 = Stream.from_kafka_batched(TOPIC2, CONSUMER_ARGS1, asynchronous=True)
        stream2.start()
        stream1.zip_latest(stream2).sink_to_list()
        stream3 = Stream.from_kafka_batched(TOPIC1, CONSUMER_ARGS2, asynchronous=True)
        stream3.start()
        stream4 = Stream.from_kafka_batched(TOPIC2, CONSUMER_ARGS2, asynchronous=True)
        stream4.start()
        stream3.zip(stream4).sink_to_list()
        stream5 = Stream.from_kafka_batched(TOPIC1, CONSUMER_ARGS3, asynchronous=True)
        stream5.start()
        stream6 = Stream.from_kafka_batched(TOPIC2, CONSUMER_ARGS3, asynchronous=True)
        stream6.start()
        stream5.combine_latest(stream6).sink_to_list()
        kafka.produce(TOPIC1, b'value-0')
        time.sleep(5)
        kafka.produce(TOPIC2, b'value-1')
        '''
        1. zip_latest emits a tuple, the lossless stream 1 commits an offset.
        2. Since zip emits a tuple, streams 3 and 4 commit offsets in their topics.
        3. combine_latest does not commit any offset since data is still to be used.
        '''
        time.sleep(5)
        kafka.produce(TOPIC1, b'value-2')
        '''
        1. zip_latest emits a tuple, the lossless stream 1 commits an offset.
        2. zip does not commit any offset.
        3. combine_latest commits an offset in stream 5.
        '''
        time.sleep(5)
        kafka.produce(TOPIC2, b'value-3')
        '''
        1. zip_latest emits a tuple, the non-lossless stream 2 commits an offset.
        2. Since zip emits a tuple, streams 3 and 4 commit offsets in their topics.
        3. combine_latest commits an offset in stream 6.
        '''
        time.sleep(10)
        stream1.upstream.stopped = True
        stream2.upstream.stopped = True
        stream3.upstream.stopped = True
        stream4.upstream.stopped = True
        stream5.upstream.stopped = True
        stream6.upstream.stopped = True
        stream1.destroy()
        stream2.destroy()
        stream3.destroy()
        stream4.destroy()
        stream5.destroy()
        stream6.destroy()
        '''
        Each stream/group.id picks up from their last committed offset.
        '''
        consumer1 = ck.Consumer(CONSUMER_ARGS1)
        consumer2 = ck.Consumer(CONSUMER_ARGS2)
        consumer3 = ck.Consumer(CONSUMER_ARGS3)
        tps = [ck.TopicPartition(TOPIC1, 0), ck.TopicPartition(TOPIC2, 0)]
        committed1 = consumer1.committed(tps)
        committed2 = consumer2.committed(tps)
        committed3 = consumer3.committed(tps)
        assert committed1[0].offset == 2
        assert committed1[1].offset == 1
        assert committed2[0].offset == 2
        assert committed2[1].offset == 2
        assert committed3[0].offset == 1
        assert committed3[1].offset == 1


@flaky(max_runs=3, min_passes=1)
def test_kafka_checkpointing_auto_offset_reset_latest():
    """
    Testing whether checkpointing works as expected with multiple topic partitions and
    with auto.offset.reset configuration set to latest (also default).
    """
    j = random.randint(0, 10000)
    ARGS = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'streamz-test%i' % j,
            'auto.offset.reset': 'latest'}
    with kafka_service() as kafka:
        kafka, TOPIC = kafka
        TOPIC = "test-checkpointing-offset-reset-latest"
        subprocess.call(shlex.split("docker exec streamz-kafka "
                                    "/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh "
                                    "--create --zookeeper localhost:2181 "
                                    "--replication-factor 1 --partitions 3 "
                                    "--topic test-checkpointing-offset-reset-latest"))

        '''
        Since the stream has not started yet, these messages are not read because
        the stream has auto.offset.reset set to latest.
        '''
        for i in range(30):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()

        stream1 = Stream.from_kafka_batched(TOPIC, ARGS, asynchronous=True)
        out1 = stream1.map(split).gather().sink_to_list()
        stream1.start()
        wait_for(lambda: stream1.upstream.started, 10, period=0.1)

        '''
        Stream has started, so these are read.
        '''
        for i in range(30):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()

        wait_for(lambda: len(out1) == 3 and (len(out1[0]) + len(out1[1]) + len(out1[2])) == 30,
                 10, period=0.1)
        '''
        Stream stops but checkpoint has been created.
        '''
        stream1.upstream.stopped = True

        '''
        When the stream is restarted, these messages are read, because the checkpoint
        overrrides the auto.offset.reset:latest config this time around as expected.
        '''
        for i in range(30):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()

        stream2 = Stream.from_kafka_batched(TOPIC, ARGS, asynchronous=True)
        out2 = stream2.map(split).gather().sink_to_list()

        '''
        Stream restarts here.
        '''
        stream2.start()
        wait_for(lambda: stream2.upstream.started, 10, 0.1)

        for i in range(30):
            kafka.produce(TOPIC, b'value-%d' % i)
        kafka.flush()

        wait_for(lambda: len(out2) == 6 and (len(out2[3]) + len(out2[4]) + len(out2[5])) == 30,
                 10, period=0.1)
        stream2.upstream.stopped = True
