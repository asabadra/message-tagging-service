# -*- coding: utf-8 -*-
#
# Message tagging service is an event-driven service to tag build.
# Copyright (C) 2019  Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Authors: Chenxiong Qi

import fedora_messaging.api
import json
import os
import pytest
import requests.exceptions
import yaml

from mock import patch, Mock
from message_tagging_service.consumer import run
from message_tagging_service import conf, consumer


test_data_dir = os.path.join(os.path.dirname(__file__), 'data')


class FakeKafkaRecord(object):
    """Minimal stand-in for kafka.consumer.fetcher.ConsumerRecord"""

    def __init__(self, value, topic='VirtualTopic.eng.mbs.module.state.change',
                 partition=0, offset=0):
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset


class FakeKafkaConsumer(object):
    """Fake KafkaConsumer that yields preset records without a real broker."""

    # Records the next constructed consumer will iterate over.
    records = []

    def __init__(self, *args, **kwargs):
        self.init_args = args
        self.init_kwargs = kwargs
        self.commit_count = 0
        self.closed = False

    def __iter__(self):
        return iter(FakeKafkaConsumer.records)

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


def _fake_kafka_module():
    """Return a fake ``kafka`` module exposing FakeKafkaConsumer."""
    fake_kafka = Mock()
    fake_kafka.KafkaConsumer = FakeKafkaConsumer
    return fake_kafka


class TestConsumer(object):
    """Test consumer method is able to handle two different type of messages

    There are two types of messages, one is coming from fedora-messaging, and
    another one is coming from Kafka.
    """

    @patch.object(conf, 'messaging_backend', new='kafka')
    @patch('message_tagging_service.consumer.tagging_service.handle')
    @patch('requests.get')
    def test_skip_if_rule_file_is_empty(self, get, handle):
        """An empty rule file must not reach the tagging service

        yaml.safe_load returns None for an empty document, which ``consume``
        has to treat as "no rules are defined" rather than passing None on to
        tagging_service.handle.
        """
        FakeKafkaConsumer.records = [
            FakeKafkaRecord(json.dumps({
                'name': 'python',
                'stream': '2.7',
                'version': '1',
                'context': 'c1',
                'state_name': 'ready',
            }).encode())
        ]
        get.return_value.text = '---'

        with patch.dict('sys.modules', {'kafka': _fake_kafka_module()}):
            run()

        handle.assert_not_called()

    @pytest.mark.parametrize('msg_body', [{
        'name': 'python',
        'stream': '2.7',
        'version': '1',
        'context': 'c1',
        'state_name': 'ready',
    }, {}])
    @patch('message_tagging_service.consumer.tagging_service.handle')
    @patch('requests.get')
    def test_handle_fedora_messaging_message(self, get, handle, msg_body):
        with open(os.path.join(test_data_dir, 'mts-test-rules.yaml'), 'r') as f:
            rules_content = f.read()

        get.return_value.text = rules_content

        def api_consume(callback):
            msg = fedora_messaging.api.Message(msg_body)
            callback(msg)

        with patch('fedora_messaging.api.consume', new=api_consume):
            run()

        if msg_body:
            handle.assert_called_once_with(yaml.safe_load(rules_content), msg_body)
        else:
            # In case event message is empty, MTS stops handling the message.
            handle.assert_not_called()

    @patch.object(conf, 'messaging_backend', new='kafka')
    @patch('message_tagging_service.consumer.tagging_service.handle')
    @patch('requests.get')
    def test_handle_kafka_message(self, get, handle):
        mbs_event_msg = {
            'name': 'python',
            'stream': '2.7',
            'version': '1',
            'context': 'c1',
            'state_name': 'ready',
        }
        FakeKafkaConsumer.records = [
            FakeKafkaRecord(json.dumps(mbs_event_msg).encode())
        ]

        with open(os.path.join(test_data_dir, 'mts-test-rules.yaml'), 'r') as f:
            rules_content = f.read()
        get.return_value.text = rules_content

        with patch.dict('sys.modules', {'kafka': _fake_kafka_module()}):
            run()

        handle.assert_called_once_with(
            yaml.safe_load(rules_content), mbs_event_msg)

    @patch.object(conf, 'messaging_backend', new='kafka')
    @patch('message_tagging_service.consumer.tagging_service.handle')
    @patch('requests.get')
    def test_kafka_consumer_commits_and_closes(self, get, handle):
        mbs_event_msg = {
            'name': 'python',
            'stream': '2.7',
            'version': '1',
            'context': 'c1',
            'state_name': 'ready',
        }
        record = FakeKafkaRecord(json.dumps(mbs_event_msg).encode())
        FakeKafkaConsumer.records = [record]

        with open(os.path.join(test_data_dir, 'mts-test-rules.yaml'), 'r') as f:
            get.return_value.text = f.read()

        created = []
        real_init = FakeKafkaConsumer.__init__

        def _track_init(self, *args, **kwargs):
            real_init(self, *args, **kwargs)
            created.append(self)

        with patch.dict('sys.modules', {'kafka': _fake_kafka_module()}):
            with patch.object(FakeKafkaConsumer, '__init__', _track_init):
                run()

        assert len(created) == 1
        consumer_instance = created[0]
        # Offset is committed once, after the message is processed.
        assert consumer_instance.commit_count == 1
        assert consumer_instance.closed is True

    @patch.object(conf, 'messaging_backend', new='kafka')
    @patch('message_tagging_service.consumer.tagging_service.handle')
    @patch('requests.get')
    def test_log_error_if_kafka_message_body_is_invalid(self, get, handle):
        with open(os.path.join(test_data_dir, 'mts-test-rules.yaml'), 'r') as f:
            get.return_value.text = f.read()
        FakeKafkaConsumer.records = [FakeKafkaRecord(b'non-JSON message body')]

        with patch.dict('sys.modules', {'kafka': _fake_kafka_module()}):
            with patch.object(consumer, 'logger') as logger:
                run()

        handle.assert_not_called()
        args, _ = logger.error.call_args_list[0]
        assert "Cannot decode message body: b'non-JSON message body'" == args[0]
        args, _ = logger.error.call_args_list[1]
        assert args[0].startswith('Reason:')

    @patch.object(conf, 'messaging_backend', new='kafka')
    @patch('message_tagging_service.consumer.tagging_service.handle')
    @patch('requests.get')
    def test_kafka_commits_offset_when_message_cannot_be_handled(self, get, handle):
        """A message that makes ``consume`` raise must not be re-delivered

        The body is valid JSON and passes the state filter, but lacks the
        fields used to build the NSVC, so ``consume`` raises KeyError. The
        offset still has to be committed, otherwise the consumer would be
        re-delivered the same message forever.
        """
        with open(os.path.join(test_data_dir, 'mts-test-rules.yaml'), 'r') as f:
            get.return_value.text = f.read()
        FakeKafkaConsumer.records = [
            FakeKafkaRecord(json.dumps({'state_name': 'ready'}).encode())
        ]

        created = []
        real_init = FakeKafkaConsumer.__init__

        def _track_init(self, *args, **kwargs):
            real_init(self, *args, **kwargs)
            created.append(self)

        with patch.dict('sys.modules', {'kafka': _fake_kafka_module()}):
            with patch.object(FakeKafkaConsumer, '__init__', _track_init):
                with patch.object(consumer, 'logger') as logger:
                    run()

        handle.assert_not_called()
        logger.exception.assert_called_once()
        assert created[0].commit_count == 1
        assert created[0].closed is True

    def test_kafka_message(self):
        from message_tagging_service.consumer import KafkaMessage
        msg = KafkaMessage(FakeKafkaRecord(
            value=json.dumps({'name': 'modulea'}).encode(),
            topic='VirtualTopic.eng.mbs.module.state.change',
            partition=2,
            offset=42,
        ))
        assert 'VirtualTopic.eng.mbs.module.state.change-2-42' == msg.id
        assert {'name': 'modulea'} == msg.body
        assert 'VirtualTopic.eng.mbs.module.state.change' == msg.topic

    def test_raise_error_if_specified_messaging_backend_is_unknown(self):
        with patch.object(conf, 'messaging_backend', new='xxxxxx'):
            with pytest.raises(ValueError, match='Unknown messaging backend: .+'):
                run()

    @patch('message_tagging_service.consumer.tagging_service.handle')
    @patch('requests.get')
    def test_consume_terminates_if_fail_to_read_rules_from_remote(self, get, handle):
        get.side_effect = requests.exceptions.HTTPError
        consumer.consume(Mock())
        handle.assert_not_called()

    @patch('message_tagging_service.consumer.tagging_service.handle')
    @patch('requests.get')
    def test_mts_should_log_and_dont_terminate_handle_function(self, get, handle):
        with open(os.path.join(test_data_dir, 'mts-test-rules.yaml'), 'r') as f:
            rules_content = f.read()
        get.return_value.text = rules_content

        # Set any error to make handle function fail
        handle.side_effect = IndexError

        msg = fedora_messaging.api.Message(body={
            'name': 'modulea',
            'stream': '10',
            'version': '20200107111030',
            'context': 'c1',
            'state_name': 'ready',
        })

        with patch.object(consumer, 'logger') as logger:
            consumer.consume(msg)

            args, _ = logger.exception.call_args
            assert "Failed to handle message" in args[0]
            args, _ = logger.info.call_args
            assert 'Continue to handle next MBS message ...' == args[0]

    def test_ignore_scratch_build(self):
        msg = fedora_messaging.api.Message(body={'name': 'modulea', 'scratch': True})

        with patch.object(consumer, 'logger') as logger:
            consumer.consume(msg)
            args, _ = logger.warning.call_args
            assert 'Ignore scratch build' in args[0]

    def test_ignore_filtered_out_by_states(self):
        msg = fedora_messaging.api.Message(body={'name': 'modulea',
                                                 'state_name': 'init'})

        with patch.object(consumer, 'logger') as logger:
            consumer.consume(msg)
            args, _ = logger.warning.call_args
            assert 'The message with build_state:' in args[0]
