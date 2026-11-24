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
# Authors: Chenxiong Qi <cqi@redhat.com>

import pytest

from message_tagging_service import messaging
from mock import patch, Mock


class TestMessaging(object):

    @patch.object(messaging.conf, 'messaging_backend', new='fedora-messaging')
    @patch('fedora_messaging.api.publish')
    @patch('fedora_messaging.message.Message')
    def test_send_via_fedora_messaging(self, Message, publish):
        messaging.publish('build.tagged', {'build_id': 1})

        Message.assert_called_once_with(topic='build.tagged', body={'build_id': 1})

        outgoing_msg = Message.return_value
        publish.assert_called_once_with(outgoing_msg)

    @patch.object(messaging.conf, 'messaging_backend', new='kafka')
    @patch.object(messaging.conf, 'dry_run', new=False)
    @patch.object(messaging.conf, 'kafka_bootstrap_servers',
                  new=['broker1:9092', 'broker2:9092'])
    @patch.object(messaging.conf, 'kafka_security_protocol', new='SASL_SSL')
    @patch.object(messaging.conf, 'kafka_sasl_mechanism', new='SCRAM-SHA-512')
    @patch.object(messaging.conf, 'kafka_sasl_username', new='mts')
    @patch.object(messaging.conf, 'kafka_sasl_password', new='secret')
    @patch.object(messaging.conf, 'kafka_ssl_cafile', new='/path/to/ca_cert')
    @patch.object(messaging.conf, 'kafka_ssl_certfile', new='')
    @patch.object(messaging.conf, 'kafka_ssl_keyfile', new='')
    @patch.object(messaging.conf, 'kafka_topic_prefix', new='eng.mts')
    @patch.object(messaging.conf, 'kafka_send_timeout', new=30)
    def test_send_via_kafka(self):
        fake_kafka = Mock()
        msg = {'koji_tag': 'module-a-1-1-c1'}

        with patch.dict('sys.modules', {'kafka': fake_kafka}):
            messaging.publish('build.tagged', msg)

        KafkaProducer = fake_kafka.KafkaProducer
        KafkaProducer.assert_called_once()
        _, kwargs = KafkaProducer.call_args
        assert kwargs['bootstrap_servers'] == ['broker1:9092', 'broker2:9092']
        assert kwargs['security_protocol'] == 'SASL_SSL'
        assert kwargs['sasl_mechanism'] == 'SCRAM-SHA-512'
        assert kwargs['sasl_plain_username'] == 'mts'
        assert kwargs['sasl_plain_password'] == 'secret'
        assert kwargs['ssl_cafile'] == '/path/to/ca_cert'
        # Empty auth material is omitted rather than passed through.
        assert 'ssl_certfile' not in kwargs
        assert 'ssl_keyfile' not in kwargs

        producer = KafkaProducer.return_value
        producer.send.assert_called_once_with(
            'eng.mts.build.tagged', value=msg)
        producer.send.return_value.get.assert_called_once_with(timeout=30)
        producer.flush.assert_called_once_with()
        producer.close.assert_called_once_with()

    @patch.object(messaging.conf, 'messaging_backend', new='kafka')
    @patch.object(messaging.conf, 'dry_run', new=True)
    @patch.object(messaging.conf, 'kafka_topic_prefix', new='eng.mts')
    def test_dry_run_does_not_send_via_kafka(self):
        fake_kafka = Mock()

        with patch.dict('sys.modules', {'kafka': fake_kafka}):
            messaging.publish('build.tagged', {'koji_tag': 'module-a-1-1-c1'})

        fake_kafka.KafkaProducer.assert_not_called()

    @patch.object(messaging.conf, 'messaging_backend', new='anothercool')
    def test_no_backend_handler_is_found(self):
        with pytest.raises(KeyError):
            messaging.publish('topic', {})
