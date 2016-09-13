# encoding: utf-8

require 'kafka/clients/ext/kafka_clients.jar'

Java::IoBurtKafkaClients::KafkaClientsLibrary.new.load(JRuby.runtime, false)

require 'kafka/clients/version'
