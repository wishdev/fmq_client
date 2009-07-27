#
# Copyright (c) 2008 Vincent Landgraf
# Copyright (c) 2009 John W Higgins
#
# This file is part of the Free Message Queue.
#
# Free Message Queue is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Free Message Queue is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Free Message Queue.  If not, see <http://www.gnu.org/licenses/>.
#
require 'net/http'

module FreeMessageQueue
  # This is the default message implementation in the queue system
  class Message
    attr_accessor :next, # reference to next Message if there is one
      :payload, # the content itself
      :created_at, # when came the message into the system
      :content_type, # the content type of the message
      :option, # options hash (META-DATA) for the message
      :valid #signifies if message was actually returned

    # Create a message item. The payload is often just a string
    def initialize(payload, content_type = "text/plain", created_at = Time.new)
      @payload = payload
      @created_at = created_at
      @content_type = content_type
      @option = {}
      if block_given? then
        yield self
      end
    end

    # Size of item in bytes
    def bytes
      @payload.size
    end
  end

  class BaseClient
    # create a connection to a queue (by url)
    def initialize(url)
      @url = URI.parse(url)
    end

    def handle_poll(res)
      message = Message.new(res.body, res["CONTENT-TYPE"])

      message.valid = (res.status == 200)
      res.each_key do |option_name|
        if option_name.upcase.match(/MESSAGE_([a-zA-Z][a-zA-Z0-9_\-]*)/)
          message.option[$1] = res[option_name]
        end
      end

      message
    end

    def put_headers(message)
      header = {}
      header["CONTENT-TYPE"] = message.content_type

      # send all options of the message back to the client
      if message.respond_to?(:option) && message.option.size > 0
        for option_name in message.option.keys
          header["MESSAGE_#{option_name}"] = message.option[option_name].to_s
        end
      end

      header
    end

    # return the size (number of messages) of the queue
    def size
      head["QUEUE_SIZE"].to_i
    end

    # return the size of the queue in bytes
    def bytes
      head["QUEUE_BYTES"].to_i
    end
  end

  # Here you can find the client side api for the free message queue.
  # This api is build using the net/http facilitys
  #
  # Some sample usage of the client api:
  #
  #  require "fmq"
  #
  #  # queue adress
  #  QA = "http://localhost/webserver_agent/messages"
  #  queue = FreeMessageQueue::ClientQueue.new(QA)
  #
  #  # pick one message
  #  message = queue.poll()
  #  puts " == URGENT MESSSAGE == " if message.option["Priority"] == "high"
  #  puts message.payload
  #
  #  # put an urgent message on the queue e.g.in yaml
  #  payload = "message:
  #    title: server don't answer a ping request
  #    date_time: 2008-06-01 20:19:28
  #    server: 10.10.30.62
  #  "
  #
  #  message = FreeMessageQueue::Message.new(payload, "application/yaml")
  #  queue.put(message)
  #

  class ClientQueue < BaseClient
    # this returns one message from the queue as a string
    def poll()
      res = Net::HTTP.start(@url.host, @url.port) do |http|
        http.get(@url.path)
      end

      handle_poll(res)
    end

    alias get poll

    # this puts one message to the queue
    def put(message)
      Net::HTTP.start(@url.host, @url.port) do |http|
        http.post(@url.path, message.payload, put_headers(message))
      end
    end

    alias post put

  protected
    # do a head request to get the state of the queue
    def head()
      res = Net::HTTP.start(@url.host, @url.port) do |http|
        http.head(@url.path)
      end
    end
  end
end


begin
  require 'patron'

  module Patron
    class Response
      #adding [] method to ease compatability
      def [](key)
        @headers[key]
      end

      def each_key(&block)
        @headers.each_key &block
      end
    end
  end

  module FreeMessageQueue
    class PatronClientQueue < BaseClient
      # create a connection to a queue (by url)
      def initialize(url)
        @sess = Patron::Session.new
        @sess.base_url = url
      end

      # this returns one message from the queue as a string
      def poll(path = '')
        handle_poll(@sess.get(path))
      end

      alias get poll

      # this puts one message to the queue
      def put(message, path = '')
        @sess.post(path, message.payload, put_headers(message))
      end

      alias post put

      protected
      # do a head request to get the state of the queue
      def head(path = '')
        sess.head(path)
      end
    end
  rescue LoadError => e
    module FreeMessageQueue
      class PatronClient
      end
    end
  end
end
