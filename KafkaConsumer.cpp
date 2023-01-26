#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <getopt.h>
#include <fstream>
#include "rdkafkacpp.h"
using namespace std;
static volatile sig_atomic_t run = 1;
static bool exit_eof = false;
static fstream fio;
static void setLogFile() {
   fio.open("./kafka.log", ios::trunc | ios::out | ios::in);
}

static void sigterm(int sig) {
  run = 0;
}
class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb(RdKafka::Event &event) {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      if (event.fatal()) {
        fio << "FATAL " << endl;
        run = 0;
      }
      fio << "ERROR (" << RdKafka::err2str(event.err())
                << "): " << event.str() << endl;
      break;

    case RdKafka::Event::EVENT_STATS:
      fio << "\"STATS\": " << event.str() << endl;
      break;

    case RdKafka::Event::EVENT_LOG:
      fio << "\"Severity:\": " << event.severity()
	      << "\"Fac:\": " << event.fac().c_str()
		  << "\"Event:\": " << event.str().c_str()
	      << endl;
      break;
    default:
      fio << "EVENT " << event.type() << " ("
                << RdKafka::err2str(event.err()) << "): " << event.str()
                << endl;
      break;
    }
  }
};


void msg_consume(RdKafka::Message *message, void *opaque) {
  const RdKafka::Headers *headers;

  switch (message->err()) {
  case RdKafka::ERR__TIMED_OUT:
    break;
  case RdKafka::ERR_NO_ERROR:
    cout << "Read msg at offset " << message->offset() << endl;
    if (message->key()) {
      cout << "Key: " << *message->key() << endl;
    }
    headers = message->headers();
    if (headers) {
      vector<RdKafka::Headers::Header> hdrs = headers->get_all();
      for (size_t i = 0; i < hdrs.size(); i++) {
        const RdKafka::Headers::Header hdr = hdrs[i];
        if (hdr.value() != NULL)
          printf(" Header: %s = \"%.*s\"\n", hdr.key().c_str(),
                 (int)hdr.value_size(), (const char *)hdr.value());
        else
          printf(" Header:  %s = NULL\n", hdr.key().c_str());
      }
    }
    printf("%.*s\n", static_cast<int>(message->len()),
           static_cast<const char *>(message->payload()));
    break;

  case RdKafka::ERR__PARTITION_EOF:
    if (exit_eof) {
      run = 0;
    }
    break;
  case RdKafka::ERR__UNKNOWN_TOPIC:
  case RdKafka::ERR__UNKNOWN_PARTITION:
    cerr << "Consume failed: " << message->errstr() << endl;
    run = 0;
    break;

  default:
    cerr << "Consume failed: " << message->errstr() << endl;
    run = 0;
  }
}
class ExampleSocketCb : public RdKafka::SocketCb{
  public:
	  int socket_cb(int domain,int type,int protocol) {
		  cout << "Socket Open (domain:" << domain
		            << " Type:" << type << " Protocol: " << protocol
		            << endl;
		  return 0;
	  }
};
class ExampleConsumeCb : public RdKafka::ConsumeCb {
 public:
  void consume_cb(RdKafka::Message &msg, void *opaque) {
    msg_consume(&msg, opaque);
  }
};
class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message) {
    string status_name;
    switch (message.status()) {
    case RdKafka::Message::MSG_STATUS_NOT_PERSISTED:
      status_name = "NotPersisted";
      break;
    case RdKafka::Message::MSG_STATUS_POSSIBLY_PERSISTED:
      status_name = "PossiblyPersisted";
      break;
    case RdKafka::Message::MSG_STATUS_PERSISTED:
      status_name = "Persisted";
      break;
    default:
      status_name = "Unknown?";
      break;
    }
    cout << "Message delivery for (" << message.len()
              << " bytes): " << status_name << ": " << message.errstr()
              << endl;
    if (message.key())
      cout << "Key: " << *(message.key()) << ";" << endl;
  }
};

int main(int argc, char *argv[]) {
  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);
  setLogFile();
  std::string brokers = "127.0.0.1";
  std::string errstr;
  std::string topic_str = "paytm_topic";
  std::string mode;
  std::string debug = "all";
  int32_t partition    = RdKafka::Topic::PARTITION_UA;
  int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
  RdKafka::Conf *conf  = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  conf->set("metadata.broker.list", brokers, errstr);
  conf->set("socket.timeout.ms","3000",errstr);
  conf->set("socket.send.buffer.bytes","100000000",errstr);
  conf->set("socket.receive.buffer.bytes","100000000",errstr);
  conf->set("socket.keepalive.enable","true",errstr);
  conf->set("socket.nagle.disable","true",errstr);
  ExampleConsumeCb ex_socket_cb;
  conf->set("socket_cb", &ex_socket_cb, errstr);
  ExampleEventCb ex_event_cb;
  conf->set("event_cb", &ex_event_cb, errstr);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  ExampleDeliveryReportCb ex_dr_cb;
  conf->set("dr_cb", &ex_dr_cb, errstr);
  conf->set("default_topic_conf", tconf, errstr);
  if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
	  cerr << errstr << endl;
	  exit(1);
  }
  RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
  if (!consumer) {
	cerr << "Failed to create consumer: " << errstr << endl;
	exit(1);
  }
  cout << "% Created consumer " << consumer->name() << endl;
  RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topic_str, tconf, errstr);
  if (!topic) {
	cerr << "Failed to create topic: " << errstr << endl;
	exit(1);
  }
  RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
  if (resp != RdKafka::ERR_NO_ERROR) {
	cerr << "Failed to start consumer: " << RdKafka::err2str(resp)
			  << endl;
	exit(1);
  }else{
	  RdKafka::Message *msg = consumer->consume(topic, partition, 1000);
	  msg_consume(msg, NULL);
	  delete msg;
  }
  return 0;
}


