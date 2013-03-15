package detach

import (
  "fmt"
  "encoding/json"
  "os"
  "flag"
  "io/ioutil"
  "github.com/streadway/amqp"
  "log"
)

/*
 * This interface needs to be implemented bei every
 * Job that should be processed with detach
 * detach consumer will call the ProcessWork function.
 *
 */
type Job interface {
  ProcessWork( payload []byte ) int
}

type Consumer struct {
  Jobs Job
}

type Config struct {
  AmqpUrl       string
  QueueName     string
  ExchangeName  string
  PrefetchCount int
  JobName       string
  MongoUrl      string
  MongoDb       string
  MongoColl     string
}

/* 
 * Should Return a Consumer
 * Consumer should then be used to react on messages 
 */
//func (cs *Consumer) Initialize() {
func Initialize( jobs map[string]Job ) {
  var config Config
  readConfig(&config)
  // Connect to Amqp
  conn, err := amqp.Dial(config.AmqpUrl)
  if err != nil {
    fmt.Println("connection.open: %s", err)
  }
  defer conn.Close()

  channel, err := conn.Channel()
  if err != nil {
    fmt.Println("channel.open: %s", err)
  }
  qoserr := channel.Qos(config.PrefetchCount, 0, false)
  if qoserr != nil {
    fmt.Println("channel.prefetch %s", qoserr)
  }

  deliveries, err := channel.Consume(
    config.QueueName, // name
    "erster", // consumerTag,
    false, // noAck
    false, // exclusive
    false, // noLocal
    false, // noWait
    nil, // arguments
  )
  if err != nil {
    fmt.Printf("Queue Consume: %s", err)
  }

  go handle(deliveries, make(chan error), jobs[config.JobName])

  select {}
}

func handle(deliveries <-chan amqp.Delivery, done chan error, j Job) {
  for d := range deliveries {
    Consume(j, d.Body)
    d.Ack(false)
  }
  log.Printf("handle: deliveries channel closed")
  done <- nil
}

/*
 * Should be executed in a goroutine on every incoming 
 * rabbitmq message call.
 */
func Consume(j Job, payload []byte) {
  // Job is implemented in your own package and
  // will just be executed here.
  j.ProcessWork( payload )
}

func readConfig(config *Config) {
  var configFile string
  flag.StringVar(&configFile, "config", "detach.json", "Please provide a config File.")
  flag.Parse()
  // Reads the File and returns its bytes, which can be turned into a struct
  file, e := ioutil.ReadFile(configFile)
  if e != nil {
    fmt.Printf("File : %v\n", e)
    os.Exit(1)
  }
  json.Unmarshal(file, &config)
}





