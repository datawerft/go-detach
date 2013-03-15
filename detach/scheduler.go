package detach

import (
  "fmt"
  "time"
  "github.com/streadway/amqp"
  "labix.org/v2/mgo"
  "labix.org/v2/mgo/bson"
  "log"
)

type Schedule struct {
  Queue   string
  Cron    string
  Payload string
}

func Scheduler() {
  var config Config
  readConfig(&config)

  session, err := mgo.Dial(config.MongoUrl)
  if err != nil { 
    panic(err)
  }
  defer session.Close()
  
  c := session.DB(config.MongoDb).C(config.MongoColl)
  
  // Setup amqp Connecion  
  conn, err := amqp.Dial(config.AmqpUrl)
  if err != nil {
    fmt.Errorf("Dial: %s", err)
  }
  defer conn.Close()

  log.Printf("got Connection, getting Channel")
  channel, err := conn.Channel()
  if err != nil {
    fmt.Errorf("Channel: %s", err)
  }

  sleepToFullMinute()

  for {
    checkTime := time.Now()
    findSchedulesAndPublish(*c, *channel, checkTime)
    duration := time.Now().Sub(checkTime)
    time.Sleep( (60 * time.Second) - duration )
  }

}

/*
 * Get the time in sync
 * check the seconds and sleep to the next full minute
 */
func sleepToFullMinute() {
  startTime := time.Now()
  sleep := 60 - startTime.Second()

  time.Sleep( time.Duration(sleep) * time.Second )
}

/*
 *
 */
func findSchedulesAndPublish(c mgo.Collection, channel amqp.Channel, checkTime time.Time) {
  // Define a result slice to use for the mongodb answer
  results := []Schedule{}
  fmt.Println( getCronStrings(checkTime) )
  err := c.Find(bson.M{ "cron": bson.M{"$in": getCronStrings(checkTime) } } ).All(&results)

  if err != nil { panic(err) }

  for _,schedule := range results {
    SendMessage(channel, schedule.Queue, schedule.Payload)
  }
}

/* m h d m w Description
 * 1 - - - - every minute (1,5,15,30)
 * - 1 - - - every hour (1,2,3,4,6,12)
 * 0 7 - - - every day at a specific time 07:00
 * 0 7 1 - - every first of the month time 07:00
 * 0 7 1 2 - at 01.02. 07:00
 * 0 7 - - 1 every first weekday at 07:00
 *         S M T W T F S 
 * Weekday 0 1 2 3 4 5 6
 * More possibilities will follow
 */
func getCronStrings(checkTime time.Time) []string {
  crons := []string{}
  
  for _,minute := range []int{1, 2, 3, 4, 5, 10, 15, 20, 30} {
    if checkTime.Minute() % minute == 0 {
      crons = append(crons, fmt.Sprintf("%v - - - -", minute) )
    }
  }
  for _,hour := range []int{1, 2, 3, 4, 6, 12} {
    if (checkTime.Hour() % hour == 0) && (checkTime.Minute() == 0) {
      crons = append(crons, fmt.Sprintf("- %v - - -", hour) )
    }
  }
  crons = append(crons, fmt.Sprintf("%v %v - - -", checkTime.Minute(), checkTime.Hour()) )
  crons = append(crons, fmt.Sprintf("%v %v %v - -", checkTime.Minute(), checkTime.Hour(), checkTime.Day()) )
  crons = append(crons, fmt.Sprintf("%v %v %v %v -", checkTime.Minute(), checkTime.Hour(), checkTime.Day(), int(checkTime.Month()) ) )
  crons = append(crons, fmt.Sprintf("%v %v - - %v", checkTime.Minute(), checkTime.Hour(), int(checkTime.Weekday()) ) )

  return crons
}

/*
 * Send a Message to a specified Queue with a payload string
 */
func SendMessage(channel amqp.Channel, queueName string, payload string) {

  if err := channel.Publish("detach.topic", queueName, false, false,
    amqp.Publishing{
      Headers:         amqp.Table{},
      ContentType:     "text/plain",
      ContentEncoding: "",
      Body:            []byte( payload ),
      DeliveryMode:    1,
      Priority:        0,
    },
  ); err != nil {
    fmt.Errorf("Queue Publish: %s", err)
  }

}
