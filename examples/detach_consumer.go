package main

import (
  "go-detach/detach"
  "fmt"
)

type JobPerformance struct {
   browser, http, ip int
}

func (jp JobPerformance) ProcessWork() int {
   return jp.browser + jp.http
}


func main() {
  fmt.Println("detach consumer")

  r := JobPerformance{browser:5, http:3}
  var s detach.Job
  s = r
  detach.Consume(s)

}