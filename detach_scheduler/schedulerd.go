package main

import (
  "go-detach/detach"
  "fmt"
)

func main() {
  fmt.Println("detach scheduler")

  detach.Scheduler()
}