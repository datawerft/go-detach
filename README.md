go-detach
=========
This is work in progress. Please refer to the comments in the code until I have completed the Readme.

Overview
--------
Consumer and Scheduler System working with RabbitMQ written in golang. While you can use the scheduler as a standalone process you need to extend the go-detach consumers and add Jobs to it.

Scheduler
---------
The Scheduler runs every Minute and uses MongoDB for it's Scheduling Documents.

Consumer
--------

Links
-----
Uses the excellent AMQP Library github.com/streadway/amqp and for Mongo labix.org/v2/mgo