
# Exercise: Tracking Event Processing

This exercise is about processing a stream of events in real-time. It should not take more than 2 hours. Start with the provided skeleton code, or write your own.

Tradedoubler track users (devices) online to determine which web site contributed to a particular conversion (sale) in a web shop. Thousands of these events are generated every second, and they need to be processed in real-time.

The event types concerned here are:

* __Impression__: Ad is shown on a web site.
* __Click__: User clicked on ad.
* __Conversion__: User bought something in web shop.

Web sites get paid when they refer a user to a web shop and a purshase has been made. This is called performance marketing, i.e. the web shop only pays for conversions, not for impressions or clicks. 

The most common commission rule is that the site with the last click will get 100% of the commission. This is called last-click. If there is no last-click, the last impression will get the commission. This is called last-imp.

## Task

Your task is to process `TrackingEvent` messages from STDIN, in delimited (varint) protobuf format. 

The protobuf messages are defined in the file [src/main/proto/trackevt.proto](src/main/proto/trackevt.proto).

For each _conversion_ tracking event received, find the previous most recent _click_ or _impression_ (if no click) event for the same `device_id` (user). Tracking events with no prior click or impression should be ignored. 

Write each found click-conversion or impression-conversion as `TrackingResult` messages to STDOUT in delimited (varint) protobuf format.

__Restrictions:__

* The same click/impression event may not be used as referrer more than once.
* Only click/impression events within the specified `timeWindow` (set to 12 hours) are eligible to be a referrer.

__Considerations:__

Write the code as you would in a professional environment. E.g. code quality, testing, etc.

## How to Build and Run 
Go to folder trackevt-task and run below command
```yaml
mvn compile exec:java -D"exec.mainClass"="com.tradedoubler.exercise.trackevt.TrackEvtApp" -Dexec.args="-inputPath /home/user/tev/input -outputPath /home/user/tev/outpu"
```
## Runtime env
```inputPath``` : The address where the `TrackingEvent` are located (you can set your custom address , default path is `trackevt-task/input`)

```outputPath``` : The address where the `TrackingResult` are located (you can set your custom address , default path is `trackevt-task/output`)
 

## How is working


* When the application starting , service `EventSourceTrackFunction` watches folder `inputPath` to get the new `TrackingEvent`  

* `EventSourceTrackFunction` ignores past `TrackingEvent` that pushed to folder `inputPath` before application started.

* `EventProcessorFunction`  processes the new `TrackingEvent` that coming to service and generate the `TrackingResult`.

* `EventSinkFunction` creates the file `TrackingResult` on folder `outputPath` . 





