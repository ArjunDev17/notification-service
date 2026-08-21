
Absolutely. Event Contract is one of the most important concepts to understand before writing Kafka code.
Let's forget Kafka for a moment and understand the idea from a real backend-system perspective.
1. What is an Event?
An event is a record that says:
"Something already happened."
For your Course Content Service:
Course was created
That becomes an event:
CourseCreated
Other examples:
StudentRegistered
CourseUpdated
CourseDeleted
StudentEnrolled
PaymentCompleted
PaymentFailed
NotificationSent
The important thing is the past tense.
Compare:
CreateCourse
This sounds like a command:
"Please create a course."
Whereas:
CourseCreated
means:
"The course has already been created."
That's a fundamental distinction.

2. Then What is an Event Contract?
An Event Contract is the agreed-upon structure, meaning, and rules of an event that a producer and its consumers use to communicate reliably.
In simple words:
An Event Contract is an agreement between the service producing an event and the services consuming it about what the event means and what data it contains.
Think of it like an API contract.
For REST:
POST /courses
might have a request contract:
{
  "title": "Master Golang",
  "category": "Backend"
}
The caller needs to know:
What fields exist?
What are their types?
Which fields are required?
What does each field mean?
Kafka events have the same requirement.

3. Your Example
Suppose your Course Service creates this course:
Course ID: 101

Title: Master Golang

Category: Backend

Instructor: Arjun
Your service publishes:
CourseCreated
The event could look like:
{
  "eventId": "abc-123",
  "eventType": "course.created",
  "occurredAt": "2026-08-19T10:00:00Z",
  "courseId": "101",
  "title": "Master Golang",
  "category": "Backend",
  "instructor": "Arjun"
}
This JSON is not just random data.
It is a contract.
The producer is effectively saying:
"Whenever I publish course.created, I guarantee that this event follows this structure and these fields have these meanings."

4. Why Do We Need a Contract?
Imagine you have three consumers.
                   Kafka
                      |
             course-created
                      |
       +--------------+--------------+
       |              |              |
       ↓              ↓              ↓
 Notification      Search        Analytics
 Service           Service        Service
Notification Service expects:
{
  "courseId": "101",
  "title": "Master Golang"
}
Search Service expects:
{
  "courseId": "101",
  "title": "Master Golang",
  "category": "Backend"
}
Analytics expects:
{
  "courseId": "101",
  "category": "Backend"
}
All three services are depending on the same event.
Therefore, we need an agreement.
That's the Event Contract.

5. What Happens Without a Contract?
Suppose your developer changes:
{
  "courseId": "101",
  "title": "Master Golang"
}
to:
{
  "id": "101",
  "courseName": "Master Golang"
}
The producer thinks:
"It's just a small change."
But Notification Service might be doing:
event.CourseID
Now it breaks.
Search Service might be doing:
event.Title
Now it breaks.
So a tiny change in one service can break multiple independent services.
This is exactly what Event Contracts help prevent.

6. Event Contract vs Database Model
This is very important.
Suppose your MongoDB model is:
type Course struct {
    ID             string
    Title          string
    Description    string
    InstructorID   string
    InternalStatus string
    CreatedBy      string
    UpdatedBy      string
    Version        int
    InternalNotes  string
}
You should not automatically publish this entire object.
Don't do:
json.Marshal(course)
and send your database model to Kafka.
Instead create a separate event model:
type CourseCreatedEvent struct {
    EventID    string    `json:"eventId"`
    EventType  string    `json:"eventType"`
    OccurredAt time.Time `json:"occurredAt"`

    CourseID   string `json:"courseId"`
    Title      string `json:"title"`
    Category   string `json:"category"`
    Instructor string `json:"instructor"`
}
Why?
Because:
Database Model
      ≠
Event Contract
They have different responsibilities.

7. Database Model
The database model represents:
How my service stores data.
For example:
MongoDB
   ↓
Course document
It is an internal implementation detail.
You are free to change it.

8. Event Contract
The event contract represents:
How my service communicates with other services.
This is an external interface.
You should change it carefully.
Think about REST.
You wouldn't randomly change:
GET /courses/{id}
from:
{
  "id": "101",
  "title": "Golang"
}
to:
{
  "courseIdentifier": "101",
  "courseName": "Golang"
}
without considering every client.
Kafka is the same.

9. Event Contract Has More Than Just JSON
This is where I want you to think deeper.
An Event Contract isn't simply:
{
   "courseId": "101"
}
It includes several things.
1. Event name
course.created
2. Schema
courseId → string
title → string
category → string
3. Meaning
What exactly does course.created mean?
For example:
A course has successfully been persisted and is available for downstream processing.
4. Required fields
For example:
eventId       required
eventType     required
occurredAt    required
courseId      required
title         required
5. Optional fields
description
thumbnailUrl
instructorName
may be optional.
6. Version
Eventually you might have:
course.created.v1
course.created.v2
or a schema version:
{
  "schemaVersion": 2
}
7. Compatibility rules
You need rules for how the event can evolve without breaking consumers.
This becomes extremely important in production Kafka systems.

10. Real-Life Analogy
Think about a courier package.
You send a package from:
Course Service
to:
Notification Service
The package has a label:
Event Type:
CourseCreated

Course ID:
101

Title:
Master Golang
The delivery company doesn't care how you store the course internally.
Maybe you use:
MongoDB
Maybe tomorrow you switch to:
PostgreSQL
Kafka doesn't care.
The receiver only cares about the package format.
That package format is your Event Contract.

11. Event Contract in Your Architecture
Your system will eventually look like:
                   Course Service
                         |
                         |
                  CourseCreated
                         |
                         ↓
                  +-------------+
                  |    Kafka    |
                  +-------------+
                         |
                course-created
                         |
          +--------------+--------------+
          |              |              |
          ↓              ↓              ↓
   Notification      Search         Analytics
      Service         Service          Service
All three consumers understand:
course.created
according to the same contract.
That's what makes event-driven architecture manageable.

12. Very Important: Producer Owns the Contract
Your Course Service is the producer.
Therefore:
Course Service
      |
      | owns
      ↓
CourseCreated Event Contract
Consumers shouldn't redefine what the event means.
For example:
Notification Service shouldn't decide:
"course.created means the course was published."
while Search Service thinks:
"course.created means someone started creating the course."
There must be one clear meaning.

13. Event Contract vs API Contract
This comparison will help you remember it.
REST API
Kafka
API Contract
Event Contract
Endpoint
Topic/Event
Request
Event
Response
Event/result
Client
Consumer
API Provider
Event Producer
JSON schema
Event schema
API version
Event/schema version

For example:
REST:
POST /courses
Contract:
{
  "title": "Master Golang"
}
Kafka:
course.created
Contract:
{
  "eventId": "...",
  "eventType": "course.created",
  "occurredAt": "...",
  "courseId": "101",
  "title": "Master Golang"
}

14. One More Important Concept
There are actually two things here:
Event
The actual message:
{
  "eventType": "course.created",
  "courseId": "101",
  "title": "Master Golang"
}
Event Contract
The rules describing that message:
Event name: course.created

courseId:
    type: string
    required: true

title:
    type: string
    required: true

occurredAt:
    type: timestamp
    required: true
So:
Event = the message.
Event Contract = the agreement/rules governing the message.

15. Why This Matters for Our Kafka Project
We're going to eventually have:
course.created
course.updated
course.deleted
course.published
Then:
student.enrolled
student.completed-course
student.subscribed
Then:
notification.requested
notification.sent
notification.failed
If we don't establish good event contracts now, the system will become messy very quickly.
So before we write the Kafka Producer, I want you to understand one more concept next:
Event vs Command vs Message
These three terms are often confused:
Command
   ↓
"Create this course"

Event
   ↓
"Course was created"

Message
   ↓
Generic term for data sent between systems
Once you understand that distinction, Kafka Producer → Topic → Consumer will become much easier to understand.



Great. Now that Event Contract is clear, the next thing we should learn is Event vs Command vs Message, because this distinction is foundational for Kafka and event-driven architecture.
Phase 1 — Step 2: Event vs Command vs Message
Let's use your Course Content Service.
Suppose a student/admin wants to create a course.
1. Command
A command is an instruction:
"Please do this."
For example:
CreateCourse
{
  "title": "Master Golang",
  "category": "Backend",
  "instructorId": "101"
}
The sender is asking another service to perform an action.
Client
   |
   | CreateCourse
   v
Course Service
   |
   | creates course
   v
MongoDB
A command is generally:
REQUEST / INSTRUCTION

2. Event
An event says:
"This already happened."
After the course is successfully created:
CourseCreated
{
  "eventId": "evt-123",
  "eventType": "course.created",
  "courseId": "101",
  "title": "Master Golang"
}
Now the flow is:
Client
   |
   | CreateCourse
   v
Course Service
   |
   | Save
   v
MongoDB
   |
   | SUCCESS
   v
CourseCreated Event
   |
   v
Kafka
Notice the difference:
CreateCourse
     ↑
   COMMAND
     |
"Please create this"

-------------------------

CourseCreated
     ↑
    EVENT
     |
"It has been created"

3. Message
Message is the generic term.
A message is simply:
Data sent from one component/system to another.
For example:
Producer
   |
   | Message
   v
Kafka
That message could contain:
Command
or
Event
or some other data.
So:
Message
├── Command
├── Event
└── Other data
Don't get too hung up on the terminology yet—the important distinction is command vs event.

4. Your Course Example
Imagine:
Admin
 |
 | "Create a Go course"
 v
Course Service
That's a command:
CreateCourse
Course Service processes it.
Course Service
      |
      v
   MongoDB
      |
      | successfully created
      v
CourseCreated
      |
      v
    Kafka
Now Kafka consumers can react:
                CourseCreated
                       |
                      Kafka
                       |
       +---------------+---------------+
       |               |               |
       v               v               v
 Notification       Search          Analytics
 Service            Service          Service
The consumers don't say:
"Create the course."
They say:
"A course was created. I'll react to that."
That's the heart of event-driven architecture.

5. Why This Is Powerful
Suppose today you have:
Course Service
      |
      v
Notification Service
Tomorrow you add:
Search Service
Analytics Service
Recommendation Service
Audit Service
Email Service
Without Kafka:
Course Service
   |
   +----> Notification
   |
   +----> Search
   |
   +----> Analytics
   |
   +----> Recommendation
   |
   +----> Audit
   |
   +----> Email
Course Service becomes tightly coupled to everything.
With Kafka:
                    Course Service
                           |
                           |
                    CourseCreated
                           |
                           v
                        Kafka
                           |
       +---------+---------+---------+---------+
       |         |         |         |         |
       v         v         v         v         v
    Notify    Search   Analytics   Audit   Recommendation
Course Service only knows:
"I published CourseCreated."
It doesn't need to know who consumes it.
This is loose coupling.

6. Now Let's Connect This to Kafka
Our Phase 1 architecture becomes:
                COURSE CONTENT SERVICE
                         |
                         |
                    Create Course
                         |
                         v
                      MongoDB
                         |
                    successful
                         |
                         v
                  CourseCreated Event
                         |
                         v
                +-------------------+
                |       Kafka       |
                +-------------------+
                         |
                   course-created
                       TOPIC
There are four concepts here that I want you to understand very clearly before we write code:
Producer
Who sends the message?
Course Content Service
Topic
Where is the event published?
course-created
Broker
Which Kafka server stores the event?
Kafka Broker
Consumer
Who reads the event?
We'll build this in Phase 2:
Notification Service

7. Our Learning Sequence From Here
I recommend we go in this order:
             PHASE 1

        Event Contract ✅
                |
                ↓
      Command vs Event ✅
                |
                ↓
       Kafka Architecture
                |
                ↓
        Broker / Topic
                |
                ↓
           Partition
                |
                ↓
            Producer
                |
                ↓
       Publish CourseCreated
                |
                ↓
          Verify Message
                |
                ↓
              DONE
Then:
             PHASE 2

           Consumer
              |
              ↓
       Consumer Group
              |
              ↓
       Notification Service
              |
              ↓
       Subscriber handling
Then we'll go much deeper:
Partitions
      ↓
Offsets
      ↓
Ordering
      ↓
Consumer Groups
      ↓
Rebalancing
      ↓
Retries
      ↓
DLT
      ↓
Idempotency
      ↓
Exactly Once
      ↓
Transactions
      ↓
Schema Registry
      ↓
Kafka Streams
      ↓
CDC / Debezium
      ↓
Production Architecture
So our immediate next topic should be:
Kafka Architecture — Broker, Topic, Partition, Offset
This is the most important foundation.
Once you understand those four, the Producer and Consumer code will make much more sense.



Phase 1 — Step 3: Kafka Architecture
Now let's understand the 4 things you must know before writing Kafka code:
Kafka
 └── Broker
      └── Topic
           └── Partition
                └── Offset
I'll explain these using our Course Content Service.

1. What is Kafka?
At a high level, Kafka is a distributed event streaming platform.
For our project, think of Kafka as a system that allows:
One service to publish events and other services to consume those events asynchronously.
Our flow:
Course Content Service
        |
        | CourseCreated
        ↓
      Kafka
        |
        +--------→ Notification Service
        |
        +--------→ Search Service
        |
        +--------→ Analytics Service
But Kafka isn't just one big queue.
Inside Kafka there are several important concepts.

2. Broker
A Kafka Broker is a Kafka server.
For example:
Kafka Cluster

+----------------+
| Broker 1       |
| localhost:9092 |
+----------------+
If you run Kafka locally with Docker, you might initially have:
Kafka Cluster
      |
      +---- Broker 1
In production, you might have:
Kafka Cluster

+----------+   +----------+   +----------+
| Broker 1 |   | Broker 2 |   | Broker 3 |
+----------+   +----------+   +----------+
These brokers work together as a Kafka cluster.

3. Why Multiple Brokers?
Imagine we have only one broker:
Course Service
      |
      v
   Broker 1
If Broker 1 dies:
Course Service
      |
      X
   Broker 1 💥
Kafka isn't available.
Not good for production.
With multiple brokers:
             Kafka Cluster

       +----------+
       | Broker 1 |
       +----------+
            |
       +----------+
       | Broker 2 |
       +----------+
            |
       +----------+
       | Broker 3 |
       +----------+
Kafka can distribute data across them.
This gives us availability and scalability.
We'll go much deeper into replication later.
For now:
Broker = Kafka server.
Remember that.

4. Topic
Now imagine Course Service produces:
CourseCreated
Where does it go?
Into a Topic.
A topic is a named stream/category of events.
For our project:
course-created
So:
Course Service
      |
      | CourseCreated
      ↓
+-------------------+
| course-created    |
|      TOPIC        |
+-------------------+
You can think of a topic as:
A named place where Kafka stores related events.

5. Multiple Topics
Our application might eventually have:
course-created
course-updated
course-deleted

student-registered
student-enrolled

notification-requested
notification-sent
notification-failed
So Kafka could look like:
Kafka

├── course-created
├── course-updated
├── course-deleted
├── student-registered
├── student-enrolled
├── notification-requested
├── notification-sent
└── notification-failed
Each topic represents a stream of related events.

6. Important: Topic Is Not a Queue
This is one of the most common beginner mistakes.
People often think:
Topic = Queue
Not exactly.
A Kafka topic is more like an append-only log.
For example:
course-created

Event 1
Event 2
Event 3
Event 4
Event 5
Kafka doesn't immediately remove Event 1 after a consumer reads it.
The event remains according to the topic's retention policy.
That's one of Kafka's superpowers.
We'll explore retention later.

7. Partition
Now we reach one of the most important Kafka concepts.
A topic is divided into partitions.
Suppose:
course-created
has 3 partitions.
course-created

Partition 0
----------------
Event A
Event D
Event G


Partition 1
----------------
Event B
Event E
Event H


Partition 2
----------------
Event C
Event F
Event I
So:
Topic
 |
 +-- Partition 0
 |
 +-- Partition 1
 |
 +-- Partition 2

8. Why Partitions?
Scalability and parallel processing.
Suppose we have:
1,000,000 CourseCreated events
One consumer processing everything sequentially could become slow.
Instead:
Partition 0 ──→ Consumer 1
Partition 1 ──→ Consumer 2
Partition 2 ──→ Consumer 3
Now three consumers can process messages in parallel.
This is one of the major reasons Kafka can handle huge workloads.

9. Partition = Ordered Log
This is another extremely important rule.
Kafka guarantees ordering within a partition.
Suppose Partition 0 contains:
Event 1
Event 2
Event 3
Event 4
A consumer reads:
1 → 2 → 3 → 4
The order is maintained.
But across different partitions:
Partition 0       Partition 1

Event A           Event B
Event C           Event D
Event E           Event F
Kafka does not guarantee:
A → B → C → D → E → F
There is no global ordering across partitions.
Remember:
Ordering is guaranteed per partition, not per topic.
This becomes extremely important when we discuss Kafka keys.

10. Offset
Now we need a way to identify messages inside a partition.
That's the offset.
Example:
Partition 0

Offset      Event

0           CourseCreated #1
1           CourseCreated #2
2           CourseCreated #3
3           CourseCreated #4
4           CourseCreated #5
The offset is basically the position of a message inside a partition.
So:
Offset 0
Offset 1
Offset 2
Offset 3
...

11. Why Does Kafka Need Offset?
Imagine Notification Service processes:
Offset 0
Offset 1
Offset 2
Then crashes.
When it comes back:
Notification Service 💥

processed:
0
1
2
Kafka needs to know where the consumer was.
It can resume from:
Offset 3
This is called offset management.
Later we'll learn:
committed offsets
auto commit
manual commit
offset reset
replaying events
These are extremely important production concepts.

12. Put Everything Together
Now our architecture makes more sense:
                 Course Service
                        |
                        |
                 CourseCreated
                        |
                        ↓
              +------------------+
              |      Kafka       |
              |                  |
              | course-created   |
              +------------------+
                 /      |      \
                /       |       \
               ↓        ↓        ↓
          Partition  Partition  Partition
              0          1          2
              |          |          |
              ↓          ↓          ↓
           Broker?    Broker?    Broker?
A slightly more accurate production picture is:
                Kafka Cluster

       +----------+----------+----------+
       | Broker 1 | Broker 2 | Broker 3 |
       +----------+----------+----------+
            |          |          |
            +----------+----------+
                       |
                 course-created
                       |
              +--------+--------+
              |        |        |
              ↓        ↓        ↓
             P0       P1       P2
Partitions are distributed across brokers.

13. Our Course Example
Suppose we create:
Course 101
Course 102
Course 103
Course 104
Course 105
Kafka might store:
course-created

Partition 0
-----------------------
Offset 0 → Course 101
Offset 1 → Course 104

Partition 1
-----------------------
Offset 0 → Course 102
Offset 1 → Course 105

Partition 2
-----------------------
Offset 0 → Course 103
The exact partition assignment depends on the partitioning strategy/key.
We'll learn that next.

14. One Important Question
You might ask:
"If I have 3 partitions, do I need 3 consumers?"
No.
You can have:
3 partitions
1 consumer
The consumer can process all three.
Or:
3 partitions
3 consumers
Each consumer can process one partition.
But:
3 partitions
5 consumers
Within the same consumer group, only 3 consumers can actively consume partitions at that moment.
Two consumers will have nothing assigned.
This is a critical concept we'll cover under Consumer Groups.

15. The Mental Model I Want You to Remember
Don't memorize complicated definitions.
Think:
BROKER
↓
Kafka server

TOPIC
↓
Category/stream of events

PARTITION
↓
Parallel ordered log inside a topic

OFFSET
↓
Position of an event inside a partition
Or even simpler:
Kafka Cluster
     |
   Broker
     |
   Topic
     |
 Partition
     |
  Offset

16. Your Actual Project
For Phase 1, we only need:
Kafka
  |
  └── course-created
          |
      Partition 0
We'll initially keep it simple.
Then we'll deliberately increase complexity:
1 Partition
     ↓
3 Partitions
     ↓
Multiple Consumers
     ↓
Consumer Groups
     ↓
Partition Keys
     ↓
Ordering
     ↓
Scaling
That way you won't just memorize Kafka—you'll see why each feature exists.

Next: Partitions + Kafka Keys 🔥
Before we write the producer, I want you to understand Kafka Keys, because this is where Kafka becomes really interesting.
We'll take:
CourseCreated
CourseUpdated
CourseDeleted
and see why simply having partitions isn't enough when you need to guarantee:
"All events for Course 101 must be processed in the correct order."
That will lead directly into partition key → partition selection → ordering, and then we'll be ready to build our first Kafka producer.


Phase 1 — Step 4: Partitions + Kafka Keys 🔥
This is one of the most important Kafka concepts because partitions are directly connected to:
scalability
parallel processing
ordering
consumer groups
performance
And the Kafka key determines which partition a message goes to.
Let's use your Course Content Service throughout.

1. First: Why Do We Need Partitions?
Suppose your application gets:
1,000,000 CourseCreated events
If Kafka had only one sequential stream:
CourseCreated 1
CourseCreated 2
CourseCreated 3
CourseCreated 4
...
CourseCreated 1,000,000
A single consumer would have to process everything.
That limits throughput.
Instead, Kafka divides the topic into partitions:
course-created

Partition 0
----------------
Event
Event
Event
Event

Partition 1
----------------
Event
Event
Event
Event

Partition 2
----------------
Event
Event
Event
Event
Now work can happen in parallel.

2. Partitions Are Independent Logs
Think of each partition as its own append-only log.
Topic: course-created

Partition 0

0 → Course A
1 → Course D
2 → Course G
3 → Course J
And:
Partition 1

0 → Course B
1 → Course E
2 → Course H
And:
Partition 2

0 → Course C
1 → Course F
2 → Course I
Each partition has its own offsets.
Notice:
Partition 0 → 0, 1, 2, 3
Partition 1 → 0, 1, 2
Partition 2 → 0, 1, 2
Offsets are not global.
This is important.

3. Ordering
Kafka guarantees ordering inside a partition.
Suppose:
Partition 0

Offset 0 → CourseCreated
Offset 1 → CourseUpdated
Offset 2 → CourseDeleted
A consumer will see:
Created
   ↓
Updated
   ↓
Deleted
That's good.
But imagine:
Partition 0

CourseCreated
CourseDeleted
and:
Partition 1

CourseUpdated
Kafka cannot guarantee:
Created
→ Updated
→ Deleted
across those partitions.
Therefore:
If two events must be processed in order, they must go to the same partition.
This is where Kafka keys become extremely important.

4. What Is a Kafka Key?
A Kafka message can contain:
Key
Value
For example:
Key:
course-101

Value:
CourseCreated event
The key helps Kafka decide which partition should receive the message.
Conceptually:
Key
 ↓
Partitioner
 ↓
Partition
For example:
course-101
     ↓
hash()
     ↓
Partition 1
Another:
course-102
     ↓
hash()
     ↓
Partition 2

5. Why Is the Key Important?
Let's take your course lifecycle.
Course 101 generates:
CourseCreated
CourseUpdated
CourseUpdated
CourseDeleted
We want:
CourseCreated
      ↓
CourseUpdated
      ↓
CourseUpdated
      ↓
CourseDeleted
to be processed in that exact order.
So we use:
Key = courseId
For example:
CourseCreated
Key = 101

CourseUpdated
Key = 101

CourseUpdated
Key = 101

CourseDeleted
Key = 101
Kafka's partitioning logic will consistently route the same key to the same partition, assuming the partition count and partitioning setup remain compatible.
Conceptually:
            courseId = 101
                    |
          +---------+---------+
          |                   |
     CourseCreated       CourseUpdated
          |                   |
          +---------+---------+
                    |
                 hash(101)
                    |
                    ↓
              Partition 1
Now ordering is preserved for that course.

6. This Is a Critical Rule
Remember this:
Same key → same partition → ordering within that partition.
For our course system:
Key = courseId
is a natural choice if we need to preserve the order of events for an individual course.

7. Example
Suppose we have:
Course 101
Course 102
Course 103
And three partitions:
Partition 0
Partition 1
Partition 2
Kafka could distribute them like:
course-101 → Partition 1

course-102 → Partition 2

course-103 → Partition 0
Then all events for Course 101:
course-101 → Created
course-101 → Updated
course-101 → Deleted
go to:
Partition 1
While Course 102 might go to:
Partition 2
This allows:
Course 101 processing
          ||
          ↓
    Partition 1

Course 102 processing
          ||
          ↓
    Partition 2
to happen in parallel.

8. This Gives Us Two Things at Once
This is the beautiful part.
Using a key gives us:
Ordering
For the same key:
101 Created
101 Updated
101 Deleted
remains ordered.
Parallelism
Different keys can be distributed:
101 → P0
102 → P1
103 → P2
and processed independently.
So:
Same key
   ↓
Ordering

Different keys
   ↓
Parallelism
That's one of the fundamental design principles of Kafka.

9. What If We Don't Provide a Key?
Kafka can still publish messages.
For example:
Key = null
Kafka's producer can distribute records across partitions using its partitioning behavior.
Conceptually:
Course 101 → P0
Course 102 → P1
Course 103 → P2
Course 104 → P0
Course 105 → P1
This is good when you don't care about ordering for a particular entity.
But consider:
CourseCreated
CourseUpdated
CourseDeleted
If they're distributed unpredictably:
P0 → Created

P1 → Updated

P2 → Deleted
there is no single partition containing the lifecycle of Course 101.
That's dangerous if consumers need ordered processing.

10. Choosing the Right Key
This is actually a system-design decision.
Don't blindly use:
courseId
for every event.
Ask:
What entity must maintain ordering?
Course events
course.created
course.updated
course.deleted
Use:
courseId
Student events
student.registered
student.updated
student.deleted
Use:
studentId
Order events
order.created
order.paid
order.shipped
order.delivered
Use:
orderId
Payment events
payment.initiated
payment.success
payment.failed
Usually:
paymentId
The key should generally represent the entity whose events need ordering.

11. Our Notification Scenario
Now let's return to your original requirement.
A new course is published:
Course 101
We publish:
Topic:
course-created

Key:
101

Value:
CourseCreatedEvent
Kafka:
                   course-created
                          |
                    hash(courseId)
                          |
             +------------+------------+
             |            |            |
             ↓            ↓            ↓
           P0            P1            P2
                          ↑
                          |
                      Course 101
Later:
Course 101 Updated
Key:
101
Again:
hash(101)
→ same partition.

12. But There Is an Important Caveat
Suppose today we have:
3 partitions
and tomorrow we increase it to:
6 partitions
The partition mapping can change.
Why?
Because partition selection is based on the partition count as well as the key/hash behavior.
So don't conclude:
"A key permanently guarantees that this entity will always use the same physical partition forever."
The practical guarantee is about messages being routed consistently under the current partitioning setup.
Changing partition count can affect future routing and therefore ordering across the expansion boundary.
We'll discuss this deeply when we cover partition expansion and ordering.

13. Partition Count Is a Capacity Decision
Suppose:
course-created
has:
1 partition
You have limited parallelism.
If you have:
10 partitions
you can have up to roughly 10 active consumers in the same consumer group.
If you have:
100 partitions
you have much more potential parallelism.
But don't think:
"More partitions is always better."
More partitions also mean:
more metadata
more files/log segments
more replication overhead
more recovery work
more consumer-group coordination
So partition count is an architectural decision.
We'll come back to this in production design.

14. Partitions and Consumer Groups
This will become extremely important in Phase 2.
Suppose:
Topic
course-created

3 partitions
And:
Notification Service
has one consumer:
Consumer 1
   |
   +-- P0
   +-- P1
   +-- P2
One consumer handles all three.
Now add consumers:
Consumer 1 → P0
Consumer 2 → P1
Consumer 3 → P2
Now processing happens in parallel.
But:
4 consumers
3 partitions
means one consumer won't have a partition assigned.
This gives us an important relationship:
Partitions determine the maximum parallelism available to consumers within a consumer group.

15. A Very Important Distinction
Don't confuse:
Topic
with:
Partition
For example:
course-created
is the topic.
Inside it:
P0
P1
P2
are partitions.
Think of it like:
Topic = Book
Partition = Chapter
Offset = Page position
It's not a perfect analogy, but it helps.

16. Our Architecture Now
We now understand:
                Course Service
                       |
                       |
                CourseCreated
                       |
                       |
                Key = courseId
                       |
                       ↓
              +----------------+
              | Kafka Topic    |
              | course-created |
              +----------------+
                 /     |     \
                /      |      \
               ↓       ↓       ↓
              P0      P1      P2
For:
Course 101
we might get:
Key = 101
       ↓
   partitioner
       ↓
      P1
Then:
Offset 0 → CourseCreated 101
Offset 1 → CourseUpdated 101
Offset 2 → CourseUpdated 101
Offset 3 → CourseDeleted 101
That's the foundation for maintaining the course's event ordering.

17. One Correction to a Common Mental Model
Don't think:
Key = partition number
It isn't.
You provide:
Key = "101"
Kafka's partitioner determines the partition.
Conceptually:
key
 ↓
hash / partitioner
 ↓
partition number
So your application generally says:
Key = courseID
not:
Partition = 1

18. What We Know Now
At this point, you should have this mental model:
                   Kafka Cluster
                         |
                    Topic
                course-created
                         |
          +--------------+--------------+
          |              |              |
         P0             P1             P2
          |              |              |
       offsets        offsets        offsets
        0,1,2          0,1,2          0,1,2
And:
CourseCreated
      |
   courseId
      |
     Key
      |
 partitioner
      |
      ↓
   Partition
      |
      ↓
   Offset

🧠 The Three Rules I Want You to Remember
Rule 1
A topic is divided into partitions.
Topic → P0, P1, P2...
Rule 2
Ordering is guaranteed within a partition.
P0:
A → B → C → D
Rule 3
A key can be used to route related events to the same partition.
courseId = 101
      ↓
same partition
      ↓
ordered events

Next: Our First Kafka Producer 🚀
Now we have enough theory to actually touch Kafka.
We'll build:
POST /courses
       |
       ↓
Course Service
       |
       ├──→ MongoDB
       |
       └──→ Kafka Producer
                |
                ↓
          course-created
                |
                ↓
          Key = courseId
                |
                ↓
           Kafka Topic
And we'll not just copy-paste a Kafka library.
We'll understand:
What a Kafka Producer actually does
How the Go Kafka client works
key vs value
Serialization
acks
Producer retries
idempotence
What happens when Kafka is down
Whether the API should fail when Kafka publishing fails
How to inspect the actual message in Kafka
Then we'll modify your existing course-content-service rather than creating another project.


Absolutely. Let's pause implementation. Your approach is better if your goal is to become strong in Kafka for interviews and production design.
We should first build the complete theoretical foundation, and only then implement it in your course-content-service.
Kafka Learning Strategy
We'll learn in this order:
LEVEL 1 — FOUNDATION
│
├── What is Kafka?
├── Why Kafka?
├── Kafka vs traditional Queue
├── Event-driven architecture
├── Event vs Command vs Message
├── Event Contract
│
LEVEL 2 — KAFKA CORE
│
├── Kafka Cluster
├── Broker
├── Topic
├── Partition
├── Record / Message
├── Key
├── Offset
├── Producer
├── Consumer
├── Consumer Group
│
LEVEL 3 — PRODUCER
│
├── Partition selection
├── Serialization
├── Batching
├── Compression
├── acks
├── Retries
├── Idempotent Producer
├── Delivery semantics
│
LEVEL 4 — CONSUMER
│
├── Polling
├── Offset management
├── Auto commit
├── Manual commit
├── Consumer lag
├── Rebalancing
├── Heartbeat
├── Session timeout
├── max.poll.interval
│
LEVEL 5 — SCALING
│
├── Partition strategy
├── Consumer Groups
├── Parallelism
├── Ordering
├── Hot partitions
├── Partition reassignment
│
LEVEL 6 — RELIABILITY
│
├── At-most-once
├── At-least-once
├── Exactly-once
├── Duplicate messages
├── Idempotency
├── Retry
├── DLT
├── Poison messages
│
LEVEL 7 — ADVANCED
│
├── Replication
├── Leader / Follower
├── ISR
├── Controller / KRaft
├── Log segments
├── Retention
├── Log compaction
├── Transactions
├── Schema Registry
│
LEVEL 8 — PRODUCTION
│
├── Monitoring
├── Consumer lag
├── Throughput
├── Capacity planning
├── Security
├── TLS
├── SASL
├── ACL
├── Disaster recovery
├── Multi-region Kafka
│
LEVEL 9 — ECOSYSTEM
│
├── Kafka Connect
├── Debezium
├── CDC
├── Kafka Streams
├── Schema Registry
└── Event-driven microservices
We'll go one topic at a time and won't move ahead until the concept is clear.

First, Let's Strengthen the Foundation
You already understand:
Event
Event Contract
Command
Message
Broker
Topic
Partition
Key
Offset
But there is a bigger question behind all of these:
Why was Kafka created?
This is a very common interview question.

1. The Problem Before Kafka
Imagine an e-commerce system.
We have:
Order Service
Payment Service
Inventory Service
Notification Service
Analytics Service
A customer places an order.
Without Kafka:
                Order Service
                      |
          +-----------+-----------+
          |           |           |
          ↓           ↓           ↓
       Payment    Inventory   Notification
          |
          ↓
      Analytics
Order Service has to communicate directly with everyone.
This creates tight coupling.

2. Tight Coupling
Suppose Order Service calls:
POST /payment
POST /inventory
POST /notification
POST /analytics
Now what happens if Notification Service is down?
Order Service
     |
     +---- Payment       ✅
     |
     +---- Inventory     ✅
     |
     +---- Notification  ❌
Should the order fail?
Maybe not.
The order was successfully created.
But your synchronous architecture makes the services dependent on each other.

3. Kafka Introduces a Middle Layer
Instead:
               Order Service
                     |
                     |
                 OrderCreated
                     |
                     ↓
                  Kafka
                     |
        +------------+-------------+
        |            |             |
        ↓            ↓             ↓
     Payment     Inventory    Notification
     Service      Service       Service
Order Service says:
"Order 123 was created."
It doesn't care who is listening.
This is loose coupling.

4. Kafka Is More Than a Message Queue
This is a very important interview distinction.
Traditional queue:
Producer
   |
   v
 Queue
   |
   v
Consumer
Often the message is removed after successful consumption.
Kafka is fundamentally an:
Append-only distributed log.
Think:
Kafka Topic

Event 1
Event 2
Event 3
Event 4
Event 5
Consumer reads:
Event 1
Event 2
Event 3
But Kafka can retain those events.
Another consumer can later read:
Event 1
Event 2
Event 3
again.
That's why Kafka is excellent for event streaming and replay.

5. Queue vs Kafka
This is an interview favorite.
Traditional Queue
Kafka
Message-oriented
Log/event-oriented
Message often removed after consumption
Events retained
Usually one consumer processes a message
Multiple consumer groups can independently read
Replay usually not native
Replay is fundamental
Scaling depends on queue architecture
Partition-based scaling
Ordering often queue-level
Ordering is per partition
Good for task distribution
Excellent for event streaming

Don't say:
"Kafka is a queue."
A better interview answer:
Kafka can be used for queue-like workload distribution, but fundamentally it is a distributed append-only log designed for high-throughput event streaming.
That's a much stronger answer.

6. Your Course Example
Let's apply this directly.
When you create:
Course 101
Course Service publishes:
CourseCreated
Kafka stores it.
Then:
Notification Service
can consume it.
But so can:
Search Service
Analytics Service
Recommendation Service
Audit Service
Each can independently consume the event.
For example:
                    Kafka
                       |
                CourseCreated
                       |
       +---------------+---------------+
       |               |               |
       ↓               ↓               ↓
Notification       Search          Analytics
Group              Group            Group
This introduces another critical Kafka concept:
Consumer Groups
And this is where many Kafka interview questions begin.

7. Consumer Group
A consumer group is a group of consumers working together to consume a topic.
Suppose:
course-created
has:
3 partitions
And we have:
Notification Consumer Group
with:
Consumer A
Consumer B
Consumer C
Kafka can assign:
P0 → Consumer A

P1 → Consumer B

P2 → Consumer C
Now they're processing in parallel.

8. Why Consumer Groups Exist
Suppose notification processing is slow.
One consumer:
P0
P1
P2
 ↓
Consumer
Maybe it can't keep up.
Add consumers:
P0 → Consumer A
P1 → Consumer B
P2 → Consumer C
Now processing is parallel.
This is horizontal scaling.

9. The Most Important Consumer Group Rule
Inside one consumer group:
One partition can be assigned to only one consumer at a time.
Example:
3 partitions

3 consumers

P0 → C1
P1 → C2
P2 → C3
Perfect.
But:
3 partitions

5 consumers
becomes:
P0 → C1
P1 → C2
P2 → C3

C4 → idle
C5 → idle
Therefore:
Maximum active consumer parallelism within a consumer group is bounded by the number of partitions.
This is one of the most important Kafka interview concepts.

10. But Why Can Multiple Services Read the Same Event?
This is where consumer groups become really powerful.
Suppose:
course-created
has three independent applications:
Notification Service
Search Service
Analytics Service
We give each its own consumer group:
course-created
       |
       +---- notification-group
       |
       +---- search-group
       |
       +---- analytics-group
Each group gets its own logical consumption position.
So:
CourseCreated
      |
      +---- Notification Group
      |
      +---- Search Group
      |
      +---- Analytics Group
All three can process the same event independently.
This is fundamentally different from the traditional "one message → one consumer" queue model.

11. Very Important Interview Question
Q: Can two consumers in the same consumer group consume the same partition simultaneously?
No.
For a given partition:
Partition 0
     |
     +---- Consumer A
Not:
Partition 0
   |       |
   ↓       ↓
 C1       C2
within the same group.
But two different groups can consume that partition independently:
                Partition 0
                /            \
               /              \
              ↓                ↓
 Notification Group       Analytics Group
       C1                     C2
That's the distinction you need to remember.

12. Your Course System
Now our architecture becomes much more interesting:
                      Course Service
                            |
                            |
                     CourseCreated
                            |
                            ↓
                     Kafka Topic
                    course-created
                            |
             +--------------+--------------+
             |              |              |
             ↓              ↓              ↓
      notification-     search-        analytics-
         group           group            group
             |              |              |
             ↓              ↓              ↓
       Notification     Elasticsearch    Analytics
The same event can power multiple independent business capabilities.
This is why Kafka is so useful in microservices.

13. The Core Mental Model
I want you to build this mental model:
Producer
   |
   ↓
Topic
   |
   +---- Partition 0
   +---- Partition 1
   +---- Partition 2
             |
             ↓
       Consumer Group
             |
       +-----+-----+
       |           |
   Consumer 1   Consumer 2
And:
Topic
 |
 +---- Group A
 |
 +---- Group B
 |
 +---- Group C
Each group can independently consume the same topic.

Where We Are in Your Learning
We've covered:
✅ Event
✅ Event Contract
✅ Command vs Event
✅ Message
✅ Broker
✅ Topic
✅ Partition
✅ Key
✅ Offset
✅ Why Kafka
✅ Kafka vs Queue
✅ Consumer Group
But there are still three foundational concepts I want to cover before we touch producer code:
Next 1 — Kafka Replication
We'll understand:
Leader
Follower
Replication Factor
ISR
High Availability
Leader Election
This answers:
"What happens if the Kafka broker containing my data dies?"
Next 2 — Kafka Storage Model
We'll understand:
Append-only log
Log segment
Offset
Retention
Log compaction
This answers:
"Where does Kafka actually store my messages, and why can it replay old events?"
Next 3 — Producer Internals
Then we'll understand:
Producer
   ↓
Serializer
   ↓
Partitioner
   ↓
Batch
   ↓
Broker
   ↓
Acknowledgement
And only after those three, we'll start implementing your course-content-service.
That sequence will give you a much stronger Kafka foundation than jumping directly into producer.SendMessage().
Phase 1 — Step 5: Kafka Replication, Leader, Follower & ISR
Now we're moving into one of the most important Kafka architecture/interview topics:
What happens if a Kafka broker crashes?
So far we know:
Producer
   ↓
Topic
   ↓
Partitions
   ↓
Consumers
But there's a problem.
What if the broker storing a partition suddenly dies?

1. The Problem
Imagine:
Kafka Cluster

Broker 1
   |
   +---- course-created
           |
         P0
Your Course Service sends:
CourseCreated
to P0.
The data is stored on Broker 1.
Now:
Broker 1 💥
What happens?
If there is only one copy:
CourseCreated
     ❌
Your data may become unavailable or lost.
Kafka solves this using replication.

2. What Is Replication?
Kafka can maintain multiple copies of each partition.
Suppose:
Replication Factor = 3
Then:
Partition 0

Broker 1 → Copy
Broker 2 → Copy
Broker 3 → Copy
Conceptually:
                Partition 0
                      |
          +-----------+-----------+
          |           |           |
          ↓           ↓           ↓
       Broker 1    Broker 2    Broker 3
        Copy         Copy        Copy
Now if Broker 1 dies:
Broker 1 💥
we still have:
Broker 2
Broker 3
So Kafka can continue operating.

3. Replication Factor
Replication Factor (RF) means:
How many copies of each partition Kafka maintains.
For example:
Replication Factor = 3
means:
1 partition
   |
   +-- Replica 1
   +-- Replica 2
   +-- Replica 3
Important:
Replication factor is configured per topic, not globally for every message.
Example:
course-created
Replication Factor = 3

4. Leader and Followers
Now here's the important part.
Kafka doesn't have all replicas independently accepting writes.
For each partition, Kafka elects one replica as the:
Leader
The other replicas are:
Followers
Example:
Partition 0

Broker 1
   |
   +---- Leader

Broker 2
   |
   +---- Follower

Broker 3
   |
   +---- Follower
So:
             Partition 0

              LEADER
             Broker 1
                |
       +--------+--------+
       |                 |
       ↓                 ↓
   Follower           Follower
   Broker 2           Broker 3

5. Who Does the Producer Talk To?
The producer sends writes to the leader replica for that partition.
Example:
Course Service
      |
      | CourseCreated
      ↓
   Kafka
      |
      ↓
Partition 0 Leader
      |
      +---- Broker 1
The followers replicate the data from the leader.
So the simplified flow is:
Producer
   ↓
Partition Leader
   ↓
Followers

6. What About Consumers?
Consumers also normally fetch data from the partition leader in traditional Kafka architecture, although Kafka has evolved with features such as follower fetching in some deployments/configurations.
For the foundational mental model, remember:
Producer
    ↓
 Leader
    ↑
 Consumer
The leader is the authoritative replica for that partition.

7. Why Not Let Everyone Write?
Suppose:
Broker 1 → writes CourseCreated
Broker 2 → writes CourseUpdated
Broker 3 → writes CourseDeleted
Now we have multiple independent writers.
Maintaining a consistent ordered log becomes complicated.
Kafka instead establishes one leader per partition:
            Partition 0

                Leader
                   |
       +-----------+-----------+
       |                       |
    Follower                Follower
The leader controls the partition's write ordering.

8. What Happens When Leader Dies?
This is where Kafka's high availability comes from.
Suppose:
Partition 0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
Then:
Broker 1 💥
Kafka needs another replica to become leader.
So it can elect:
Broker 2 → New Leader
Broker 3 → Follower
Now:
Producer
   |
   ↓
Broker 2
   |
 Partition 0
The application can continue without manually changing the producer configuration.

9. ISR — In-Sync Replicas
Now we get to an extremely common interview question.
What does ISR mean?
ISR = In-Sync Replicas
It is the set of replicas that are considered sufficiently caught up with the partition leader according to Kafka's replication rules.
Example:
Partition 0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
If all are healthy and caught up:
ISR = {Broker 1, Broker 2, Broker 3}

10. What If One Follower Falls Behind?
Suppose:
Broker 1 → Leader       ✅
Broker 2 → Follower     ✅
Broker 3 → Follower     ❌
Broker 3 is too far behind.
Kafka may remove it from the ISR.
Now:
ISR = {Broker 1, Broker 2}
Broker 3 still exists, but it isn't currently considered in sync.
This distinction is very important:
Replica
   ≠
In-Sync Replica
A replica can exist without being in the ISR.

11. Why Does ISR Matter?
Because Kafka needs to know:
Which replicas are healthy enough to participate in failover and satisfy durability requirements?
Imagine:
Broker 1 → Leader
Broker 2 → ISR
Broker 3 → Not ISR
If Broker 1 crashes:
Broker 2
is a much safer candidate for leadership than a severely lagging Broker 3.

12. Replication Example
Let's say:
Topic: course-created

Partition 0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
Event arrives:
CourseCreated(101)
The leader appends it:
Broker 1

Offset 0 → CourseCreated(101)
Followers replicate it:
Broker 2

Offset 0 → CourseCreated(101)
Broker 3

Offset 0 → CourseCreated(101)
Now the replicas are caught up.

13. What If Broker 3 Is Slow?
Suppose:
Broker 1
Offset 0
Offset 1
Offset 2
Offset 3
Broker 2:
Offset 0
Offset 1
Offset 2
Offset 3
Broker 3:
Offset 0
Offset 1
Broker 3 is behind.
Kafka may have:
ISR = Broker 1, Broker 2
Broker 3 is still a replica, but not in ISR.

14. Replication Factor vs ISR
These are commonly confused.
Suppose:
Replication Factor = 3
That means:
3 replicas exist
But:
ISR = 2
means:
Only 2 replicas are currently in sync.
So:
RF = 3
ISR = 2
is completely possible.

15. acks Enters the Picture
Now we're reaching an important producer concept.
When the producer sends:
CourseCreated
it needs to know:
When should Kafka tell me that the write succeeded?
That's controlled by producer acknowledgement settings, commonly called:
acks
There are three classic settings:
acks=0
acks=1
acks=all

16. acks=0
Producer essentially says:
"Send the message. I don't need an acknowledgement."
Conceptually:
Producer
   |
   | CourseCreated
   ↓
Kafka

Producer continues
Very low waiting time, but poor durability guarantees.
If the broker fails immediately, the producer may not know the record wasn't safely accepted.

17. acks=1
Producer says:
"Tell me once the partition leader has accepted the record."
Conceptually:
Producer
   |
   ↓
Leader
   |
   | ACK
   ↓
Producer
But followers might not have replicated it yet.
If the leader crashes immediately afterward, durability depends on replication state and subsequent recovery/election behavior.

18. acks=all
Producer says:
"Wait until the leader considers the record replicated to the required in-sync replicas according to the topic/broker configuration."
Conceptually:
Producer
   |
   ↓
Leader
   |
   +----> Follower
   |
   +----> Follower
   |
   ↓
ACK
This gives stronger durability.
But there is a trade-off:
Stronger durability
       ↓
Potentially more waiting
We'll study acks, min.insync.replicas, and idempotence together later because they make much more sense as a group.

19. Important Interview Question
What is the difference between Replication Factor and min.insync.replicas?
Suppose:
Replication Factor = 3
There are three replicas.
But you can configure:
min.insync.replicas = 2
This means that for certain producer acknowledgement configurations, Kafka requires at least 2 in-sync replicas for the write to be considered successful.
So:
RF = 3
means:
"I maintain up to three replicas."
While:
min.insync.replicas = 2
means:
"I require at least two in-sync replicas for the required durability condition."

20. Failure Scenario
Let's say:
RF = 3
min.insync.replicas = 2
Initially:
Broker 1 → Leader
Broker 2 → ISR
Broker 3 → ISR
Everything is good.
Then Broker 3 fails:
Broker 1 → Leader
Broker 2 → ISR
Broker 3 → 💥
ISR becomes:
Broker 1
Broker 2
There are still 2 ISR replicas.
Writes can continue under the appropriate acks=all setup.

Now Broker 2 also fails:
Broker 1 → Leader
Broker 2 → 💥
Broker 3 → 💥
ISR:
Broker 1
Only one ISR remains.
But:
min.insync.replicas = 2
Therefore a producer using:
acks=all
cannot satisfy the required ISR condition.
The write can fail rather than silently accepting a less durable write.
This is a very important production reliability mechanism.

21. Why Kafka Replication Is Important for Your Project
Imagine your Course Service publishes:
CourseCreated(101)
You absolutely don't want:
MongoDB:
Course 101 exists ✅

Kafka:
CourseCreated lost ❌
because then:
Notification Service
       ❌
Search Service
       ❌
Analytics
       ❌
may never know the course was created.
Kafka replication helps ensure that the event remains available even when individual brokers fail.

22. One More Modern Kafka Concept: KRaft
You may see this in modern Kafka interviews.
Older Kafka architecture commonly relied on:
Kafka
 +
ZooKeeper
ZooKeeper was used for cluster metadata and coordination.
Modern Kafka uses:
KRaft (Kafka Raft)
to manage Kafka's metadata without requiring ZooKeeper.
So if an interviewer asks:
"Does Kafka require ZooKeeper?"
A modern answer is:
No. Modern Kafka uses KRaft mode for metadata management and cluster coordination. Older Kafka deployments commonly used ZooKeeper.
Don't spend too much time on KRaft yet. We'll cover it when we study Kafka internals.

23. The Mental Model
Keep this picture in your head:
                Kafka Cluster
        ┌────────────┬────────────┐
        │            │            │
     Broker 1     Broker 2     Broker 3
        │            │            │
        └────────────┼────────────┘
                     │
                Partition 0
                     │
             ┌───────┼───────┐
             │       │       │
          Leader  Follower Follower
If the leader dies:
Before:

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower

Broker 1 💥

After:

Broker 2 → New Leader
Broker 3 → Follower
That's Kafka's basic high-availability mechanism.

🎯 Interview Questions You Should Now Be Able to Answer
Try answering these yourself before looking at the answers:
Q1. What is replication factor?
Answer: Number of replicas maintained for each partition.
Q2. What is a Kafka leader?
Answer: The replica responsible for handling writes for a partition and serving as the authoritative replica for that partition.
Q3. What are followers?
Answer: Replicas that replicate data from the partition leader.
Q4. What is ISR?
Answer: In-Sync Replicas—the replicas currently considered sufficiently caught up with the leader.
Q5. Can RF=3 and ISR=2?
Yes. One replica can fall out of sync while the topic still has three replicas.
Q6. What happens if the leader broker fails?
Kafka can elect another eligible in-sync replica as the partition leader.
Q7. What does acks=all mean?
The producer waits for acknowledgement based on the required in-sync replica condition, rather than just the leader's local acknowledgement.

Where We Are
Our foundation is now:
                   Kafka
                      |
       +--------------+--------------+
       |                             |
   Distributed                  Event Streaming
       |
   +---+---+
   |       |
Broker   Replication
   |
 Topic
   |
Partition
   |
Offset
   |
 Key
   |
Consumer Group
Next theoretical topic: Kafka Storage Model
This is where Kafka becomes very different from a normal queue.
We'll go deep into:
Topic
  ↓
Partition
  ↓
Append-only Log
  ↓
Log Segment
  ↓
Offset
  ↓
Retention
  ↓
Log Compaction
  ↓
Replay
And I'll explain the important interview question:
"If a consumer has already consumed a Kafka message, how can another consumer read that same message again?"
Once you understand that, Kafka's replay, retention, and event-driven architecture will become much clearer.
Phase 1 — Step 6: Kafka Storage Model
This is a very important topic because it explains why Kafka is not just a traditional queue.
The key idea is:
Kafka stores events as an append-only log, and consumers track their position in that log using offsets.
Let's build this from the ground up.

1. What Happens When Kafka Receives a Message?
Suppose your Course Content Service publishes:
CourseCreated
with:
courseId = 101
Kafka doesn't simply put it into some temporary memory queue.
It writes the record to a partition log.
course-created
       |
       ↓
Partition 0

+--------------------------------------+
| CourseCreated | CourseCreated | ... |
+--------------------------------------+
        0               1
      offset          offset
Think of the partition as a continuously growing log.

2. Append-Only Log
Append-only means Kafka primarily adds new records to the end of the log.
Suppose:
Partition 0
initially contains:
Offset 0 → CourseCreated 101
Offset 1 → CourseCreated 102
Offset 2 → CourseCreated 103
New event arrives:
CourseCreated 104
Kafka appends it:
Offset 0 → CourseCreated 101
Offset 1 → CourseCreated 102
Offset 2 → CourseCreated 103
Offset 3 → CourseCreated 104
It doesn't insert it at the beginning.
That's why it's called an append-only log.

3. Why Is Append-Only Important?
Sequential writes are extremely efficient.
Instead of constantly modifying random locations:
write
update
delete
insert
update
delete
Kafka primarily does:
append
append
append
append
append
This works very well with disk storage.
Kafka can therefore achieve very high throughput.

4. Kafka Stores Data on Disk
A common misconception is:
"Kafka is an in-memory message queue."
No.
Kafka persists records to disk.
Conceptually:
Producer
   |
   ↓
Kafka Broker
   |
   ↓
Disk
   |
   └── Partition Log
Kafka also uses memory heavily for caching and buffering, but the durable log is stored on disk.
Modern Kafka performance benefits heavily from the operating system's filesystem/page cache and sequential I/O patterns.

5. Partition Is the Physical Log
Remember:
Topic
   |
   +── Partition 0
   +── Partition 1
   +── Partition 2
Each partition is its own ordered log.
For example:
Partition 0

Offset 0
Offset 1
Offset 2
Offset 3
Offset 4
And:
Partition 1

Offset 0
Offset 1
Offset 2
Offset 3
Each partition has its own log.

6. Offset Is the Position
This now becomes much easier to understand.
Partition 0

Offset       Event
------       --------------------
0            CourseCreated 101
1            CourseCreated 102
2            CourseCreated 103
3            CourseCreated 104
4            CourseCreated 105
The offset identifies the record's position within that partition.
Important:
Offsets are unique only within a partition.
So this is perfectly valid:
Partition 0 → Offset 10
Partition 1 → Offset 10
Partition 2 → Offset 10
There is no single global offset for the entire topic.

7. The Big Difference from a Traditional Queue
Imagine:
Producer
   ↓
Queue
   ↓
Consumer
Consumer processes:
Message A
In many traditional queue systems, after successful processing:
Message A → removed
Kafka works differently.
Suppose:
Kafka

Offset 0 → Event A
Offset 1 → Event B
Offset 2 → Event C
Consumer reads:
A
B
C
Kafka doesn't say:
"Consumer read it, therefore delete it."
The records remain according to the topic's retention policy.

8. Then How Does Kafka Know What the Consumer Has Read?
This is where consumer offsets come in.
Suppose Notification Service has processed:
Offset 0
Offset 1
Offset 2
It can commit its progress.
Conceptually:
Notification Group

Partition 0
Committed position = 3
Meaning:
"I've successfully processed through offset 2; the next record to process is offset 3."
This is extremely important.

9. Consumer Position vs Kafka Data
Don't confuse these two things.
Kafka has:
DATA
↓
Offset 0
Offset 1
Offset 2
Offset 3
Offset 4
Consumer group has:
PROGRESS
↓
Committed offset = 3
The data and the consumer's progress are separate.
That's why multiple consumer groups can read the same data independently.

10. Multiple Consumer Groups
Suppose:
course-created

Offset 0 → Course 101
Offset 1 → Course 102
Offset 2 → Course 103
We have:
Notification Group
Search Group
Analytics Group
Their positions can be completely different.
For example:
Notification Group → offset 3
Search Group       → offset 2
Analytics Group    → offset 1
All three can independently consume the same records.
This is one of Kafka's biggest strengths.

11. Replay
Now we reach one of Kafka's most powerful features.
Suppose Analytics Service has processed:
Offset 0
Offset 1
Offset 2
Offset 3
Offset 4
Then your analytics algorithm changes.
You want to process old events again.
With Kafka, you can move the consumer group's position backward and replay events, provided those records are still retained.
For example:
Current:

Offset 0
Offset 1
Offset 2
Offset 3 ← current
Offset 4
Offset 5
You can reset the consumer group to:
Offset 0
and process again.
That's event replay.

12. Why Replay Is Useful
Imagine your Analytics Service had a bug.
For example:
CourseCompleted
was incorrectly calculated.
You fix the code.
Instead of asking Course Service:
"Can you send me all historical course events again?"
you can replay the Kafka topic.
Kafka
   |
   | historical events
   ↓
Analytics Service
   |
   ↓
Rebuild analytics
This is extremely useful in event-driven systems.

13. Retention
But there's a question:
"Does Kafka store events forever?"
Not necessarily.
Kafka topics have retention policies.
For example:
retention = 7 days
means Kafka can remove records once they are older than the configured retention criteria.
Conceptually:
Today

Event A → 8 days old ❌ removed
Event B → 5 days old ✅
Event C → 2 days old ✅
Event D → 1 hour old ✅
Therefore:
Replay is only possible while the required data is still retained, unless another archival mechanism exists.

14. Retention Is Not Based on Consumption
This is extremely important.
Suppose:
Notification Service
never consumes:
CourseCreated
Does Kafka immediately delete it?
No.
Kafka retention is not simply:
Consumed → delete
Instead, retention is governed by topic configuration and log-cleanup policies.
So:
Consumer reads event
        ↓
event remains
        ↓
retention policy eventually removes it
This is one of the biggest conceptual differences between Kafka and many queue systems.

15. Time-Based Retention
A topic can be configured with a retention period.
For example:
7 days
Conceptually:
Day 1 → Event A
Day 2 → Event B
Day 3 → Event C
...
Day 8 → Event A may be eligible for deletion
The exact cleanup timing is not necessarily instantaneous at the exact moment the record reaches the retention threshold because Kafka cleans logs asynchronously.
For interviews, remember:
Retention controls how long records are kept.

16. Size-Based Retention
Retention doesn't have to be only time-based.
You can also configure a maximum log size.
For example:
100 GB
Conceptually:
Partition Log

A
B
C
D
E
F
G
H
...
Once the log exceeds the configured size threshold, older segments can become eligible for deletion.
So Kafka retention can be based on:
Time
+
Size
depending on configuration.

17. Log Segments
Now we go one level deeper.
A partition isn't normally represented as one enormous file forever.
Kafka divides the partition log into log segments.
Conceptually:
Partition 0

+----------+----------+----------+----------+
| Segment  | Segment  | Segment  | Segment  |
|    0     |    1     |    2     |    3     |
+----------+----------+----------+----------+
Each segment contains a range of records.
For example:
Segment 0

Offset 0
Offset 1
Offset 2
...
Offset 999
Then:
Segment 1

Offset 1000
Offset 1001
...
Offset 1999
and so on.

18. Why Segments?
This makes retention and cleanup manageable.
Imagine a topic has:
100 GB
of data.
Kafka doesn't need to rewrite the entire 100 GB when old data expires.
It can delete old segments.
For example:
Before:

Segment 0 → old
Segment 1 → old
Segment 2 → recent
Segment 3 → recent

After cleanup:

Segment 0 ❌
Segment 1 ❌

Segment 2 ✅
Segment 3 ✅
This is much more efficient.

19. Log Compaction
Now we come to another important interview topic:
Log Compaction
This is different from normal time/size-based deletion.
Imagine we have:
Key = student-101
Value = Basic
Then later:
Key = student-101
Value = Premium
Then:
Key = student-101
Value = Enterprise
The log might contain:
student-101 → Basic
student-101 → Premium
student-101 → Enterprise
For some use cases, you don't need every historical state.
You care about the latest value for each key.
Log compaction allows Kafka to clean older records for the same key while retaining the latest state, subject to Kafka's compaction semantics.
Conceptually:
Before compaction:

101 → Basic
101 → Premium
101 → Enterprise

After compaction:

101 → Enterprise

20. Retention vs Compaction
This is a common interview question.
Delete-based retention
Think:
"Delete old records."
Example:
Older than 7 days → eligible for deletion
Log compaction
Think:
"Keep the latest value for each key."
Example:
101 → Basic
101 → Premium
101 → Enterprise

↓

101 → Enterprise
So:
Retention
    ↓
Time/size based cleanup

Compaction
    ↓
Latest state per key

21. When Would We Use Compaction?
Suppose you create a topic:
student-profile
Events:
student-101 → Arjun
student-102 → Rahul
student-101 → Arjun Singh
student-101 → Arjun Singh Kumar
If the topic is compacted, Kafka can eventually clean older versions of the same key.
The topic effectively becomes a durable changelog of latest state.
This is useful for:
current configuration
user profiles
account state
latest product information
caches/state stores
Kafka Streams state restoration

22. Tombstone Records
Compaction introduces another important concept.
Suppose:
Key = course-101
Value = course data
Then you want to represent deletion.
Kafka can use a tombstone:
Key = course-101
Value = null
Conceptually:
course-101 → Course data
course-101 → null
The null value indicates deletion in a compacted topic.
Later, compaction can remove the old value and eventually the tombstone according to the relevant cleanup rules.

23. Your Course Example
Imagine we have:
course-state
with:
Key = courseId
Events:
101 → Golang
102 → Java
101 → Advanced Golang
103 → Kafka
101 → Advanced Kafka + Go
A compacted topic could eventually retain approximately:
101 → Advanced Kafka + Go
102 → Java
103 → Kafka
This is very different from:
course-created
where you may want every historical creation event.

24. Event Topic vs State Topic
This gives us an important architectural distinction.
Event/history topic
course-created
You care about:
Event 1
Event 2
Event 3
Event 4
...
State/changelog topic
course-state
You may care primarily about:
latest state per key
This distinction becomes very useful later when we study Kafka Streams.

25. Kafka Storage Mental Model
You should now visualize Kafka like this:
                        Kafka
                           |
                         Topic
                           |
                     course-created
                           |
              +------------+------------+
              |                         |
         Partition 0                Partition 1
              |                         |
        Append-only log          Append-only log
              |                         |
        +-----+------+            +-----+------+
        |            |            |            |
    Segment 0    Segment 1    Segment 0    Segment 1
        |
     Records
        |
     Offsets
And separately:
Consumer Group
      |
      ↓
Committed Offset
The consumer's offset is progress, not deletion of the underlying event.

26. The Most Important Concept
Here's the sentence I want you to remember for interviews:
Kafka stores records in partitioned, ordered, append-only logs and retains those records according to topic cleanup policies, allowing consumers to independently track their positions and replay retained data.
That's a strong Kafka definition.

27. Interview Questions
Q1. Is Kafka an in-memory system?
No. Kafka persists records to disk and leverages memory/page cache for performance.
Q2. Does Kafka delete a message after a consumer reads it?
No. Consumption and deletion are separate concepts.
Q3. How does a consumer know where it stopped?
Through its consumer-group offset/progress.
Q4. Can a consumer replay an old message?
Yes, if the record is still retained, by moving the consumer group's position backward.
Q5. What is a log segment?
A physical segment of a partition's append-only log used to organize storage and enable efficient cleanup.
Q6. What is retention?
The policy controlling when records become eligible for deletion based on factors such as time and/or log size.
Q7. What is log compaction?
A cleanup strategy that retains the latest record for a given key, allowing a topic to represent current state while still retaining changes according to compaction rules.
Q8. Retention vs compaction?
Retention → remove old data based on cleanup limits
Compaction → retain latest state per key

Our Kafka Foundation So Far
We now have a pretty strong theoretical base:
                   KAFKA
                      |
        +-------------+-------------+
        |                           |
   Architecture                 Storage
        |                           |
     Broker                     Disk
        |                           |
     Topic                  Append-only Log
        |                           |
    Partition                 Segments
        |                           |
      Offset                  Retention
        |                           |
       Key                  Log Compaction
        |
 Consumer Group
Next theoretical topic: Kafka Producer Internals
Before writing any Go code, we'll understand exactly what happens when your Course Service executes:
Publish(CourseCreated)
We'll trace the complete journey:
Course Service
      ↓
Producer
      ↓
Serialization
      ↓
Key selection
      ↓
Partitioner
      ↓
Batch
      ↓
Network
      ↓
Kafka Broker
      ↓
Partition Leader
      ↓
Replication
      ↓
Acknowledgement
Then we'll go deep into the interview-heavy producer concepts:
acks=0/1/all → retries → idempotent producer → batching → compression → delivery guarantees.
