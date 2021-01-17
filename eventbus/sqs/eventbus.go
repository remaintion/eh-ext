// Copyright (c) 2014 - The Event Horizon authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/google/uuid"

	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"google.golang.org/api/option"

	// Register uuid.UUID as BSON type.
	_ "github.com/looplab/eventhorizon/types/mongodb"

	eh "github.com/looplab/eventhorizon"
)

// EventBus is a local event bus that delegates handling of published events
// to all matching registered handlers, in order of registration.
type EventBus struct {
	appID        string
	sns          *sns.SNS
	sqs          *sqs.SQS
	topic        *pubsub.Topic
	registered   map[eh.EventHandlerType]struct{}
	registeredMu sync.RWMutex
	errCh        chan eh.EventBusError
	wg           sync.WaitGroup
	queueURL     string
}

var sess *session.Session

// NewEventBus creates an EventBus, with optional GCP connection settings.
func NewEventBus(appID string, opts ...option.ClientOption) (*EventBus, error) {
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	sns := sns.New(sess)
	sqs := sqs.New(sess)
	queueURL := os.Getenv("SQS_URL")
	// Get or create the topic.
	return &EventBus{
		appID:      appID,
		sns:        sns,
		sqs:        sqs,
		queueURL:   queueURL,
		registered: map[eh.EventHandlerType]struct{}{},
		errCh:      make(chan eh.EventBusError, 100),
	}, nil
}

// HandlerType implements the HandlerType method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandlerType() eh.EventHandlerType {
	return "eventbus"
}

const (
	aggregateTypeAttribute = "aggregate_type"
	eventTypeAttribute     = "event_type"
)

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandleEvent(ctx context.Context, event eh.Event) error {
	e := evt{
		AggregateID:   event.AggregateID().String(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Metadata:      event.Metadata(),
		Context:       eh.MarshalContext(ctx),
	}

	// Marshal event data if there is any.

	if event.Data() != nil {
		b, _ := json.Marshal(event.Data())
		e.RawData = string(b)
	}

	j, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}

	publishInput := &sns.PublishInput{
		Message:        aws.String(string(j)),
		TopicArn:       aws.String(os.Getenv("SNS_ARN")),
		MessageGroupId: aws.String(b.appID),
	}

	if _, err := b.sns.Publish(publishInput); err != nil {
		log.Println("could not publish event: " + err.Error())
		return errors.New("could not publish event: " + err.Error())
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(ctx context.Context, m eh.EventMatcher, h eh.EventHandler) error {
	if m == nil {
		return eh.ErrMissingMatcher
	}
	if h == nil {
		return eh.ErrMissingHandler
	}

	// Check handler existence.
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()
	if _, ok := b.registered[h.HandlerType()]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	b.registered[h.HandlerType()] = struct{}{}

	b.wg.Add(1)
	go b.handle(ctx, m, h)

	return nil
}

// Errors implements the Errors method of the eventhorizon.EventBus interface.
func (b *EventBus) Errors() <-chan eh.EventBusError {
	return b.errCh
}

// Wait for all channels to close in the event bus group
func (b *EventBus) Wait() {
	b.wg.Wait()
}

// Handles all events coming in on the channel.
func (b *EventBus) handle(ctx context.Context, m eh.EventMatcher, h eh.EventHandler) {
	defer b.wg.Done()

	for {
		msgResult, err := b.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            aws.String(b.queueURL),
			MaxNumberOfMessages: aws.Int64(2),
			VisibilityTimeout:   aws.Int64(100),
			WaitTimeSeconds:     aws.Int64(1),
		})
		for _, msg := range msgResult.Messages {
			b.handlerMessage(m, h, msg, ctx)
		}
		if err != nil {
			log.Println(err)
		}
	}
}

func (b *EventBus) handlerMessage(m eh.EventMatcher, h eh.EventHandler, msg *sqs.Message, ctx context.Context) {

	// Decode the raw BSON event data.
	rawMessage := map[string]interface{}{}
	json.Unmarshal([]byte(*msg.Body), &rawMessage)
	rawJSON := ""
	var e evt
	if _, ok := rawMessage["Message"]; ok == true {
		rawJSON = rawMessage["Message"].(string)
	} else {
		rawJSON = *msg.Body
	}
	if err := json.Unmarshal([]byte(rawJSON), &e); err != nil {
		err = fmt.Errorf("could not unmarshal event: %w", err)
		select {
		case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx}:
		default:
			log.Printf("eventhorizon: missed error in SQS event bus: %s", err)
		}
		return
	}
	log.Println("EVT", e.Version, e, e.RawData)

	// Create an event of the correct type and decode from raw BSON.
	if len(e.RawData) > 0 {
		var err error
		if e.data, err = eh.CreateEventData(e.EventType); err != nil {
			err = fmt.Errorf("could not create event data: %w", err)
			select {
			case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx}:
			default:
				log.Printf("eventhorizon: missed error in SQS event bus: %s", err)
			}
			return
		}
		if err := json.Unmarshal([]byte(e.RawData), &e.data); err != nil {
			log.Println(e.RawData)
			b.sqs.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(b.queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			err = fmt.Errorf("could not unmarshal event data: %w", err)
			select {
			case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx}:
			default:
				log.Printf("eventhorizon: missed error in SQS event bus: %s", err)
			}
			return
		}
		e.RawData = ""
	}

	ctx = eh.UnmarshalContext(ctx, e.Context)
	aggregateID, err := uuid.Parse(e.AggregateID)
	if err != nil {
		aggregateID = uuid.Nil
	}
	event := eh.NewEvent(
		e.EventType,
		e.data,
		e.Timestamp,
		eh.ForAggregate(
			e.AggregateType,
			aggregateID,
			e.Version,
		),
		eh.WithMetadata(e.Metadata),
	)

	// Ignore non-matching events.
	if !m.Match(event) {
		log.Println("non matching events", event)
		return
	}

	// Handle the event if it did match.
	if err := h.HandleEvent(ctx, event); err != nil {
		err = fmt.Errorf("could not handle event (%s): %w", h.HandlerType(), err)

		b.sqs.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(b.queueURL),
			ReceiptHandle: msg.ReceiptHandle,
		})
		select {
		case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx, Event: event}:
		default:
			log.Printf("eventhorizon: missed error in SQS event bus: %s", err)
		}
		return
	}
	b.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(b.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
}

// Creates a filter in the GCP pub sub filter syntax:
// https://cloud.google.com/pubsub/docs/filtering
func createFilter(m eh.EventMatcher) string {
	switch m := m.(type) {
	case eh.MatchEvents:
		s := make([]string, len(m))
		for i, et := range m {
			s[i] = fmt.Sprintf(`attributes:"%s"`, et) // Filter event types by key to save space.
		}
		return strings.Join(s, " OR ")
	case eh.MatchAggregates:
		s := make([]string, len(m))
		for i, at := range m {
			s[i] = fmt.Sprintf(`attributes.%s="%s"`, aggregateTypeAttribute, at)
		}
		return strings.Join(s, " OR ")
	case eh.MatchAny:
		s := make([]string, len(m))
		for i, sm := range m {
			s[i] = fmt.Sprintf("(%s)", createFilter(sm))
		}
		return strings.Join(s, " OR ")
	case eh.MatchAll:
		s := make([]string, len(m))
		for i, sm := range m {
			s[i] = fmt.Sprintf("(%s)", createFilter(sm))
		}
		return strings.Join(s, " AND ")
	default:
		return ""
	}
}

// evt is the internal event used on the wire only.
type evt struct {
	EventType     eh.EventType           `bson:"event_type" json:"event_type"`
	RawData       string                 `bson:"data,omitempty" json:"data"`
	data          eh.EventData           `bson:"-"`
	Timestamp     time.Time              `bson:"timestamp" json:"timestamp"`
	AggregateType eh.AggregateType       `bson:"aggregate_type" json:"aggregate_type"`
	AggregateID   string                 `bson:"_id" json:"aggregate_id"`
	Version       int                    `bson:"version" json:"version"`
	Metadata      map[string]interface{} `bson:"metadata" json:"metadata"`
	Context       map[string]interface{} `bson:"context" json:"context"`
}
