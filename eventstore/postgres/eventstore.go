package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"gorm.io/gorm"
)

// ErrCouldNotDialDB is when the database could not be dialed.
var ErrCouldNotDialDB = errors.New("could not dial database")

// ErrNoDBClient is when no database client is set.
var ErrNoDBClient = errors.New("no database client")

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

// ErrCouldNotMarshalEvent is when an event could not be marshaled into BSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshaled into a concrete type.
var ErrCouldNotUnmarshalEvent = errors.New("could not unmarshal event")

// ErrCouldNotLoadAggregate is when an aggregate could not be loaded.
var ErrCouldNotLoadAggregate = errors.New("could not load aggregate")

// ErrCouldNotSaveAggregate is when an aggregate could not be saved.
var ErrCouldNotSaveAggregate = errors.New("could not save aggregate")

// EventStore implements an EventStore for Postgres.
type EventStore struct {
	client    *gorm.DB
	dbPrefix  string
	dbName    func(ctx context.Context) string
	tableName string
}

// NewEventStore creates a new EventStore with a Postgres
func NewEventStore(d *gorm.DB, dbPrefix string) (*EventStore, error) {
	return NewEventStoreWithClient(d, dbPrefix)
}

// NewEventStoreWithClient creates a new EventStore with a client.
func NewEventStoreWithClient(client *gorm.DB, dbPrefix string) (*EventStore, error) {
	if client == nil {
		return nil, ErrNoDBClient
	}
	s := &EventStore{
		client:    client,
		dbPrefix:  dbPrefix,
		tableName: dbPrefix + "_events",
	}

	s.dbName = func(ctx context.Context) string {
		return dbPrefix + "_events"
	}

	return s, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventStore) error

// WithPrefixAsDBName uses only the prefix as DB name, without namespace support.
func WithPrefixAsDBName() Option {
	return func(s *EventStore) error {
		s.dbName = func(context.Context) string {
			return s.dbPrefix
		}
		return nil
	}
}

// WithDBName uses a custom DB name function.
func WithDBName(dbName func(context.Context) string) Option {
	return func(s *EventStore) error {
		s.dbName = dbName
		return nil
	}
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	if len(events) == 0 {
		return eh.EventStoreError{
			Err:       eh.ErrNoEventsToAppend,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	dbEvents := make([]evt, len(events))
	aggregateID := events[0].AggregateID()
	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != aggregateID {
			return eh.EventStoreError{
				Err:       eh.ErrInvalidEvent,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != originalVersion+i+1 {
			return eh.EventStoreError{
				Err:       eh.ErrIncorrectEventVersion,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}

		// Create the event record for the DB.
		e, err := newEvt(ctx, event)
		if err != nil {
			return err
		}
		dbEvents[i] = *e
	}

	currentLatestVersion, _ := s.loadLatestVersion(ctx, aggregateID)
	tx := s.client.Begin()

	for _, e := range dbEvents {
		if e.Version > currentLatestVersion {
			tx.Table(s.tableName).Create(&e)
			tx.Table("events").Create(&e)
		}
	}
	tx.Commit()

	return nil
}

func (s *EventStore) loadLatestVersion(ctx context.Context, id uuid.UUID) (int, error) {
	var count int64
	s.client.Table(s.tableName).Where("aggregate_id = ?", id).Count(&count)
	return int(count), nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (s *EventStore) Load(ctx context.Context, id uuid.UUID) ([]eh.Event, error) {
	// var aggregate aggregateRecord

	events := []evt{}

	result := []eh.Event{}
	s.client.Table(s.tableName).Where("aggregate_id = ?", id).Find(&events)
	for _, e := range events {
		e.data, _ = eh.CreateEventData(e.EventType)
		json.Unmarshal(e.RawData, &e.data)
		var meta map[string]interface{}
		json.Unmarshal(e.Metadata, &meta)

		event := eh.NewEvent(
			e.EventType,
			e.data,
			e.Timestamp,
			eh.ForAggregate(
				e.AggregateType,
				e.AggregateID,
				e.Version,
			),
			eh.WithMetadata(meta),
		)
		result = append(result, event)
	}

	return result, nil
}

// Replace implements the Replace method of the eventhorizon.EventStore interface.
func (s *EventStore) Replace(ctx context.Context, event eh.Event) error {
	return nil
}

// RenameEvent implements the RenameEvent method of the eventhorizon.EventStore interface.
func (s *EventStore) RenameEvent(ctx context.Context, from, to eh.EventType) error {
	return nil
}

// Clear clears the event storage.
func (s *EventStore) Clear(ctx context.Context) error {
	return nil
}

// Close closes the database client.
func (s *EventStore) Close(ctx context.Context) {
}

// aggregateRecord is the Database representation of an aggregate.
type aggregateRecord struct {
	AggregateID uuid.UUID `bson:"_id" gorm:"column: aggregate_id"`
	Version     int       `bson:"version"`
	Events      []evt     `bson:"events"`
	// Type        string        `bson:"type"`
	// Snapshot    bson.Raw      `bson:"snapshot"`
}

// evt is the internal event record for the MongoDB event store used
// to save and load events from the DB.

type evt struct {
	EventType     eh.EventType     `bson:"event_type" gorm:"column:event_type" json:"event_type"`
	RawData       []byte           `bson:"data,omitempty" gorm:"column:raw_data" json:"raw_data"`
	data          eh.EventData     `bson:"-" gorm:"-"`
	Timestamp     time.Time        `bson:"timestamp" json:"timestamp"`
	AggregateType eh.AggregateType `bson:"aggregate_type" json:"aggregate_type"`
	AggregateID   uuid.UUID        `bson:"_id" json:"aggregate_id"`
	Version       int              `bson:"version" json:"version"`
	Metadata      []byte           `bson:"metadata" json:"metadata"`
}

// Migrate event store
func (s *EventStore) Migrate(d *gorm.DB) {
	d.Table("events").AutoMigrate(&evt{})
	d.Table(s.tableName).AutoMigrate(&evt{})
}

// newEvt returns a new evt for an event.
func newEvt(ctx context.Context, event eh.Event) (*evt, error) {
	b, _ := json.Marshal(event.Metadata())
	e := &evt{
		EventType:     event.EventType(),
		Timestamp:     event.Timestamp(),
		AggregateType: event.AggregateType(),
		AggregateID:   event.AggregateID(),
		Version:       event.Version(),
		Metadata:      b,
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error
		e.RawData, err = json.Marshal(event.Data())
		if err != nil {
			return nil, eh.EventStoreError{
				Err:       ErrCouldNotMarshalEvent,
				BaseErr:   err,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}
	}

	return e, nil
}
