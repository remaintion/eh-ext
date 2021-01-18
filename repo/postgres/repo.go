// Copyright (c) 2015 - The Event Horizon authors
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

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	pubsub "github.com/remaintion/eh-ext/pubsub/sns"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"

	// Register uuid.UUID as BSON type.
	_ "github.com/looplab/eventhorizon/types/mongodb"

	eh "github.com/looplab/eventhorizon"
)

// ErrCouldNotDialDB is when the database could not be dialed.
var ErrCouldNotDialDB = errors.New("could not dial database")

// ErrNoDBClient is when no database client is set.
var ErrNoDBClient = errors.New("no database client")

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

// ErrModelNotSet is when an model factory is not set on the Repo.
var ErrModelNotSet = errors.New("model not set")

// ErrInvalidQuery is when a query was not returned from the callback to FindCustom.
var ErrInvalidQuery = errors.New("invalid query")

// ErrNotmplemented is when func is unimplemented
var ErrNotmplemented = errors.New("postgres repo func is unimplemented")

// Repo implements an MongoDB repository for entities.
type Repo struct {
	client          *gorm.DB
	dbPrefix        string
	collection      string
	factoryFn       func() eh.Entity
	dbName          func(context.Context) string
	tableName       string
	enableBroadcast bool
	pubsub          *pubsub.PubSub
}

// NewRepo creates a new Repo.
func NewRepo(d *gorm.DB, tableName string, enableBroadcast bool) (*Repo, error) {
	return NewRepoWithClient(d, tableName, enableBroadcast)
}

// NewRepoWithClient creates a new Repo with a client.
func NewRepoWithClient(client *gorm.DB, tableName string, enableBroadcast bool) (*Repo, error) {
	if client == nil {
		return nil, ErrNoDBClient
	}

	r := &Repo{
		client:          client,
		tableName:       tableName,
		enableBroadcast: enableBroadcast,
		pubsub:          pubsub.CreatePubSub(),
	}

	// Use the a prefix and namespcae from the context for DB name.
	r.dbName = func(ctx context.Context) string {
		return tableName
	}

	return r, nil
}

// Option is an option setter used to configure creation.
type Option func(*Repo) error

// WithPrefixAsDBName uses only the prefix as DB name, without namespace support.
func WithPrefixAsDBName() Option {
	return func(r *Repo) error {
		r.dbName = func(context.Context) string {
			return r.dbPrefix
		}
		return nil
	}
}

// WithDBName uses a custom DB name function.
func WithDBName(dbName func(context.Context) string) Option {
	return func(r *Repo) error {
		r.dbName = dbName
		return nil
	}
}

// Parent implements the Parent method of the eventhorizon.ReadRepo interface.
func (r *Repo) Parent() eh.ReadRepo {
	return nil
}

// Find implements the Find method of the eventhorizon.ReadRepo interface.
func (r *Repo) Find(ctx context.Context, aggregateID uuid.UUID) (eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	entity := r.factoryFn()
	result := map[string]interface{}{}

	r.client.Table(r.tableName).Where("aggregate_id = ?", aggregateID).Scan(&result)
	b, _ := json.Marshal(result)
	json.Unmarshal(b, &entity)
	v := reflect.ValueOf(entity)
	x := reflect.Indirect(v)
	ParseJson(x, result)

	return entity, nil
}

func CopyValue(src interface{}, dest interface{}) {
	srcRef := reflect.ValueOf(src)
	vp := reflect.ValueOf(dest)
	vp.Elem().Set(srcRef)
}
func ParseJson(x reflect.Value, result map[string]interface{}) {
	for i := 0; i < x.NumField(); i++ {
		tag := x.Type().Field(i).Tag.Get("gorm")

		if strings.Contains(tag, "json") {
			jsonTag := x.Type().Field(i).Tag.Get("json")
			if len(result) != 0 && result[jsonTag] != nil {
				c := reflect.Indirect(reflect.New(x.Field(i).Type()))
				parsed := parseInnerJson(c, result[jsonTag], true)
				x.Field(i).Set(parsed)
			}
		}
	}
}

func parseInnerJson(v reflect.Value, raw interface{}, isString bool) reflect.Value {
	c := reflect.Indirect(v)

	switch c.Kind() {
	case reflect.Slice:
		elem := c.Type().Elem()
		tmp := []interface{}{}
		json.Unmarshal([]byte(raw.(string)), &tmp)
		t := reflect.MakeSlice(reflect.SliceOf(elem), 0, 0)
		for i := range tmp {
			newV := reflect.Indirect(reflect.New(elem))
			parsed := parseInnerJson(newV, tmp[i], false)
			t = reflect.Append(t, parsed)
		}
		c.Set(t)
	case reflect.Struct:
		tmp := map[string]interface{}{}
		if isString {
			json.Unmarshal([]byte(raw.(string)), &tmp)
		} else {
			tmp = raw.(map[string]interface{})
		}

		for j := 0; j < c.NumField(); j++ {
			tag := c.Type().Field(j).Tag.Get("json")
			ft := c.Field(j).Type().String()
			if tmp[tag] == nil {
				continue
			}
			switch ft {
			case "int":
				c.Field(j).SetInt(int64(tmp[tag].(float64)))
			case "float64":
				c.Field(j).SetFloat(tmp[tag].(float64))
			case "string":
				c.Field(j).SetString(tmp[tag].(string))
			case "time.Time":
				layout := time.RFC3339Nano
				t, terr := time.Parse(layout, tmp[tag].(string))
				if terr != nil {
					panic(terr)
				}
				c.Field(j).Set(reflect.ValueOf(t))
			default:
				parsed := parseInnerJson(c.Field(j), tmp[tag].(interface{}), true)
				c.Field(j).Set(parsed)
			}
		}
	case reflect.Map:
		elem := c.Type().Elem()
		var keyType = c.Type().Key()

		tmp := map[string]interface{}{}
		json.Unmarshal([]byte(raw.(string)), &tmp)

		mapType := reflect.MapOf(keyType, elem)
		t := reflect.MakeMapWithSize(mapType, 0)

		for i := range tmp {
			newV := reflect.Indirect(reflect.New(elem))
			parsed := parseInnerJson(newV, tmp[i], false)
			k := reflect.ValueOf(i)
			switch keyType.Kind() {
			case reflect.Int:
				n, _ := strconv.Atoi(i)
				k = reflect.ValueOf(n)
			case reflect.Int64:
				n, _ := strconv.Atoi(i)
				k = reflect.ValueOf(int64(n))
			case reflect.String:
				k = reflect.ValueOf(string(i))
			}
			t.SetMapIndex(k, parsed)
		}
		c.Set(t)
	default:
		// panic(c.Kind())
	}

	return c
}

// FindAll implements the FindAll method of the eventhorizon.ReadRepo interface.
func (r *Repo) FindAll(ctx context.Context) ([]eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil, ErrNotmplemented
}

// The iterator is not thread safe.
type iter struct {
	cursor    *mongo.Cursor
	data      eh.Entity
	factoryFn func() eh.Entity
	decodeErr error
}

func (i *iter) Next(ctx context.Context) bool {
	if !i.cursor.Next(ctx) {
		return false
	}

	item := i.factoryFn()
	i.decodeErr = i.cursor.Decode(item)
	i.data = item
	return true
}

func (i *iter) Value() interface{} {
	return i.data
}

func (i *iter) Close(ctx context.Context) error {
	if err := i.cursor.Close(ctx); err != nil {
		return err
	}
	return i.decodeErr
}

// FindCustomIter returns a mgo cursor you can use to stream results of very large datasets
func (r *Repo) FindCustomIter(ctx context.Context, f func(context.Context, *mongo.Collection) (*mongo.Cursor, error)) (eh.Iter, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	// c := r.client.Database(r.dbName(ctx)).Collection(r.collection)

	// cursor, err := f(ctx, c)
	// if err != nil {
	// 	return nil, eh.RepoError{
	// 		Err:       ErrInvalidQuery,
	// 		BaseErr:   err,
	// 		Namespace: eh.NamespaceFromContext(ctx),
	// 	}
	// }
	// if cursor == nil {
	// 	return nil, eh.RepoError{
	// 		Err:       ErrInvalidQuery,
	// 		Namespace: eh.NamespaceFromContext(ctx),
	// 	}
	// }

	return &iter{
		// cursor:    cursor,
		// factoryFn: r.factoryFn,
	}, ErrNotmplemented
}

// FindCustom uses a callback to specify a custom query for returning models.
// It can also be used to do queries that does not map to the model by executing
// the query in the callback and returning nil to block a second execution of
// the same query in FindCustom. Expect a ErrInvalidQuery if returning a nil
// query from the callback.
func (r *Repo) FindCustom(ctx context.Context, f func(context.Context, *mongo.Collection) (*mongo.Cursor, error)) ([]interface{}, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	result := []interface{}{}

	return result, ErrNotmplemented
}

func (r *Repo) BroadcastEntity(ctx context.Context, entity eh.Entity) {
	en, err := r.Find(ctx, entity.EntityID())
	if err != nil {
		panic(err)
	}
	data, _ := json.Marshal(en)

	err = r.pubsub.Publish(entity.EntityID(), data)
	if err != nil {
		panic(err)
	}
}

// Save implements the Save method of the eventhorizon.WriteRepo interface.
func (r *Repo) Save(ctx context.Context, entity eh.Entity) error {
	if entity.EntityID() == uuid.Nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   eh.ErrMissingEntityID,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	b, _ := json.Marshal(entity)
	x := map[string]interface{}{}

	json.Unmarshal(b, &x)
	timeStr := x["updated_at"].(string)
	timeArr := strings.Split(timeStr, "-")
	if timeArr[0] == "0001" {
		er := r.client.Table(r.tableName).Create(entity).Error
		if er != nil {
			panic(er)
		}
	} else {
		er := r.client.Table(r.tableName).Updates(entity).Error
		if er != nil {
			panic(er)
		}
	}
	if r.enableBroadcast {
		r.BroadcastEntity(ctx, entity)
	}
	return nil
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo) Remove(ctx context.Context, id uuid.UUID) error {
	return nil
}

// Collection lets the function do custom actions on the collection.
func (r *Repo) Collection(ctx context.Context, f func(context.Context, *mongo.Collection) error) error {
	return nil
}

// SetEntityFactory sets a factory function that creates concrete entity types.
func (r *Repo) SetEntityFactory(f func() eh.Entity) {
	r.factoryFn = f
}

// Clear clears the read model database.
func (r *Repo) Clear(ctx context.Context) error {
	return nil
}

// Close closes a database session.
func (r *Repo) Close(ctx context.Context) {
	// r.client.Disconnect(ctx)
}

// Migrate table
func Migrate(d *gorm.DB, tableName string, entity interface{}) {
	if err := d.Table(tableName).AutoMigrate(entity); err != nil {
		panic(err)
	}
}

// Repository returns a parent ReadRepo if there is one.
func Repository(repo eh.ReadRepo) *Repo {
	if repo == nil {
		return nil
	}

	if r, ok := repo.(*Repo); ok {
		return r
	}

	return Repository(repo.Parent())
}
