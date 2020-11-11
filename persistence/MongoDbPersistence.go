package persistence

import (
	"math/rand"
	"reflect"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cerror "github.com/pip-services3-go/pip-services3-commons-go/errors"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
	cmpersist "github.com/pip-services3-go/pip-services3-data-go/persistence"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	mngoptions "go.mongodb.org/mongo-driver/mongo/options"
	mongoopt "go.mongodb.org/mongo-driver/mongo/options"
)

/*
 MongoDbPersistence abstract persistence component that stores data in MongoDB using plain driver.

 This is the most basic persistence component that is only
 able to store data items of any type. Specific CRUD operations
 over the data items must be implemented in child classes by
 accessing c.Db or c.Collection properties.

Configuration parameters:

 - collection:                  (optional) MongoDB collection name
 - connection(s):
    - discovery_key:             (optional) a key to retrieve the connection from IDiscovery
  	- host:                      host name or IP address
	- port:                      port number (default: 27017)
	- database:                  database name
   	- uri:                       resource URI or connection string with all parameters in it
 - credential(s):
   	- store_key:                 (optional) a key to retrieve the credentials from ICredentialStore
   	- username:                  (optional) user name
   	- password:                  (optional) user password
 - options:
   	- max_pool_size:             (optional) maximum connection pool size (default: 2)
   	- keep_alive:                (optional) enable connection keep alive (default: true)
   	- connect_timeout:           (optional) connection timeout in milliseconds (default: 5000)
   	- socket_timeout:            (optional) socket timeout in milliseconds (default: 360000)
   	- auto_reconnect:            (optional) enable auto reconnection (default: true) (not used)
   	- reconnect_interval:        (optional) reconnection interval in milliseconds (default: 1000) (not used)
   	- max_page_size:             (optional) maximum page size (default: 100)
   	- replica_set:               (optional) name of replica set
   	- ssl:                       (optional) enable SSL connection (default: false) (not implements in this release)
   	- auth_source:               (optional) authentication source
   	- debug:                     (optional) enable debug output (default: false). (not used)

 References:

 - *:logger:*:*:1.0           (optional) ILogger components to pass log messages
 - *:discovery:*:*:1.0        (optional) IDiscovery services
 - *:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials

 Example:

	type MyMongoDbPersistence struct {
		MongoDbPersistence
	}

    func NewMyMongoDbPersistence(proto reflect.Type, collection string) *MyMongoDbPersistence {
		mc:= MyMongoDbPersistence{}
		mc.MongoDbPersistence = NewMongoDbPersistence(proto, collection)
		return &mc
    }

    func (c * MyMongoDbPersistence) GetByName(correlationId string, name string) (item interface{}, err error) {
        filter := bson.M{"name": name}
		docPointer := NewObjectByPrototype(c.Prototype)
		foRes := c.Collection.FindOne(context.TODO(), filter)
		ferr := foRes.Decode(docPointer.Interface())
		if ferr != nil {
			if ferr == mongo.ErrNoDocuments {
				return nil, nil
			}
			return nil, ferr
		}
		item = docPointer.Elem().Interface()
		c.ConvertToPublic(&item)
		return item, nil
       }

    func (c * MyMongoDbPersistence) Set(correlatonId string, item MyData) (result interface{}, err error) {
		newItem = cmpersist.CloneObject(item)
		// Assign unique id if not exist
		cmpersist.GenerateObjectId(&newItem)
		id := cmpersist.GetObjectId(newItem)
		c.ConvertFromPublic(&newItem)
		filter := bson.M{"_id": id}
		var options mngoptions.FindOneAndReplaceOptions
		retDoc := mngoptions.After
		options.ReturnDocument = &retDoc
		upsert := true
		options.Upsert = &upsert
		frRes := c.Collection.FindOneAndReplace(context.TODO(), filter, newItem, &options)
		if frRes.Err() != nil {
			return nil, frRes.Err()
		}
		docPointer := NewObjectByPrototype(c.Prototype)
		err = frRes.Decode(docPointer.Interface())
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, nil
			}
			return nil, err
		}
		item = docPointer.Elem().Interface()
		c.ConvertToPublic(&item)
		return item, nil
    }

    persistence := NewMyMongoDbPersistence(reflect.TypeOf(MyData{}), "mycollection")
    persistence.Configure(NewConfigParamsFromTuples(
        "host", "localhost",
		"port", "27017",
		"database", "test",
    ))

	opnErr := persitence.Open("123")
	if opnErr != nil {
		...
	}

	resItem, setErr := persistence.Set("123", MyData{ name: "ABC" })
	if setErr != nil {
		...
	}

	item, getErr := persistence.GetByName("123", "ABC")
	if getErr != nil {
		...
	}
    fmt.Println(item)                   // Result: { name: "ABC" }
    ("123", "ABC")
	if getErr != nil {
		...
	}
    fmt.Println(item)                   // Result: { name: "ABC" }

    ("123", "ABC")
	if getErr != nil {
		...
	}
    fmt.Println(item)                   // Result: { name: "ABC" }
*/

type MongoDbPersistence struct {
	defaultConfig cconf.ConfigParams

	config          cconf.ConfigParams
	references      crefer.IReferences
	opened          bool
	localConnection bool
	indexes         []mongodrv.IndexModel
	Prototype       reflect.Type
	maxPageSize     int32

	ConvertFromPublic        func(interface{}) interface{}
	ConvertToPublic          func(interface{}) interface{}
	ConvertFromPublicPartial func(interface{}) interface{}

	// The dependency resolver.
	DependencyResolver crefer.DependencyResolver
	// The logger.
	Logger clog.CompositeLogger
	// The MongoDB connection component.
	Connection *MongoDbConnection
	// The MongoDB connection object.
	Client *mongodrv.Client
	// The MongoDB database name.
	DatabaseName string
	// The MongoDB colleciton object.
	CollectionName string
	//  The MongoDb database object.
	Db *mongodrv.Database
	// The MongoDb collection object.
	Collection *mongodrv.Collection
}

// NewMongoDbPersistence are creates a new instance of the persistence component.
// Parameters:
//	- proto reflect.Type
//	type of saved data, need for correct decode from DB
// 	- collection  string
//  a collection name.
// Return *MongoDbPersistence
// new created MongoDbPersistence component
func NewMongoDbPersistence(proto reflect.Type, collection string) *MongoDbPersistence {
	c := MongoDbPersistence{}
	c.defaultConfig = *cconf.NewConfigParamsFromTuples(
		"collection", "",
		"dependencies.connection", "*:connection:mongodb:*:1.0",
		"options.max_pool_size", "2",
		"options.keep_alive", "1000",
		"options.connect_timeout", "5000",
		"options.auto_reconnect", "true",
		"options.max_page_size", "100",
		"options.debug", "true",
	)
	c.DependencyResolver = *crefer.NewDependencyResolverWithParams(&c.defaultConfig, c.references)
	c.Logger = *clog.NewCompositeLogger()
	c.CollectionName = collection
	c.indexes = make([]mongodrv.IndexModel, 0, 10)
	c.config = *cconf.NewEmptyConfigParams()
	c.ConvertFromPublic = c.PerformConvertFromPublic
	c.ConvertToPublic = c.PerformConvertToPublic
	c.ConvertFromPublicPartial = c.PerformConvertFromPublic

	c.Prototype = proto

	return &c
}

// Configure method is configures component by passing configuration parameters.
// Parameters:
// 	- config  *cconf.ConfigParams
//  configuration parameters to be set.
func (c *MongoDbPersistence) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(&c.defaultConfig)
	c.config = *config
	c.DependencyResolver.Configure(config)
	c.CollectionName = config.GetAsStringWithDefault("collection", c.CollectionName)
}

// SetReferences method are sets references to dependent components.
// Parameters:
// 	- references crefer.IReferences
//	references to locate the component dependencies.
func (c *MongoDbPersistence) SetReferences(references crefer.IReferences) {
	c.references = references
	c.Logger.SetReferences(references)

	// Get connection
	c.DependencyResolver.SetReferences(references)
	con, ok := c.DependencyResolver.GetOneOptional("connection").(*MongoDbConnection)
	if ok {
		c.Connection = con
	}
	// Or create a local one
	if c.Connection == nil {
		c.Connection = c.createConnection()
		c.localConnection = true
	} else {
		c.localConnection = false
	}
}

// UnsetReferences method is unsets (clears) previously set references to dependent components.
func (c *MongoDbPersistence) UnsetReferences() {
	c.Connection = nil
}

func (c *MongoDbPersistence) createConnection() *MongoDbConnection {
	connection := NewMongoDbConnection()

	//if c.config != nil {
	connection.Configure(&c.config)
	//}
	if c.references != nil {
		connection.SetReferences(c.references)
	}
	return connection
}

// EnsureIndex method are adds index definition to create it on opening
// Parameters:
// 	- keys interface{}
//	index keys (fields)
//  - options *mongoopt.IndexOptions
// 	index options
func (c *MongoDbPersistence) EnsureIndex(keys interface{}, options *mongoopt.IndexOptions) {
	if keys == nil {
		return
	}
	index := mongodrv.IndexModel{
		Keys:    keys,
		Options: options,
	}
	c.indexes = append(c.indexes, index)
}

// ConvertFromPublic method help convert object (map) from public view by replaced "Id" to "_id" field
// Parameters:
// 	- item *interface{}
// 	converted item
func (c *MongoDbPersistence) PerformConvertFromPublic(item interface{}) interface{} {

	if item == nil {
		return nil
	}

	var value interface{} = item
	var t reflect.Type = reflect.TypeOf(item)

	if reflect.TypeOf(item).Kind() == reflect.Ptr {
		value = reflect.ValueOf(item).Elem().Interface()
		t = reflect.ValueOf(item).Elem().Type()
	}

	if t.Kind() == reflect.Map {
		m, ok := value.(map[string]interface{})
		if ok {
			m["_id"] = m["Id"]
			delete(m, "Id")

		}
	}

	return item
}

// ConvertToPublic method is convert object (map) to public view by replaced "_id" to "Id" field
// Parameters:
// 	- item *interface{}
// 	converted item
func (c *MongoDbPersistence) PerformConvertToPublic(value interface{}) interface{} {

	if value == nil {
		return nil
	}

	docPointer, ok := value.(reflect.Value)
	if !ok {
		if c.Prototype.Kind() == reflect.Ptr {
			docPointer = reflect.New(c.Prototype.Elem())
		} else {
			docPointer = reflect.New(c.Prototype)
		}
		docPointer.Elem().Set(reflect.ValueOf(value))
	}

	item := docPointer.Elem().Interface()

	if reflect.TypeOf(item).Kind() == reflect.Map {
		m, ok := item.(map[string]interface{})
		if ok {
			m["Id"] = m["_id"]
			delete(m, "_id")
		}

	}

	if c.Prototype.Kind() == reflect.Ptr {
		return docPointer.Interface()
	}
	return item
}

// IsOpen method is checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *MongoDbPersistence) IsOpen() bool {
	return c.opened
}

// Open method is opens the component.
// Parameters:
// 	- correlationId  string
//	(optional) transaction id to trace execution through call chain.
// Return error
// error or nil when no errors occured.
func (c *MongoDbPersistence) Open(correlationId string) error {

	var err error
	if c.opened {
		//callback(null)
		return nil
	}
	if c.Connection == nil {
		c.Connection = c.createConnection()
		c.localConnection = true
	}
	c.opened = false
	if c.localConnection {
		err = c.Connection.Open(correlationId)
	}
	if err == nil && c.Connection == nil {
		return cerror.NewInvalidStateError(correlationId, "NO_CONNECTION", "MongoDB connection is missing")
	}
	if err == nil && !c.Connection.IsOpen() {
		return cerror.NewConnectionError(correlationId, "CONNECT_FAILED", "MongoDB connection is not opened")
	}
	c.Client = c.Connection.GetConnection()
	c.Db = c.Connection.GetDatabase()
	c.DatabaseName = c.Connection.GetDatabaseName()
	c.Collection = c.Db.Collection(c.CollectionName)
	if c.Collection == nil {
		c.Db = nil
		c.Client = nil
		return cerror.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to mongodb failed").WithCause(err)
	}
	//ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	//defer cancel()

	// Recreate indexes
	if len(c.indexes) > 0 {
		keys, errIndexes := c.Collection.Indexes().CreateMany(c.Connection.Ctx, c.indexes, mongoopt.CreateIndexes())
		if errIndexes != nil {
			c.Db = nil
			c.Client = nil
			return cerror.NewConnectionError(correlationId, "CREATE_IDX_FAILED", "Recreate indexes failed").WithCause(err)
		}
		for _, v := range keys {
			c.Logger.Debug(correlationId, "Created index %s for collection %s", v, c.CollectionName)
		}
	}
	c.opened = true
	c.Logger.Debug(correlationId, "Connected to mongodb database %s, collection %s", c.DatabaseName, c.CollectionName)
	return nil
}

// Close methos closes component and frees used resources.
// Parameters:
// 	- correlationId string
//	(optional) transaction id to trace execution through call chain.
// Return error
// error or nil when no errors occured.
func (c *MongoDbPersistence) Close(correlationId string) error {
	var err error

	if !c.opened {
		return nil
	}
	if c.Connection == nil {
		return cerror.NewInvalidStateError(correlationId, "NO_CONNECTION", "MongoDb connection is missing")
	}
	if c.localConnection {
		err = c.Connection.Close(correlationId)
	}
	if err != nil {
		return err
	}
	c.opened = false
	c.Client = nil
	c.Db = nil
	c.Collection = nil
	return nil
}

// Clear method are clears component state.
// Parameters:
// 	- correlationId string
// 	(optional) transaction id to trace execution through call chain.
// Returns error
// error or nil when no errors occured.
func (c *MongoDbPersistence) Clear(correlationId string) error {
	// Return error if collection is not set
	if c.CollectionName == "" {
		return cerror.NewError("Collection name is not defined")
	}

	err := c.Collection.Drop(c.Connection.Ctx)
	if err != nil {
		return cerror.NewConnectionError(correlationId, "CLEAR_FAILED", "Clear collection failed.").WithCause(err)
	}
	return nil
}

// GetPageByFilter is gets a page of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a func (c *IdentifiableMongoDbPersistence) GetPageByFilter method from child type that
// receives FilterParams and converts them into a filter function.
// Parameters:
// 	- correlationId  string
//   (optional) transaction id to Trace execution through call chain.
//  - filter interface{}
//  (optional) a filter JSON object
//  - paging *cdata.PagingParams
//  (optional) paging parameters
//  - sort interface{}
//  (optional) sorting BSON object
//  - select  interface{}
//  (optional) projection BSON object
// Returns page cdata.DataPage, err error
// a data page or error, if they are occured
func (c *MongoDbPersistence) GetPageByFilter(correlationId string, filter interface{}, paging *cdata.PagingParams,
	sort interface{}, sel interface{}) (page *cdata.DataPage, err error) {
	// Adjust max item count based on configuration
	if paging == nil {
		paging = cdata.NewEmptyPagingParams()
	}
	skip := paging.GetSkip(-1)
	take := paging.GetTake((int64)(c.maxPageSize))
	pagingEnabled := paging.Total
	// Configure options
	var options mngoptions.FindOptions
	if skip >= 0 {
		options.Skip = &skip
	}
	options.Limit = &take
	if sort != nil {
		options.Sort = sort
	}
	if sel != nil {
		options.Projection = sel
	}
	cursor, ferr := c.Collection.Find(c.Connection.Ctx, filter, &options)
	items := make([]interface{}, 0, 1)
	if ferr != nil {
		var total int64 = 0
		page = cdata.NewDataPage(&total, items)
		return page, ferr
	}
	for cursor.Next(c.Connection.Ctx) {
		docPointer := c.NewObjectByPrototype()
		curErr := cursor.Decode(docPointer.Interface())
		if curErr != nil {
			continue
		}

		item := c.ConvertToPublic(docPointer)
		items = append(items, item)
	}
	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.CollectionName)
	}
	if pagingEnabled {
		docCount, _ := c.Collection.CountDocuments(c.Connection.Ctx, filter)
		page = cdata.NewDataPage(&docCount, items)
	} else {
		var total int64 = 0
		page = cdata.NewDataPage(&total, items)
	}
	return page, nil
}

// GetListByFilter is gets a list of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a func (c *IdentifiableMongoDbPersistence) GetListByFilter method from child type that
// receives FilterParams and converts them into a filter function.
// Parameters:
// 	- correlationId	string
// 	(optional) transaction id to Trace execution through call chain.
// 	- filter interface{}
//	(optional) a filter BSON object
// 	- sort interface{}
//	(optional) sorting BSON object
// 	- select interface{}
//	(optional) projection BSON object
// Returns items []interface{}, err error
// data list and error, if they are ocurred
func (c *MongoDbPersistence) GetListByFilter(correlationId string, filter interface{}, sort interface{}, sel interface{}) (items []interface{}, err error) {

	// Configure options
	var options mngoptions.FindOptions

	if sort != nil {
		options.Sort = sort
	}
	if sel != nil {
		options.Projection = sel
	}

	cursor, ferr := c.Collection.Find(c.Connection.Ctx, filter, &options)
	if ferr != nil {
		return nil, ferr
	}

	for cursor.Next(c.Connection.Ctx) {
		docPointer := c.NewObjectByPrototype()
		curErr := cursor.Decode(docPointer.Interface())
		if curErr != nil {
			continue
		}

		item := c.ConvertToPublic(docPointer)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.CollectionName)
	}
	return items, nil
}

// GetOneRandom is gets a random item from items that match to a given filter.
// This method shall be called by a func (c *IdentifiableMongoDbPersistence) getOneRandom method from child class that
// receives FilterParams and converts them into a filter function.
// Parameters:
// 	- correlationId string
//	(optional) transaction id to Trace execution through call chain.
// - filter interface{}
// (optional) a filter BSON object
// Returns: item interface{}, err error
// random item and error, if theq are occured
func (c *MongoDbPersistence) GetOneRandom(correlationId string, filter interface{}) (item interface{}, err error) {

	docCount, cntErr := c.Collection.CountDocuments(c.Connection.Ctx, filter)
	if cntErr != nil {
		return nil, cntErr
	}
	var options mngoptions.FindOptions
	rand.Seed(time.Now().UnixNano())
	var itemNum int64 = rand.Int63n(docCount)
	var itemLim int64 = 1

	if itemNum < 0 {
		itemNum = 0
	}
	options.Skip = &itemNum
	options.Limit = &itemLim
	cursor, fndErr := c.Collection.Find(c.Connection.Ctx, filter, &options)
	if fndErr != nil {
		return nil, fndErr
	}
	docPointer := c.NewObjectByPrototype()
	err = cursor.Decode(docPointer.Interface())
	if err != nil {
		return nil, err
	}

	item = c.ConvertToPublic(docPointer)
	return item, nil
}

// Create was creates a data item.
// Parameters:
// 	- correlation_id string
//	(optional) transaction id to Trace execution through call chain.
// 	- item interface{}
// an item to be created.
// Returns result interface{}, err error
// created item and error, if they are occured
func (c *MongoDbPersistence) Create(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil {
		return nil, nil
	}
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	newItem = c.ConvertFromPublic(newItem)
	insRes, insErr := c.Collection.InsertOne(c.Connection.Ctx, newItem)
	newItem = c.ConvertToPublic(newItem)

	if insErr != nil {
		return nil, insErr
	}
	c.Logger.Trace(correlationId, "Created in %s with id = %s", c.Collection, insRes.InsertedID)
	return newItem, nil
}

// DeleteByFilter is deletes data items that match to a given filter.
// This method shall be called by a func (c *IdentifiableMongoDbPersistence) deleteByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// Parameters:
// 	- correlationId  string
//  (optional) transaction id to Trace execution through call chain.
// 	- filter  interface{}
//	(optional) a filter BSON object.
// Return error
// error or nil for success.
func (c *MongoDbPersistence) DeleteByFilter(correlationId string, filter interface{}) error {
	delRes, delErr := c.Collection.DeleteMany(c.Connection.Ctx, filter)
	var count = delRes.DeletedCount
	if delErr != nil {
		return delErr
	}
	c.Logger.Trace(correlationId, "Deleted %d items from %s", count, c.Collection)
	return nil
}

// GetCountByFilter is gets a count of data items retrieved by a given filter.
// This method shall be called by a func (c *IdentifiableMongoDbPersistence) GetCountByFilter method from child type that
// receives FilterParams and converts them into a filter function.
// Parameters:
// 	- correlationId  string
//   (optional) transaction id to Trace execution through call chain.
//  - filter interface{}
// Returns count int, err error
// a data count or error, if they are occured
func (c *MongoDbPersistence) GetCountByFilter(correlationId string, filter interface{}) (count int64, err error) {

	// Configure options
	var options mngoptions.CountOptions
	count = 0
	count, err = c.Collection.CountDocuments(c.Connection.Ctx, filter, &options)
	c.Logger.Trace(correlationId, "Find %d items in %s", count, c.CollectionName)
	return count, err
}

// service function for return pointer on new prototype object for unmarshaling
func (c *MongoDbPersistence) NewObjectByPrototype() reflect.Value {
	proto := c.Prototype
	if proto.Kind() == reflect.Ptr {
		proto = proto.Elem()
	}
	return reflect.New(proto)
}

// func (c *MongoDbPersistence) ConvertResultToPublic(docPointer reflect.Value, proto reflect.Type) interface{} {
// 	item := docPointer.Elem().Interface()
// 	c.ConvertToPublic(&item)
// 	if proto.Kind() == reflect.Ptr {
// 		return docPointer.Interface()
// 	}
// 	return item
// }
