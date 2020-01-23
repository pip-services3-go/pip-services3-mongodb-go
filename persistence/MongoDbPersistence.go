package persistence

import (
	"reflect"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerror "github.com/pip-services3-go/pip-services3-commons-go/errors"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
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
		mmdbp:= MyMongoDbPersistence{}
		mmdbp.MongoDbPersistence = NewMongoDbPersistence(proto, collection)
		return &mmdbp
    }

    func (c * MyMongoDbPersistence) GetByName(correlationId string, name string) (item interface{}, err error) {
        filter := bson.M{"name": name}
		docPointer := getProtoPtr(c.Prototype)
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
		docPointer := getProtoPtr(c.Prototype)
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
*/
type MongoDbPersistence struct {
	defaultConfig cconf.ConfigParams

	config          cconf.ConfigParams
	references      crefer.IReferences
	opened          bool
	localConnection bool
	indexes         []mongodrv.IndexModel
	Prototype       reflect.Type

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
	mdbp := MongoDbPersistence{}
	mdbp.defaultConfig = *cconf.NewConfigParamsFromTuples(
		"collection", "",
		"dependencies.connection", "*:connection:mongodb:*:1.0",
		"options.max_pool_size", "2",
		"options.keep_alive", "1000",
		"options.connect_timeout", "5000",
		"options.auto_reconnect", "true",
		"options.max_page_size", "100",
		"options.debug", "true",
	)
	mdbp.DependencyResolver = *crefer.NewDependencyResolverWithParams(&mdbp.defaultConfig, mdbp.references)
	mdbp.Logger = *clog.NewCompositeLogger()
	mdbp.CollectionName = collection
	mdbp.indexes = make([]mongodrv.IndexModel, 0, 10)
	mdbp.config = *cconf.NewEmptyConfigParams()
	mdbp.Prototype = proto

	return &mdbp
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
func (c *MongoDbPersistence) ConvertFromPublic(item *interface{}) {
	var value interface{} = *item
	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		panic("ConvertFromPublic:Error! Item is not a pointer!")
	}

	if reflect.TypeOf(value).Kind() == reflect.Map {
		m, ok := value.(map[string]interface{})
		if ok {
			m["_id"] = m["Id"]
			delete(m, "Id")
			return
		}
	}

	if reflect.TypeOf(value).Kind() == reflect.Struct {

		return
	}

	panic("ConvertFromPublic:Error! Item must to be a map[string]interface{} or struct!")
}

// ConvertToPublic method is convert object (map) to public view by replaced "_id" to "Id" field
// Parameters:
// 	- item *interface{}
// 	converted item
func (c *MongoDbPersistence) ConvertToPublic(item *interface{}) {
	var value interface{} = *item
	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		panic("ConvertToPublic:Error! Item is not a pointer!")
	}

	if reflect.TypeOf(value).Kind() == reflect.Map {
		m, ok := value.(map[string]interface{})
		if ok {
			m["Id"] = m["_id"]
			delete(m, "_id")
			return
		}
	}

	if reflect.TypeOf(value).Kind() == reflect.Struct {

		return
	}

	panic("ConvertToPublic:Error! Item must to be a map[string]interface{} or struct!")
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
