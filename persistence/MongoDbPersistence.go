package persistence

import (
	"reflect"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/v3/config"
	cerror "github.com/pip-services3-go/pip-services3-commons-go/v3/errors"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/v3/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/v3/log"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	mongoopt "go.mongodb.org/mongo-driver/mongo/options"
)

/*
 Abstract persistence component that stores data in MongoDB using plain driver.

 This is the most basic persistence component that is only
 able to store data items of any type. Specific CRUD operations
 over the data items must be implemented in child classes by
 accessing <code>c._db</code> or <code>c._collection</code> properties.

 ### Configuration parameters ###

 - collection:                  (optional) MongoDB collection name
 - connection(s):
    - discovery_key:             (optional) a key to retrieve the connection from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]]
  - host:                      host name or IP address
   - port:                      port number (default: 27017)
   - uri:                       resource URI or connection string with all parameters in it
 - credential(s):
   - store_key:                 (optional) a key to retrieve the credentials from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore]]
   - username:                  (optional) user name
   - password:                  (optional) user password
 - options:
   - max_pool_size:             (optional) maximum connection pool size (default: 2)
   - keep_alive:                (optional) enable connection keep alive (default: true)
   - connect_timeout:           (optional) connection timeout in milliseconds (default: 5000)
   - socket_timeout:            (optional) socket timeout in milliseconds (default: 360000)
   - auto_reconnect:            (optional) enable auto reconnection (default: true)
   - reconnect_interval:        (optional) reconnection interval in milliseconds (default: 1000)
   - max_page_size:             (optional) maximum page size (default: 100)
   - replica_set:               (optional) name of replica set
   - ssl:                       (optional) enable SSL connection (default: false)
   - auth_source:               (optional) authentication source
   - debug:                     (optional) enable debug output (default: false).

 References

 - <code>\*:logger:\*:\*:1.0</code>           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages
 - <code>\*:discovery:\*:\*:1.0</code>        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
 - <code>\*:credential-store:\*:\*:1.0</code> (optional) Credential stores to resolve credentials

 Example

     class MyMongoDbPersistence extends MongoDbPersistence<MyData> {

       func (c * MongoDbPersistence) constructor() {
           base("mydata")
       }

       func (c * MongoDbPersistence) getByName(correlationId: string, name: string, callback: (err, item) => void) {
         let criteria = { name: name }
         c._model.findOne(criteria, callback)
       })

       func (c * MongoDbPersistence) set(correlatonId: string, item: MyData, callback: (err) => void) {
         let criteria = { name: item.name }
         let options = { upsert: true, new: true }
         c._model.findOneAndUpdate(criteria, item, options, callback)
       }

     }

     let persistence = new MyMongoDbPersistence()
     persistence.configure(ConfigParams.fromTuples(
         "host", "localhost",
         "port", 27017
     ))

     persitence.open("123", (err) => {
          ...
     })

     persistence.set("123", { name: "ABC" }, (err) => {
         persistence.getByName("123", "ABC", (err, item) => {
             console.log(item)                   // Result: { name: "ABC" }
         })
     })
*/
//implements IReferenceable, IUnreferenceable, IConfigurable, IOpenable, ICleanable
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

// Creates a new instance of the persistence component.
// - collection    (optional) a collection name.
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

//  Configures component by passing configuration parameters.
//  - config    configuration parameters to be set.
func (c *MongoDbPersistence) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(&c.defaultConfig)
	c.config = *config
	c.DependencyResolver.Configure(config)
	c.CollectionName = config.GetAsStringWithDefault("collection", c.CollectionName)
}

// Sets references to dependent components.
// - references 	references to locate the component dependencies.
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

// Unsets (clears) previously set references to dependent components.
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

//  Adds index definition to create it on opening
//  - keys index keys (fields)
//  - options index options
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

// Convert object (map) from public view
// replace "Id" to "_id" field
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

// Convert object (map) to public view
// replace "_id" to "Id" field
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

// Checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *MongoDbPersistence) IsOpen() bool {
	return c.opened
}

// Opens the component.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - callback 			callback function that receives error or null no errors occured.
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

// Closes component and frees used resources.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - callback 			callback function that receives error or null no errors occured.

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

// Clears component state.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - callback 			callback function that receives error or null no errors occured.
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
