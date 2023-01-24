package persistence

import (
	"reflect"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cmpersist "github.com/pip-services3-go/pip-services3-data-go/persistence"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mngoptions "go.mongodb.org/mongo-driver/mongo/options"
)

/*
IdentifiableMongoDbPersistence is abstract persistence component that stores data in MongoDB
and implements a number of CRUD operations over data items with unique ids.
The data items must implement IIdentifiable interface.

In basic scenarios child classes shall only override GetPageByFilter,
GetListByFilter or DeleteByFilter operations with specific filter function.
All other operations can be used out of the box.

In complex scenarios child classes can implement additional operations by
accessing c.Collection properties.

Configuration parameters:

  - collection:                  (optional) MongoDB collection name
  - connection(s):
    - discovery_key:             (optional) a key to retrieve the connection from IDiscovery
    - host:                      host name or IP address
    - port:                      port number (default: 27017)
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
    - auth_user:                 (optional) authentication user name
    - auth_password:             (optional) authentication user password
    - debug:                     (optional) enable debug output (default: false). (not used)

References:

- *:logger:*:*:1.0           (optional) ILogger components to pass log messages components to pass log messages
- *:discovery:*:*:1.0        (optional) IDiscovery services
- *:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials

Example:

  type MyMongoDbPersistence  struct {
    IdentifiableMongoDbPersistence
  }

  func NewMyMongoDbPersistence() {
    proto := reflect.TypeOf(MyData{})
    return &DummyMongoDbPersistence{*persist.NewIdentifiableMongoDbPersistence(proto, "mydata")}
  }

  composeFilter(filter cdata.FilterParams) interface{} {
    if filter == nil {
      filter = *cdata.NewEmptyFilterParams()
	}

    name := filter.GetAsNullableString("name")
    var filterObj bson.M
	if *name != "" {
	    filterObj = bson.M{"name": *name}
	  else {
	    filterObj = bson.M{}
	}
	return filterObj
  }

  func (c *MyMongoDbPersistence) GetPageByFilter(correlationId string, filter cdata.FilterParams, paging cdata.PagingParams) (page MyDataPage, err error){
      tempPage, err := c.IdentifiableMongoDbPersistence.GetPageByFilter(correlationId,
  	  composeFilter(filter), paging, nil, nil)
  	  // Convert to MyDataPage
  	  dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
  	  data := make([]MyData, dataLen)
  	  for i, v := range tempPage.Data {
  	    data[i] = v.(MyData)
  	  }
  	  page = *NewMyDataPage(&dataLen, data)
  	  return page, err
  }

  persistence = NewMyMongoDbPersistence()
  persistence.Configure(NewConfigParamsFromTuples(
    "host", "localhost",
  	"port", "27017"
  	"database", "test",
  ))

  opnErr := persitence.Open("123")
  if opnErr != nil {
  	...
  }

  crtRes, crtErr := persistence.Create("123", MyData{ id: "1", name: "ABC" })
  if crtErr != nil {
	...
  }
  getRes, getErr := persistence.GetPageByFilter("123", NewFilterParamsFromTuples("name", "ABC"), nil)
  if getErr != nil {
	...
  }
  fmt.Println(getRes.Data);          // Result: { id: "1", name: "ABC" }

  persistence.deleteById("123", "1")
	...
*/
type IdentifiableMongoDbPersistence struct {
	MongoDbPersistence
}

// NewIdentifiableMongoDbPersistence is creates a new instance of the persistence component.
// Parameters:
//  - proto reflect.Type
//  type of saved data, need for correct decode from DB
//  - collection string
//  (optional) a collection name.
// Return *IdentifiableMongoDbPersistence
// new created IdentifiableMongoDbPersistence component
func InheritIdentifiableMongoDbPersistence(overrides IMongoDbPersistenceOverrides, proto reflect.Type, collection string) *IdentifiableMongoDbPersistence {
	if collection == "" {
		panic("Collection name could not be nil")
	}
	c := IdentifiableMongoDbPersistence{}
	c.MongoDbPersistence = *InheritMongoDbPersistence(overrides, proto, collection)
	c.maxPageSize = 100
	return &c
}

// Configure is configures component by passing configuration parameters.
// Parameters:
//  - config  *cconf.ConfigParams
//  configuration parameters to be set.
func (c *IdentifiableMongoDbPersistence) Configure(config *cconf.ConfigParams) {
	c.MongoDbPersistence.Configure(config)
	c.maxPageSize = (int32)(config.GetAsIntegerWithDefault("options.max_page_size", (int)(c.maxPageSize)))
}

// GetListByIds is gets a list of data items retrieved by given unique ids.
// Parameters:
//   - correlationId  string
//   (optional) transaction id to Trace execution through call chain.
//   - ids  []interface{}
//   ids of data items to be retrieved
// Returns items []interface{}, err error
// a data list and error, if theq are occured.
func (c *IdentifiableMongoDbPersistence) GetListByIds(correlationId string, ids []interface{}) (items []interface{}, err error) {
	filter := bson.M{
		"_id": bson.M{"$in": ids},
	}
	items, err = c.GetListByFilter(correlationId, filter, nil, nil)
	return items, err
}

// GetOneById is gets a data item by its unique id.
// Parameters:
//   - correlationId     (optional) transaction id to Trace execution through call chain.
//   - id                an id of data item to be retrieved.
//   - callback          callback function that receives data item or error.
func (c *IdentifiableMongoDbPersistence) GetOneById(correlationId string, id interface{}) (item interface{}, err error) {
	filter := bson.M{"_id": id}
	docPointer := c.NewObjectByPrototype()
	foRes := c.Collection.FindOne(c.Connection.Ctx, filter)
	ferr := foRes.Decode(docPointer.Interface())
	if ferr != nil {
		if ferr == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, ferr
	}
	c.Logger.Trace(correlationId, "Retrieved from %s by id = %s", c.CollectionName, id)

	item = c.Overrides.ConvertToPublic(docPointer)
	return item, nil
}

// Create was creates a data item.
// Parameters:
//   - correlation_id string
//   (optional) transaction id to Trace execution through call chain.
//   - item interface{}
// an item to be created.
// Returns result interface{}, err error
// created item and error, if they are occured
func (c *IdentifiableMongoDbPersistence) Create(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil {
		return nil, nil
	}
	var newItem interface{}
	newItem = cmpersist.CloneObject(item, c.Prototype)
	// Assign unique id if not exist
	cmpersist.GenerateObjectId(&newItem)
	newItem = c.Overrides.ConvertFromPublic(newItem)
	insRes, insErr := c.Collection.InsertOne(c.Connection.Ctx, newItem)
	newItem = c.Overrides.ConvertToPublic(newItem)

	if insErr != nil {
		return nil, insErr
	}
	c.Logger.Trace(correlationId, "Created in %s with id = %s", c.Collection, insRes.InsertedID)

	return newItem, nil
}

// Set is sets a data item. If the data item exists it updates it,
// otherwise it create a new data item.
// Parameters:
//   - correlation_id string
//   (optional) transaction id to Trace execution through call chain.
//   - item interface{}
//   a item to be set.
// Returns result interface{}, err error
// updated item and error, if they occured
func (c *IdentifiableMongoDbPersistence) Set(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil {
		return nil, nil
	}
	var newItem interface{}
	newItem = cmpersist.CloneObject(item, c.Prototype)
	// Assign unique id if not exist
	cmpersist.GenerateObjectId(&newItem)
	id := cmpersist.GetObjectId(newItem)
	c.Overrides.ConvertFromPublic(&newItem)
	filter := bson.M{"_id": id}
	var options mngoptions.FindOneAndReplaceOptions
	retDoc := mngoptions.After
	options.ReturnDocument = &retDoc
	upsert := true
	options.Upsert = &upsert
	frRes := c.Collection.FindOneAndReplace(c.Connection.Ctx, filter, newItem, &options)
	if frRes.Err() != nil {
		return nil, frRes.Err()
	}
	c.Logger.Trace(correlationId, "Set in %s with id = %s", c.CollectionName, id)
	docPointer := c.NewObjectByPrototype()
	err = frRes.Decode(docPointer.Interface())
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	item = c.Overrides.ConvertToPublic(docPointer)
	return item, nil
}

// Update is updates a data item.
// Parameters:
//   - correlation_id string
//   (optional) transaction id to Trace execution through call chain.
//   - item  interface{}
//   an item to be updated.
// Returns result interface{}, err error
// updated item and error, if theq are occured
func (c *IdentifiableMongoDbPersistence) Update(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil { //|| item.id == nil
		return nil, nil
	}
	newItem := cmpersist.CloneObject(item, c.Prototype)
	id := cmpersist.GetObjectId(newItem)
	filter := bson.M{"_id": id}
	update := bson.D{{"$set", newItem}}
	var options mngoptions.FindOneAndUpdateOptions
	retDoc := mngoptions.After
	options.ReturnDocument = &retDoc
	fuRes := c.Collection.FindOneAndUpdate(c.Connection.Ctx, filter, update, &options)
	if fuRes.Err() != nil {
		return nil, fuRes.Err()
	}
	c.Logger.Trace(correlationId, "Updated in %s with id = %s", c.CollectionName, id)
	docPointer := c.NewObjectByPrototype()
	err = fuRes.Decode(docPointer.Interface())
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	item = c.Overrides.ConvertToPublic(docPointer)
	return item, nil
}

// UpdatePartially is updates only few selected fields in a data item.
// Parameters:
//   - correlation_id string
//   (optional) transaction id to Trace execution through call chain.
//   - id interface{}
//   an id of data item to be updated.
//   - data  cdata.AnyValueMap
//   a map with fields to be updated.
// Returns item interface{}, err error
// updated item and error, if they are occured
func (c *IdentifiableMongoDbPersistence) UpdatePartially(correlationId string, id interface{}, data *cdata.AnyValueMap) (item interface{}, err error) {
	if id == nil { //data == nil ||
		return nil, nil
	}
	newItem := bson.M{}
	for k, v := range data.Value() {
		newItem[k] = v
	}
	filter := bson.M{"_id": id}
	update := bson.D{{"$set", newItem}}
	var options mngoptions.FindOneAndUpdateOptions
	retDoc := mngoptions.After
	options.ReturnDocument = &retDoc
	fuRes := c.Collection.FindOneAndUpdate(c.Connection.Ctx, filter, update, &options)
	if fuRes.Err() != nil {
		return nil, fuRes.Err()
	}
	c.Logger.Trace(correlationId, "Updated partially in %s with id = %s", c.Collection, id)
	docPointer := c.NewObjectByPrototype()
	err = fuRes.Decode(docPointer.Interface())
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	item = c.Overrides.ConvertToPublic(docPointer)
	return item, nil
}

// DeleteById is deleted a data item by it"s unique id.
// Parameters:
//   - correlation_id string
//   (optional) transaction id to Trace execution through call chain.
//   - id  interface{}
//   an id of the item to be deleted
// Returns item interface{}, err error
// deleted item and error, if they are occured
func (c *IdentifiableMongoDbPersistence) DeleteById(correlationId string, id interface{}) (item interface{}, err error) {
	filter := bson.M{"_id": id}
	fdRes := c.Collection.FindOneAndDelete(c.Connection.Ctx, filter)
	if fdRes.Err() != nil {
		return nil, fdRes.Err()
	}
	c.Logger.Trace(correlationId, "Deleted from %s with id = %s", c.CollectionName, id)
	docPointer := c.NewObjectByPrototype()
	err = fdRes.Decode(docPointer.Interface())
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	item = c.Overrides.ConvertToPublic(docPointer)
	return item, nil
}

// DeleteByIds is deletes multiple data items by their unique ids.
//   - correlationId string
//   (optional) transaction id to Trace execution through call chain.
//   - ids  []interface{}
//   ids of data items to be deleted.
// Retrun error
// error or nil for success.
func (c *IdentifiableMongoDbPersistence) DeleteByIds(correlationId string, ids []interface{}) error {
	filter := bson.M{
		"_id": bson.M{"$in": ids},
	}
	return c.DeleteByFilter(correlationId, filter)
}
