package persistence

import (
	"math/rand"
	"reflect"
	"time"

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
  - store_key:                 (optional) a key to retrieve the credentials from ICredentialStore]]
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
		return &DummyMongoDbPersistence{*mngpersist.NewIdentifiableMongoDbPersistence(proto, "mydata")}
    }

    composeFilter(filter cdata.FilterParams) interface{} {
        if &filter == nil {
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
	maxPageSize int32
}

// NewIdentifiableMongoDbPersistence is creates a new instance of the persistence component.
// Parameters:
// 	- proto reflect.Type
// 	type of saved data, need for correct decode from DB
// 	- collection string
//  (optional) a collection name.
// Return *IdentifiableMongoDbPersistence
// new created IdentifiableMongoDbPersistence component
func NewIdentifiableMongoDbPersistence(proto reflect.Type, collection string) *IdentifiableMongoDbPersistence {
	if collection == "" {
		panic("Collection name could not be nil")
		return nil
	}
	imdbp := IdentifiableMongoDbPersistence{}
	imdbp.MongoDbPersistence = *NewMongoDbPersistence(proto, collection)
	imdbp.maxPageSize = 100
	return &imdbp
}

// Configure is configures component by passing configuration parameters.
// Parameters:
// 	- config  *cconf.ConfigParams
//  configuration parameters to be set.
func (c *IdentifiableMongoDbPersistence) Configure(config *cconf.ConfigParams) {
	c.MongoDbPersistence.Configure(config)
	c.maxPageSize = (int32)(config.GetAsIntegerWithDefault("options.max_page_size", (int)(c.maxPageSize)))
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
func (c *IdentifiableMongoDbPersistence) GetPageByFilter(correlationId string, filter interface{}, paging *cdata.PagingParams,
	sort interface{}, sel interface{}) (page cdata.DataPage, err error) {
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
		page = *cdata.NewDataPage(&total, items)
		return page, ferr
	}
	for cursor.Next(c.Connection.Ctx) {
		docPointer := getProtoPtr(c.Prototype)
		curErr := cursor.Decode(docPointer.Interface())
		if curErr != nil {
			continue
		}
		// item := docPointer.Elem().Interface()
		// c.ConvertToPublic(&item)
		item := c.getConvResult(docPointer, c.Prototype)
		items = append(items, item)
	}
	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.CollectionName)
	}
	if pagingEnabled {
		docCount, _ := c.Collection.CountDocuments(c.Connection.Ctx, filter)
		page = *cdata.NewDataPage(&docCount, items)
	} else {
		var total int64 = 0
		page = *cdata.NewDataPage(&total, items)
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
func (c *IdentifiableMongoDbPersistence) GetListByFilter(correlationId string, filter interface{}, sort interface{}, sel interface{}) (items []interface{}, err error) {

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
		docPointer := getProtoPtr(c.Prototype)
		curErr := cursor.Decode(docPointer.Interface())
		if curErr != nil {
			continue
		}

		// item := docPointer.Elem().Interface()
		// c.ConvertToPublic(&item)
		item := c.getConvResult(docPointer, c.Prototype)

		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.CollectionName)
	}
	return items, nil
}

// GetListByIds is gets a list of data items retrieved by given unique ids.
// Parameters:
// 	- correlationId  string
//	(optional) transaction id to Trace execution through call chain.
//  - ids  []interface{}
//	ids of data items to be retrieved
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
// - correlationId     (optional) transaction id to Trace execution through call chain.
// - id                an id of data item to be retrieved.
// - callback          callback function that receives data item or error.
func (c *IdentifiableMongoDbPersistence) GetOneById(correlationId string, id interface{}) (item interface{}, err error) {
	filter := bson.M{"_id": id}
	docPointer := getProtoPtr(c.Prototype)
	foRes := c.Collection.FindOne(c.Connection.Ctx, filter)
	ferr := foRes.Decode(docPointer.Interface())
	if ferr != nil {
		if ferr == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, ferr
	}
	c.Logger.Trace(correlationId, "Retrieved from %s by id = %s", c.CollectionName, id)
	// item = docPointer.Elem().Interface()
	// c.ConvertToPublic(&item)
	item = c.getConvResult(docPointer, c.Prototype)
	return item, nil
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
func (c *IdentifiableMongoDbPersistence) GetOneRandom(correlationId string, filter interface{}) (item interface{}, err error) {

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
	docPointer := getProtoPtr(c.Prototype)
	err = cursor.Decode(docPointer.Interface())
	if err != nil {
		return nil, err
	}
	// item = docPointer.Elem().Interface()
	// c.ConvertToPublic(&item)
	item = c.getConvResult(docPointer, c.Prototype)
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
func (c *IdentifiableMongoDbPersistence) Create(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil {
		return nil, nil
	}
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	// Assign unique id if not exist
	cmpersist.GenerateObjectId(&newItem)
	c.ConvertFromPublic(&newItem)
	insRes, insErr := c.Collection.InsertOne(c.Connection.Ctx, newItem)
	c.ConvertToPublic(&newItem)
	if insErr != nil {
		return nil, insErr
	}
	c.Logger.Trace(correlationId, "Created in %s with id = %s", c.Collection, insRes.InsertedID)

	if c.Prototype.Kind() == reflect.Ptr {
		newPtr := reflect.New(c.Prototype.Elem())
		newPtr.Elem().Set(reflect.ValueOf(newItem))
		return newPtr.Interface(), nil
	}
	return newItem, nil
}

// Set is sets a data item. If the data item exists it updates it,
// otherwise it create a new data item.
// Parameters:
// 	- correlation_id string
//	(optional) transaction id to Trace execution through call chain.
// 	- item interface{}
//	 a item to be set.
// Returns result interface{}, err error
// updated item and error, if they occured
func (c *IdentifiableMongoDbPersistence) Set(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil {
		return nil, nil
	}
	var newItem interface{}
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
	frRes := c.Collection.FindOneAndReplace(c.Connection.Ctx, filter, newItem, &options)
	if frRes.Err() != nil {
		return nil, frRes.Err()
	}
	c.Logger.Trace(correlationId, "Set in %s with id = %s", c.CollectionName, id)
	docPointer := getProtoPtr(c.Prototype)
	err = frRes.Decode(docPointer.Interface())
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	// item = docPointer.Elem().Interface()
	// c.ConvertToPublic(&item)
	item = c.getConvResult(docPointer, c.Prototype)
	return item, nil
}

// Update is updates a data item.
// Parameters:
// 	- correlation_id string
//	(optional) transaction id to Trace execution through call chain.
// 	- item  interface{}
//  an item to be updated.
// Returns result interface{}, err error
// updated item and error, if theq are occured
func (c *IdentifiableMongoDbPersistence) Update(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil { //|| item.id == nil
		return nil, nil
	}
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
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
	docPointer := getProtoPtr(c.Prototype)
	err = fuRes.Decode(docPointer.Interface())
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	// item = docPointer.Elem().Interface()
	// c.ConvertToPublic(&item)
	item = c.getConvResult(docPointer, c.Prototype)
	return item, nil
}

// UpdatePartially is updates only few selected fields in a data item.
// Parameters:
// 	- correlation_id string
// 	(optional) transaction id to Trace execution through call chain.
// 	- id interface{}
// 	an id of data item to be updated.
// 	- data  cdata.AnyValueMap
//	a map with fields to be updated.
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
	docPointer := getProtoPtr(c.Prototype)
	err = fuRes.Decode(docPointer.Interface())
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	// item = docPointer.Elem().Interface()
	// c.ConvertToPublic(&item)
	item = c.getConvResult(docPointer, c.Prototype)
	return item, nil
}

// DeleteById is deleted a data item by it"s unique id.
// Parameters:
// 	- correlation_id string
//  (optional) transaction id to Trace execution through call chain.
// 	- id  interface{}
//  an id of the item to be deleted
// Returns item interface{}, err error
// deleted item and error, if they are occured
func (c *IdentifiableMongoDbPersistence) DeleteById(correlationId string, id interface{}) (item interface{}, err error) {
	filter := bson.M{"_id": id}
	fdRes := c.Collection.FindOneAndDelete(c.Connection.Ctx, filter)
	if fdRes.Err() != nil {
		return nil, fdRes.Err()
	}
	c.Logger.Trace(correlationId, "Deleted from %s with id = %s", c.CollectionName, id)
	docPointer := getProtoPtr(c.Prototype)
	err = fdRes.Decode(docPointer.Interface())
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	// item = docPointer.Elem().Interface()
	// c.ConvertToPublic(&item)
	item = c.getConvResult(docPointer, c.Prototype)
	return item, nil
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
func (c *IdentifiableMongoDbPersistence) DeleteByFilter(correlationId string, filter interface{}) error {
	delRes, delErr := c.Collection.DeleteMany(c.Connection.Ctx, filter)
	var count = delRes.DeletedCount
	if delErr != nil {
		return delErr
	}
	c.Logger.Trace(correlationId, "Deleted %d items from %s", count, c.Collection)
	return nil
}

// DeleteByIds is deletes multiple data items by their unique ids.
// - correlationId string
// (optional) transaction id to Trace execution through call chain.
// - ids  []interface{}
// ids of data items to be deleted.
// Retrun error
// error or nil for success.
func (c *IdentifiableMongoDbPersistence) DeleteByIds(correlationId string, ids []interface{}) error {
	filter := bson.M{
		"_id": bson.M{"$in": ids},
	}
	return c.DeleteByFilter(correlationId, filter)
}

// service function for return pointer on new prototype object for unmarshaling
func getProtoPtr(proto reflect.Type) reflect.Value {
	if proto.Kind() == reflect.Ptr {
		proto = proto.Elem()
	}
	return reflect.New(proto)
}

func (c *IdentifiableMongoDbPersistence) getConvResult(docPointer reflect.Value, proto reflect.Type) interface{} {
	item := docPointer.Elem().Interface()
	c.ConvertToPublic(&item)
	if proto.Kind() == reflect.Ptr {
		return docPointer.Interface()
	}
	return item
}
