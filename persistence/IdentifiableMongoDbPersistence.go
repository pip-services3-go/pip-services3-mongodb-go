package persistence

import (
	"context"
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	"reflect"
	//cerror "github.com/pip-services3-go/pip-services3-commons-go/errors"
	//crefer "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	//clog "github.com/pip-services3-go/pip-services3-components-go/log"
	"go.mongodb.org/mongo-driver/bson"
	//mongodrv "go.mongodb.org/mongo-driver/mongo"
	mongoopt "go.mongodb.org/mongo-driver/mongo/options"
)

//  _ = require("lodash");
// /** @hidden */
//  async = require("async");

// import { ConfigParams } from "pip-services3-commons-node";
// import { PagingParams } from "pip-services3-commons-node";
// import { DataPage } from "pip-services3-commons-node";
// import { AnyValueMap } from "pip-services3-commons-node";
// import { IIdentifiable } from "pip-services3-commons-node";
// import { IdGenerator } from "pip-services3-commons-node";

// import { IWriter } from "pip-services3-data-node";
// import { IGetter } from "pip-services3-data-node";
// import { ISetter } from "pip-services3-data-node";

// import { MongoDbPersistence } from "./MongoDbPersistence";

/*
Abstract persistence component that stores data in MongoDB
and implements a number of CRUD operations over data items with unique ids.
The data items must implement IIdentifiable interface.

In basic scenarios child classes shall only override [[getPageByFilter]],
[[getListByFilter]] or [[deleteByFilter]] operations with specific filter function.
All other operations can be used out of the box.

In complex scenarios child classes can implement additional operations by
accessing <code>c.Collection</code> and <code>c._model</code> properties.
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
  - auth_user:                 (optional) authentication user name
  - auth_password:             (optional) authentication user password
  - debug:                     (optional) enable debug output (default: false).

### References ###

- <code>\*:logger:\*:\*:1.0</code>           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages components to pass log messages
- <code>\*:discovery:\*:\*:1.0</code>        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
- <code>\*:credential-store:\*:\*:1.0</code> (optional) Credential stores to resolve credentials

### Example ###

    class MyMongoDbPersistence extends MongoDbPersistence<MyData, string> {

    func (c *IdentifiableMongoDbPersistence) constructor() {
        base("mydata", new MyDataMongoDbSchema());
    }

    private composeFilter(filter: FilterParams): interface{} {
        filter = filter || new FilterParams();
         criteria = [];
         name = filter.getAsNullableString("name");
        if (name != nil)
            criteria.push({ name: name });
        return criteria.length > 0 ? { $and: criteria } : nil;
    }

    func (c *IdentifiableMongoDbPersistence) getPageByFilter(correlationId string, filter: FilterParams, paging: PagingParams,
        callback: (err: interface{}, page: DataPage<MyData>) => void): void {
        base.getPageByFilter(correlationId, c.composeFilter(filter), paging, nil, nil, callback);
    }

    }

     persistence = new MyMongoDbPersistence();
    persistence.configure(ConfigParams.fromTuples(
        "host", "localhost",
        "port", 27017
    ));

    persitence.open("123", (err) => {
        ...
    });

    persistence.create("123", { id: "1", name: "ABC" }, (err, item) => {
        persistence.getPageByFilter(
            "123",
            FilterParams.fromTuples("name", "ABC"),
            nil,
            (err, page) => {
                console.log(page.data);          // Result: { id: "1", name: "ABC" }

                persistence.deleteById("123", "1", (err, item) => {
                   ...
                });
            }
        )
    });
*/
//<T extends IIdentifiable<K>, K> extends MongoDbPersistence implements IWriter<T, K>, IGetter<T, K>, ISetter<T> {

type IdentifiableMongoDbPersistence struct {
	MongoDbPersistence
	maxPageSize int32
}

/*
   Creates a new instance of the persistence component.

   - collection    (optional) a collection name.
*/
func NewIdentifiableMongoDbPersistence(proto reflect.Type, collection string) *IdentifiableMongoDbPersistence {
	if collection == "" {
		//throw new Error("Collection name could not be nil");
		return nil
	}
	imdbp := IdentifiableMongoDbPersistence{}
	imdbp.MongoDbPersistence = *NewMongoDbPersistence(proto, collection)
	imdbp.maxPageSize = 100
	return &imdbp
}

/*
   Configures component by passing configuration parameters.

   - config    configuration parameters to be set.
*/
func (c *IdentifiableMongoDbPersistence) Configure(config cconf.ConfigParams) {
	//super.configure(config);
	c.Configure(config)

	c.maxPageSize = (int32)(config.GetAsIntegerWithDefault("options.max_page_size", (int)(c.maxPageSize)))
}

/*
   Converts the given object from the func (c *IdentifiableMongoDbPersistence) partial format.

   - value     the object to convert from the func (c *IdentifiableMongoDbPersistence) partial format.
   Returns the initial object.
*/
func (c *IdentifiableMongoDbPersistence) ConvertFromPublicPartial(value interface{}) interface{} {
	return c.ConvertFromPublic(value)
}

/*
   Gets a page of data items retrieved by a given filter and sorted according to sort parameters.

   This method shall be called by a func (c *IdentifiableMongoDbPersistence) getPageByFilter method from child class that
   receives FilterParams and converts them into a filter function.

   - correlationId     (optional) transaction id to Trace execution through call chain.
   - filter            (optional) a filter JSON object
   - paging            (optional) paging parameters
   - sort              (optional) sorting JSON object
   - select            (optional) projection JSON object
   - callback          callback function that receives a data page or error.
*/
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
	var options mongoopt.FindOptions

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

	cursor, ferr := c.Collection.Find(context.TODO(), filter, &options)
	items := make([]interface{}, 0, 1)
	if ferr != nil {
		var total int64 = 0
		page = *cdata.NewDataPage(&total, items)
		return page, ferr
	}

	for cursor.Next(context.TODO()) {
		docPointer := reflect.New(c.Prototype)
		item := cursor.Decode(docPointer)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.CollectionName)
	}
	if pagingEnabled {
		docCount, _ := c.Collection.CountDocuments(context.TODO(), filter)
		page = *cdata.NewDataPage(&docCount, items)
	} else {
		var total int64 = 0
		page = *cdata.NewDataPage(&total, items)
	}

	return page, nil
}

/*
   Gets a list of data items retrieved by a given filter and sorted according to sort parameters.

   This method shall be called by a func (c *IdentifiableMongoDbPersistence) getListByFilter method from child class that
   receives FilterParams and converts them into a filter function.

   - correlationId    (optional) transaction id to Trace execution through call chain.
   - filter           (optional) a filter JSON object
   - paging           (optional) paging parameters
   - sort             (optional) sorting JSON object
   - select           (optional) projection JSON object
   - callback         callback function that receives a data list or error.
*/
func (c *IdentifiableMongoDbPersistence) GetListByFilter(correlationId string, filter interface{}, sort interface{}, sel interface{}) (items []interface{}, err error) {

	// Configure options
	var options mongoopt.FindOptions

	if sort != nil {
		options.Sort = sort
	}
	if sel != nil {
		options.Projection = sel
	}

	cursor, ferr := c.Collection.Find(context.TODO(), filter, &options)
	if ferr != nil {
		return nil, ferr
	}

	for cursor.Next(context.TODO()) {
		docPointer := reflect.New(c.Prototype)
		item := cursor.Decode(docPointer)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.CollectionName)
	}
	return items, nil
}

/*
   Gets a list of data items retrieved by given unique ids.

   - correlationId     (optional) transaction id to Trace execution through call chain.
   - ids               ids of data items to be retrieved
   - callback         callback function that receives a data list or error.
*/
func (c *IdentifiableMongoDbPersistence) GetListByIds(correlationId string, ids []interface{}) (items []interface{}, err error) {
	filter := bson.M{
		"_id": bson.M{"$in": ids},
	}
	items, err = c.GetListByFilter(correlationId, filter, nil, nil)
	return items, err
}

/*
   Gets a data item by its unique id.

   - correlationId     (optional) transaction id to Trace execution through call chain.
   - id                an id of data item to be retrieved.
   - callback          callback function that receives data item or error.
*/
func (c *IdentifiableMongoDbPersistence) GetOneById(correlationId string, id interface{}) (item interface{}, err error) {
	filter := bson.M{"_id": id}

	docPointer := reflect.New(c.Prototype)
	ferr := c.Collection.FindOne(context.TODO(), filter).Decode(docPointer)
	if ferr != nil {
		return nil, ferr
	}

	c.Logger.Trace(correlationId, "Retrieved from %s by id = %s", c.CollectionName, id)

	//item = c.convertToPublic(item);
	return item, nil

}

// /*
// Gets a random item from items that match to a given filter.

// This method shall be called by a func (c *IdentifiableMongoDbPersistence) getOneRandom method from child class that
// receives FilterParams and converts them into a filter function.

// - correlationId     (optional) transaction id to Trace execution through call chain.
// - filter            (optional) a filter JSON object
// - callback          callback function that receives a random item or error.
//  */
// func (c *IdentifiableMongoDbPersistence) GetOneRandom(correlationId string, filter interface{}) (item interface{}, err error){
//     c.Collection.CountDocuments(filter, (err, count) => {
//         if (err) {
//             callback(err, nil);
//             return;
//         }

//          pos = _.random(0, count - 1);
//          options = {
//             skip: pos >= 0 ? pos : 0,
//             limit: 1,
//         }

//         c.Collection.find(filter, options).toArray((err, items) => {
//              item = (items != nil && items.length > 0) ? items[0] : nil;

//             item = c.convertToPublic(item);
//             callback(err, item);
//         });
//     });
// }

// /*
// Creates a data item.

// - correlation_id    (optional) transaction id to Trace execution through call chain.
// - item              an item to be created.
// - callback          (optional) callback function that receives created item or error.
//  */
// func (c *IdentifiableMongoDbPersistence) Create(correlationId string, item interface{}) (item interface{}, err error) {
//     if item == nil {
//         //callback(nil, nil);
//         return nil, nil
//     }

//     // Assign unique id
//     newItem interface{} = _.omit(item, "id");
//     newItem._id = item.id || IdGenerator.nextLong();
//     newItem = c.ConvertFromPublic(newItem);

//     c.Collection.InsertOne(newItem, (err, result) => {
//         if (!err)
//             c.Logger.Trace(correlationId, "Created in %s with id = %s", c.Collection, newItem._id);

//         newItem = result && result.ops ? c.convertToPublic(result.ops[0]) : nil;
//         callback(err, newItem);
//     });
// }

// /*
// Sets a data item. If the data item exists it updates it,
// otherwise it create a new data item.

// - correlation_id    (optional) transaction id to Trace execution through call chain.
// - item              a item to be set.
// - callback          (optional) callback function that receives updated item or error.
//  */
// func (c *IdentifiableMongoDbPersistence) Set(correlationId string, item interface{}) (item interface{}, err error){
//     if (item == nil) {
//         if (callback) callback(nil, nil);
//         return;
//     }

//     // Assign unique id
//      newItem: interface{} = _.omit(item, "id");
//     newItem._id = item.id || IdGenerator.nextLong();
//     newItem = c.ConvertFromPublic(newItem);

//      filter = {
//         _id: newItem._id
//     };

//      options = {
//         returnOriginal: false,
//         upsert: true
//     };

//     c.Collection.findOneAndReplace(filter, newItem, options, (err, result) => {
//         if (!err)
//             c.Logger.Trace(correlationId, "Set in %s with id = %s", c.Collection, item.id);

//         if (callback) {
//             newItem = result ? c.convertToPublic(result.value) : nil;
//             callback(err, newItem);
//         }
//     });
// }

// /*
// Updates a data item.

// - correlation_id    (optional) transaction id to Trace execution through call chain.
// - item              an item to be updated.
// - callback          (optional) callback function that receives updated item or error.
//  */
// func (c *IdentifiableMongoDbPersistence) Update(correlationId string, item interface{})(item interface{}, err error){
//     if item == nil || item.id == nil {
//         if callback
//         {
//             callback(nil, nil);
//         }
//         return nil, nil
//     }

//      newItem = _.omit(item, "id");
//     newItem = c.ConvertFromPublic(newItem);

//      filter = { _id: item.id };
//      update = { $set: newItem };
//      options = {
//         returnOriginal: false
//     };

//     c.Collection.FindOneAndUpdate(filter, update, options, (err, result) => {
//         if (!err)
//             c.Logger.Trace(correlationId, "Updated in %s with id = %s", c.Collection, item.id);

//         if (callback) {
//             newItem = result ? c.convertToPublic(result.value) : nil;
//             callback(err, newItem);
//         }
//     });
// }

// /*
// Updates only few selected fields in a data item.

// - correlation_id    (optional) transaction id to Trace execution through call chain.
// - id                an id of data item to be updated.
// - data              a map with fields to be updated.
// - callback          callback function that receives updated item or error.
//  */
// func (c *IdentifiableMongoDbPersistence) UpdatePartially(correlationId string, id interface{}, data AnyValueMap)
//     (item interface{}, err error) {

//     if data == nil || id == nil {
//         if callback {
//             callback(nil, nil);
//         }
//         return nil, nil
//     }

//      newItem = data.getAsObject();
//     newItem = c.ConvertFromPublicPartial(newItem);

//      filter = { _id: id };
//      update = { $set: newItem };
//      options = {
//         returnOriginal: false
//     };

//     c.Collection.FindOneAndUpdate(filter, update, options, (err, result) => {
//         if !err {
//             c.Logger.Trace(correlationId, "Updated partially in %s with id = %s", c.Collection, id);
//         }
//         if callback {
//             newItem = result ? c.convertToPublic(result.value) : nil;
//             callback(err, newItem);
//         }
//     });
// }

// /*
// Deleted a data item by it"s unique id.

// - correlation_id    (optional) transaction id to Trace execution through call chain.
// - id                an id of the item to be deleted
// - callback          (optional) callback function that receives deleted item or error.
//  */
// func (c *IdentifiableMongoDbPersistence) DeleteById(correlationId string, id: K, callback?: (err: interface{}, item: T) => void): void {
//      filter = { _id: id };
//     c.Collection.FindOneAndDelete(filter, (err, result) => {
//         if (!err)
//             c.Logger.Trace(correlationId, "Deleted from %s with id = %s", c.Collection, id);

//         if (callback) {
//              oldItem = result ? c.convertToPublic(result.value) : nil;
//             callback(err, oldItem);
//         }
//     });
// }

// /*
// Deletes data items that match to a given filter.

// This method shall be called by a func (c *IdentifiableMongoDbPersistence) deleteByFilter method from child class that
// receives FilterParams and converts them into a filter function.

// - correlationId     (optional) transaction id to Trace execution through call chain.
// - filter            (optional) a filter JSON object.
// - callback          (optional) callback function that receives error or nil for success.
//  */
// func (c *IdentifiableMongoDbPersistence) DeleteByFilter(correlationId string, filter interface{}) error {
//     c.Collection.deleteMinterface{}(filter, (err, result) => {
//          count = result ? result.deletedCount : 0;
//         if (!err)
//             c.Logger.Trace(correlationId, "Deleted %d items from %s", count, c.Collection);

//         if (callback) callback(err);
//     });
// }

// /*
// Deletes multiple data items by their unique ids.

// - correlationId     (optional) transaction id to Trace execution through call chain.
// - ids               ids of data items to be deleted.
// - callback          (optional) callback function that receives error or nil for success.
//  */
// func (c *IdentifiableMongoDbPersistence) DeleteByIds(correlationId string, ids []interface{}) error {
//      filter = {
//         _id: { $in: ids }
//     }
//     c.DeleteByFilter(correlationId, filter, callback);
// }
