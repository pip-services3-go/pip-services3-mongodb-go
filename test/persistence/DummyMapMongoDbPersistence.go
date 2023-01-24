package test_persistence

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	persist "github.com/pip-services3-go/pip-services3-mongodb-go/persistence"
	"go.mongodb.org/mongo-driver/bson"
)

type DummyMapMongoDbPersistence struct {
	persist.IdentifiableMongoDbPersistence
}

func NewDummyMapMongoDbPersistence() *DummyMapMongoDbPersistence {
	var t map[string]interface{}
	proto := reflect.TypeOf(t)

	c := &DummyMapMongoDbPersistence{}
	c.IdentifiableMongoDbPersistence = *persist.InheritIdentifiableMongoDbPersistence(c, proto, "dummies")
	return c
}

func (c *DummyMapMongoDbPersistence) Create(correlationId string, item map[string]interface{}) (result map[string]interface{}, err error) {
	value, err := c.IdentifiableMongoDbPersistence.Create(correlationId, item)
	if value != nil {
		val, _ := value.(map[string]interface{})
		result = val
	}
	return result, err
}

func (c *DummyMapMongoDbPersistence) GetListByIds(correlationId string, ids []string) (items []map[string]interface{}, err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	result, err := c.IdentifiableMongoDbPersistence.GetListByIds(correlationId, convIds)
	items = make([]map[string]interface{}, len(result))
	for i, v := range result {
		val, _ := v.(map[string]interface{})
		items[i] = val
	}
	return items, err
}

func (c *DummyMapMongoDbPersistence) GetOneById(correlationId string, id string) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableMongoDbPersistence.GetOneById(correlationId, id)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapMongoDbPersistence) Update(correlationId string, item map[string]interface{}) (result map[string]interface{}, err error) {
	value, err := c.IdentifiableMongoDbPersistence.Update(correlationId, item)

	if value != nil {
		val, _ := value.(map[string]interface{})
		result = val
	}
	return result, err
}

func (c *DummyMapMongoDbPersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableMongoDbPersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapMongoDbPersistence) DeleteById(correlationId string, id string) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableMongoDbPersistence.DeleteById(correlationId, id)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapMongoDbPersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableMongoDbPersistence.DeleteByIds(correlationId, convIds)
}

func (c *DummyMapMongoDbPersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *MapPage, err error) {

	if filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}

	key := filter.GetAsNullableString("Key")
	var filterObj bson.M
	if key != nil && *key != "" {
		filterObj = bson.M{"key": *key}
	} else {
		filterObj = bson.M{}
	}
	sorting := bson.M{"key": -1}

	tempPage, err := c.IdentifiableMongoDbPersistence.GetPageByFilter(correlationId, filterObj, paging,
		sorting, nil)
	dataLen := int64(len(tempPage.Data))
	data := make([]map[string]interface{}, dataLen)
	for i, v := range tempPage.Data {
		data[i] = v.(map[string]interface{})
	}
	dataPage := NewMapPage(&dataLen, data)
	return dataPage, err
}

func (c *DummyMapMongoDbPersistence) GetCountByFilter(correlationId string, filter *cdata.FilterParams) (count int64, err error) {

	if filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}

	key := filter.GetAsNullableString("Key")
	var filterObj bson.M
	if key != nil && *key != "" {
		filterObj = bson.M{"key": *key}
	} else {
		filterObj = bson.M{}
	}
	return c.IdentifiableMongoDbPersistence.GetCountByFilter(correlationId, filterObj)
}
