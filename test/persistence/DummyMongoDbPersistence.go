package test_persistence

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	persist "github.com/pip-services3-go/pip-services3-mongodb-go/persistence"
	"go.mongodb.org/mongo-driver/bson"
)

// extends IdentifiableMongoDbPersistence
// implements IDummyPersistence
type DummyMongoDbPersistence struct {
	persist.IdentifiableMongoDbPersistence
}

func NewDummyMongoDbPersistence() *DummyMongoDbPersistence {
	proto := reflect.TypeOf(Dummy{})
	c := &DummyMongoDbPersistence{}
	c.IdentifiableMongoDbPersistence = *persist.InheritIdentifiableMongoDbPersistence(c, proto, "dummies")
	return c
}

func (c *DummyMongoDbPersistence) Create(correlationId string, item Dummy) (result Dummy, err error) {
	value, err := c.IdentifiableMongoDbPersistence.Create(correlationId, item)

	if value != nil {
		val, _ := value.(Dummy)
		result = val
	}
	return result, err
}

func (c *DummyMongoDbPersistence) GetListByIds(correlationId string, ids []string) (items []Dummy, err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	result, err := c.IdentifiableMongoDbPersistence.GetListByIds(correlationId, convIds)
	items = make([]Dummy, len(result))
	for i, v := range result {
		val, _ := v.(Dummy)
		items[i] = val
	}
	return items, err
}

func (c *DummyMongoDbPersistence) GetOneById(correlationId string, id string) (item Dummy, err error) {
	result, err := c.IdentifiableMongoDbPersistence.GetOneById(correlationId, id)
	if result != nil {
		val, _ := result.(Dummy)
		item = val
	}
	return item, err
}

func (c *DummyMongoDbPersistence) Update(correlationId string, item Dummy) (result Dummy, err error) {
	value, err := c.IdentifiableMongoDbPersistence.Update(correlationId, item)
	if value != nil {
		val, _ := value.(Dummy)
		result = val
	}
	return result, err
}

func (c *DummyMongoDbPersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item Dummy, err error) {
	result, err := c.IdentifiableMongoDbPersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(Dummy)
		item = val
	}
	return item, err
}

func (c *DummyMongoDbPersistence) DeleteById(correlationId string, id string) (item Dummy, err error) {
	result, err := c.IdentifiableMongoDbPersistence.DeleteById(correlationId, id)
	if result != nil {
		val, _ := result.(Dummy)
		item = val
	}
	return item, err
}

func (c *DummyMongoDbPersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableMongoDbPersistence.DeleteByIds(correlationId, convIds)
}

func (c *DummyMongoDbPersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *DummyPage, err error) {

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

	tempPage, err := c.IdentifiableMongoDbPersistence.GetPageByFilter(correlationId,
		filterObj, paging,
		sorting, nil)
	// Convert to DummyPage
	dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
	data := make([]Dummy, dataLen)
	for i, v := range tempPage.Data {
		data[i] = v.(Dummy)
	}
	page = NewDummyPage(&dataLen, data)
	return page, err
}

func (c *DummyMongoDbPersistence) GetCountByFilter(correlationId string, filter *cdata.FilterParams) (count int64, err error) {

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
