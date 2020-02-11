package test_persistence

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	mngpersist "github.com/pip-services3-go/pip-services3-mongodb-go/persistence"
	"go.mongodb.org/mongo-driver/bson"
)

// extends IdentifiableMongoDbPersistence<Dummy, string>
// implements IDummyPersistence {
type DummyRefMongoDbPersistence struct {
	mngpersist.IdentifiableMongoDbPersistence
}

func NewDummyRefMongoDbPersistence() *DummyRefMongoDbPersistence {

	proto := reflect.TypeOf(&Dummy{})
	return &DummyRefMongoDbPersistence{*mngpersist.NewIdentifiableMongoDbPersistence(proto, "dummies")}
}

func (c *DummyRefMongoDbPersistence) Create(correlationId string, item *Dummy) (result *Dummy, err error) {
	value, err := c.IdentifiableMongoDbPersistence.Create(correlationId, item)

	if value != nil {
		val, _ := value.(*Dummy)
		result = val
	}
	return result, err
}

func (c *DummyRefMongoDbPersistence) GetListByIds(correlationId string, ids []string) (items []*Dummy, err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	result, err := c.IdentifiableMongoDbPersistence.GetListByIds(correlationId, convIds)
	items = make([]*Dummy, len(result))
	for i, v := range result {
		val, _ := v.(*Dummy)
		items[i] = val
	}
	return items, err
}

func (c *DummyRefMongoDbPersistence) GetOneById(correlationId string, id string) (item *Dummy, err error) {
	result, err := c.IdentifiableMongoDbPersistence.GetOneById(correlationId, id)
	if result != nil {
		val, _ := result.(*Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefMongoDbPersistence) Update(correlationId string, item *Dummy) (result *Dummy, err error) {
	value, err := c.IdentifiableMongoDbPersistence.Update(correlationId, item)
	if value != nil {
		val, _ := value.(*Dummy)
		result = val
	}
	return result, err
}

func (c *DummyRefMongoDbPersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item *Dummy, err error) {
	result, err := c.IdentifiableMongoDbPersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(*Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefMongoDbPersistence) DeleteById(correlationId string, id string) (item *Dummy, err error) {
	result, err := c.IdentifiableMongoDbPersistence.DeleteById(correlationId, id)
	if result != nil {
		val, _ := result.(*Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefMongoDbPersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableMongoDbPersistence.DeleteByIds(correlationId, convIds)
}

func (c *DummyRefMongoDbPersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *DummyRefPage, err error) {

	if &filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}

	key := filter.GetAsNullableString("Key")
	var filterObj bson.M
	if *key != "" {
		filterObj = bson.M{"key": *key}
	} else {
		filterObj = bson.M{}
	}
	sorting := bson.M{"key": -1}

	tempPage, err := c.IdentifiableMongoDbPersistence.GetPageByFilter(correlationId, filterObj, paging,
		sorting, nil)
	// Convert to DummyRefPage
	dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
	data := make([]*Dummy, dataLen)
	for i := range tempPage.Data {
		temp := tempPage.Data[i].(*Dummy)
		data[i] = temp
	}
	page = NewDummyRefPage(&dataLen, data)
	return page, err
}
