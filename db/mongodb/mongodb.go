package mongodb

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/name5566/leaf/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Client holds a MongoDB client
type Client struct {
	*mongo.Client
	ref   int
	index int
}

// Client heap
type ClientHeap []*Client

func (h ClientHeap) Len() int {
	return len(h)
}

func (h ClientHeap) Less(i, j int) bool {
	return h[i].ref < h[j].ref
}

func (h ClientHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *ClientHeap) Push(s interface{}) {
	s.(*Client).index = len(*h)
	*h = append(*h, s.(*Client))
}

func (h *ClientHeap) Pop() interface{} {
	l := len(*h)
	s := (*h)[l-1]
	s.index = -1
	*h = (*h)[:l-1]
	return s
}

type ConnectionContext struct {
	sync.Mutex
	clients    ClientHeap
	clientOpts *options.ClientOptions
}

// goroutine safe
func Connect(url string, clientNum int) (*ConnectionContext, error) {
	return ConnectWithTimeout(url, clientNum, 10*time.Second, 5*time.Minute)
}

// goroutine safe
func ConnectWithTimeout(url string, clientNum int, connectTimeout, timeout time.Duration) (*ConnectionContext, error) {
	if clientNum <= 0 {
		clientNum = 100
		log.Release("invalid clientNum, reset to %v", clientNum)
	}

	c := &ConnectionContext{
		clientOpts: options.Client().
			ApplyURI(url).
			SetConnectTimeout(connectTimeout).
			SetTimeout(timeout),
		clients: make(ClientHeap, 0, clientNum),
	}

	for i := 0; i < clientNum; i++ {
		newClient, err := mongo.Connect(c.clientOpts)
		if err != nil {
			return nil, err
		}
		c.clients = append(c.clients, &Client{Client: newClient, index: i})
	}
	heap.Init(&c.clients)

	return c, nil
}

// goroutine safe
func (c *ConnectionContext) Close() {
	c.Lock()
	defer c.Unlock()
	for _, s := range c.clients {
		s.Disconnect(context.Background())
		if s.ref != 0 {
			log.Error("client ref = %v", s.ref)
		}
	}
}

// goroutine safe
func (c *ConnectionContext) Ref() (*Client, error) {
	c.Lock()
	defer c.Unlock()

	s := c.clients[0]
	if s.ref == 0 {
		// Refresh the client connection only when ref is 0, as it indicates the client is not in use.
		if err := s.Ping(context.Background(), nil); err != nil {
			s.Disconnect(context.Background())
			newClient, err := mongo.Connect(c.clientOpts)
			if err != nil {
				return nil, err
			}
			s.Client = newClient
		}
	}
	s.ref++
	heap.Fix(&c.clients, 0)

	return s, nil
}

// goroutine safe
func (c *ConnectionContext) UnRef(s *Client) {
	if s == nil {
		return
	}
	c.Lock()
	defer c.Unlock()

	s.ref--
	heap.Fix(&c.clients, s.index)
}

// goroutine safe
func (c *ConnectionContext) EnsureCounter(db, collection, id string) error {
	s, err := c.Ref()
	if err != nil {
		return err
	}
	defer c.UnRef(s)

	collectionRef := s.Database(db).Collection(collection)
	_, err = collectionRef.InsertOne(context.Background(), bson.M{
		"_id": id,
		"seq": 0,
	})
	if mongo.IsDuplicateKeyError(err) {
		return nil
	} else {
		return err
	}
}

// goroutine safe
func (c *ConnectionContext) NextSeq(db, collection, id string) (int, error) {
	s, err := c.Ref()
	if err != nil {
		return 0, err
	}
	defer c.UnRef(s)

	collectionRef := s.Database(db).Collection(collection)
	filter := bson.M{"_id": id}
	update := bson.M{"$inc": bson.M{"seq": 1}}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var res struct {
		Seq int `bson:"seq"`
	}
	err = collectionRef.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&res)

	return res.Seq, err
}

// goroutine safe
func (c *ConnectionContext) EnsureIndex(db, collection string, key []string) error {
	return c.ensureIndex(db, collection, key, false)
}

// goroutine safe
func (c *ConnectionContext) EnsureUniqueIndex(db, collection string, key []string) error {
	return c.ensureIndex(db, collection, key, true)
}

func (c *ConnectionContext) ensureIndex(db, collection string, key []string, unique bool) error {
	s, err := c.Ref()
	if err != nil {
		return err
	}
	defer c.UnRef(s)

	collectionRef := s.Database(db).Collection(collection)
	keysDoc := make(bson.D, len(key))
	for i, k := range key {
		keysDoc[i] = bson.E{Key: k, Value: 1}
	}

	indexModel := mongo.IndexModel{
		Keys:    keysDoc,
		Options: options.Index().SetUnique(unique).SetSparse(true),
	}
	_, err = collectionRef.Indexes().CreateOne(context.Background(), indexModel)
	return err
}
